"""Real, read-only shadow adapter for the photo evidence pipeline."""
from __future__ import annotations

import base64
import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict
from dataclasses import asdict
from typing import Any

import httpx

import config
from photo_pipeline import Evidence, ExtractionPass, PipelineConfig, ReconciliationResult, build_cache_key, reconcile_extractions

logger = logging.getLogger("mcf.photo_shadow")
_CACHE_TTL_SECONDS = 45.0
_CACHE_MAX_ENTRIES = 128
_MAX_IMAGE_BYTES = 10 * 1024 * 1024
_CACHE: OrderedDict[str, tuple[float, ReconciliationResult]] = OrderedDict()
_LOCK_STRIPES = 64
_LOCKS = tuple(asyncio.Lock() for _ in range(_LOCK_STRIPES))


def _schema() -> dict[str, Any]:
    return {
        "type": "object", "additionalProperties": False,
        "properties": {"evidence": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "properties": {
                "field": {"type": "string", "enum": ["amount", "currency", "status", "type"]},
                "id": {"type": "string"}, "raw_text": {"type": "string"},
                "value": {"type": "string"}, "region": {"type": ["string", "null"]},
            }, "required": ["field", "id", "raw_text", "value", "region"]
        }}}, "required": ["evidence"]
    }


def _prompt() -> str:
    return (
        "Extract evidence from exactly one full original transaction image. Return only structured JSON evidence. "
        "Fields are amount, currency, status, and type. Each item must include the exact visible raw_text, a "
        "normalized value, a stable local id, and region when visible. Never infer totals, currency, status, or type. "
        "If the image contains unsupported content, multiple transactions, a statement, or ambiguous evidence, "
        "return an empty evidence array. Do not include account IDs, card numbers, or unrelated text."
    )


def _payload(*, image_bytes: bytes, mime_type: str, model: str, detail: str) -> dict[str, Any]:
    data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"
    return {
        "model": model, "store": False, "max_output_tokens": config.OPENAI_MAX_OUTPUT_TOKENS,
        "input": [{"role": "user", "content": [
            {"type": "input_text", "text": _prompt()},
            {"type": "input_image", "image_url": data_url, "detail": detail},
        ]}],
        "text": {"format": {"type": "json_schema", "name": "photo_shadow_evidence", "strict": True, "schema": _schema()}},
    }


def _extract_text_payload(payload: dict[str, Any]) -> str:
    text = payload.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    for item in payload.get("output") or []:
        if isinstance(item, dict) and item.get("type") == "message":
            for content in item.get("content") or []:
                if isinstance(content, dict) and isinstance(content.get("text"), str) and content["text"].strip():
                    return content["text"].strip()
    raise ValueError("Responses payload has no text output")


def _normalize_mime_type(mime_type: str) -> str:
    normalized = str(mime_type or "").split(";", 1)[0].strip().lower()
    return "image/jpeg" if normalized == "image/jpg" else normalized


def _lock_for(key: str) -> asyncio.Lock:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return _LOCKS[int.from_bytes(digest[:8], "big") % _LOCK_STRIPES]


def _parse_pass(source: str, text: str) -> ExtractionPass:
    raw = json.loads(text)
    rows = raw.get("evidence", []) if isinstance(raw, dict) else []
    evidence: list[Evidence] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or row.get("field") not in {"amount", "currency", "status", "type"}:
            continue
        evidence.append(Evidence(
            id=f"{source}-{str(row.get('id') or len(evidence))}", field=str(row["field"]),
            raw_text=str(row.get("raw_text") or ""), value=str(row.get("value") or ""),
            region=str(row["region"]) if row.get("region") is not None else None,
        ))
    return ExtractionPass(source=source, evidence=tuple(evidence))


async def _request(image_bytes: bytes, *, mime_type: str, model: str, detail: str, source: str) -> ExtractionPass:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    headers = {"Authorization": f"Bearer {config.OPENAI_API_KEY}", "Content-Type": "application/json"}
    timeout = max(config.OPENAI_REQUEST_TIMEOUT_MS, 1_000) / 1000.0
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post("https://api.openai.com/v1/responses", headers=headers,
                                     json=_payload(image_bytes=image_bytes, mime_type=mime_type, model=model, detail=detail))
        response.raise_for_status()
        return _parse_pass(source, _extract_text_payload(response.json()))


async def analyze_shadow(image_bytes: bytes, *, mime_type: str, model: str, detail: str) -> ReconciliationResult:
    """Run two independent full-image passes and reconcile; never writes financial state."""
    normalized_mime = _normalize_mime_type(mime_type)
    if normalized_mime not in {"image/jpeg", "image/png", "image/webp"}:
        raise ValueError("Unsupported image type")
    if not image_bytes:
        raise ValueError("Image is required")
    if len(image_bytes) > _MAX_IMAGE_BYTES:
        raise ValueError("Image is too large")
    key = ":".join((build_cache_key(image_bytes, config=PipelineConfig(), model=model, detail=detail).as_string(), normalized_mime))
    now = time.monotonic()
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        _CACHE.move_to_end(key)
        logger.info("photo shadow cache hit: findings=%d accepted=%s", len(cached[1].findings), cached[1].accepted)
        return cached[1]
    lock = _lock_for(key)
    async with lock:
        now = time.monotonic(); cached = _CACHE.get(key)
        if cached and now - cached[0] < _CACHE_TTL_SECONDS:
            _CACHE.move_to_end(key); return cached[1]
        try:
            first, second = await asyncio.gather(
                _request(image_bytes, mime_type=normalized_mime, model=model, detail=detail, source="full-a"),
                _request(image_bytes, mime_type=normalized_mime, model=model, detail=detail, source="full-b"),
            )
            result = reconcile_extractions((first, second))
        except Exception as exc:
            logger.warning("photo shadow analysis failed: error_type=%s", type(exc).__name__)
            raise
        _CACHE[key] = (time.monotonic(), result); _CACHE.move_to_end(key)
        while len(_CACHE) > _CACHE_MAX_ENTRIES:
            _CACHE.popitem(last=False)
        logger.info("photo shadow complete: findings=%d accepted=%s", len(result.findings), result.accepted)
        return result
