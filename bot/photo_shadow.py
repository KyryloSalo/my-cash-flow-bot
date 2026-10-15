"""Real, read-only shadow adapter for the photo evidence pipeline."""
from __future__ import annotations

import base64
import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict
from typing import Any

import httpx
from io import BytesIO
from datetime import date
from decimal import Decimal

from PIL import Image, ImageOps

import config
from photo_pipeline import Evidence, ExtractionPass, PipelineConfig, ReconciliationResult, build_cache_key, reconcile_extractions, plan_vertical_tiles, parse_raw_amount

logger = logging.getLogger("mcf.photo_shadow")
_CACHE_TTL_SECONDS = 45.0
_CACHE_MAX_ENTRIES = 128
_MAX_IMAGE_BYTES = 10 * 1024 * 1024
_CACHE: OrderedDict[str, tuple[float, ReconciliationResult]] = OrderedDict()
_LOCK_STRIPES = 64
_LOCKS = tuple(asyncio.Lock() for _ in range(_LOCK_STRIPES))


def _durable_cache_path(key: str):
    root = str(getattr(config, "PHOTO_PIPELINE_CACHE_DIR", "") or "").strip()
    if not root:
        return None
    os.makedirs(root, exist_ok=True)
    return os.path.join(root, hashlib.sha256(key.encode("utf-8")).hexdigest() + ".json")


def _json_default(value):
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, date):
        return {"__date__": value.isoformat()}
    raise TypeError(type(value).__name__)


def _json_hook(value):
    if isinstance(value, dict) and "__decimal__" in value:
        return Decimal(value["__decimal__"])
    if isinstance(value, dict) and "__date__" in value:
        return date.fromisoformat(value["__date__"])
    return value


def _load_durable(key: str):
    path = _durable_cache_path(key)
    if not path:
        return None
    try:
        if time.time() - os.path.getmtime(path) > max(1, int(getattr(config, "PHOTO_PIPELINE_CACHE_TTL_SECONDS", 86400))):
            return None
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle, object_hook=_json_hook)
        return ReconciliationResult(**payload)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _store_durable(key: str, result: ReconciliationResult) -> None:
    path = _durable_cache_path(key)
    if not path:
        return
    directory = os.path.dirname(path)
    fd, temporary = tempfile.mkstemp(prefix="photo-cache-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(asdict(result), handle, default=_json_default, separators=(",", ":"))
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _core_schema() -> dict[str, Any]:
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


def _schema() -> dict[str, Any]:
    schema = _core_schema()
    schema["properties"].update({
        "transaction_date": {"type": ["string", "null"]},
        "merchant": {"type": ["string", "null"]},
        "receipt_total": {"type": ["string", "null"]},
        "receipt_items": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "properties": {"name": {"type": "string"}, "amount_raw": {"type": "string"}},
            "required": ["name", "amount_raw"],
        }},
    })
    schema["required"] = list(schema["properties"])
    return schema


def _prompt() -> str:
    return (
        "Extract evidence from exactly one full original transaction image. Return only structured JSON evidence. "
        "Fields are amount, currency, status, and type. Each item must include the exact visible raw_text, a "
        "normalized value, a stable local id, and region when visible. Never infer totals, currency, status, or type. "
        "If the image contains unsupported content, multiple transactions, a statement, or ambiguous evidence, "
        "return an empty evidence array. Do not include account IDs, card numbers, or unrelated text. "
        "The image may be a crop: omit absent fields, never infer them. "
        "For a receipt extract the visible ISO transaction_date, merchant, receipt_total as a decimal string, "
        "and receipt_items with exact visible name and amount_raw. Use null or an empty list if absent. "
        "Receipt line items are optional review metadata, never separate transactions. Do not invent payment status."
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
    if not isinstance(raw, dict):
        raise ValueError("Invalid extraction object")
    transaction_date = None
    if isinstance(raw.get("transaction_date"), str):
        try:
            transaction_date = date.fromisoformat(raw["transaction_date"])
        except ValueError:
            pass
    line_items = []
    receipt_rows = raw.get("receipt_items")
    if isinstance(receipt_rows, list):
        for row in receipt_rows[:200]:
            if not isinstance(row, dict):
                continue
            name, amount_raw = row.get("name"), row.get("amount_raw")
            amount = parse_raw_amount(amount_raw) if isinstance(amount_raw, str) else None
            if isinstance(name, str) and name.strip() and amount is not None:
                line_items.append({"name": name.strip()[:160], "amount": amount})
    total_raw = raw.get("receipt_total")
    receipt_total = parse_raw_amount(total_raw) if isinstance(total_raw, str) else None
    merchant = raw.get("merchant")
    return ExtractionPass(
        source=source, evidence=tuple(evidence), line_items=tuple(line_items),
        receipt_total=receipt_total, transaction_date=transaction_date,
        merchant=merchant.strip()[:160] if isinstance(merchant, str) and merchant.strip() else None,
        raw_evidence={"receipt_items_incomplete": isinstance(receipt_rows, list) and len(line_items) != len(receipt_rows)},
    )


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
    durable = _load_durable(key)
    if durable is not None:
        _CACHE[key] = (time.monotonic(), durable)
        return durable
    now = time.monotonic()
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        _CACHE.move_to_end(key)
        logger.info("photo shadow cache hit: findings=%d accepted=%s", len(cached[1].findings), cached[1].accepted)
        return cached[1]
    lock = _lock_for(key)
    async with lock:
        durable = _load_durable(key)
        if durable is not None:
            _CACHE[key] = (time.monotonic(), durable)
            return durable
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
        _store_durable(key, result)
        logger.info("photo shadow complete: findings=%d accepted=%s", len(result.findings), result.accepted)
        return result


async def analyze_photo(image_bytes: bytes, *, mime_type: str, model: str, detail: str) -> ReconciliationResult:
    """Decode, canonicalize, crop, submit, and reconcile real image passes."""
    normalized_mime = _normalize_mime_type(mime_type)
    if normalized_mime not in {"image/jpeg", "image/png", "image/webp"}:
        raise ValueError("Unsupported image type")
    if not image_bytes or len(image_bytes) > _MAX_IMAGE_BYTES: raise ValueError("Invalid image")
    try:
        with Image.open(BytesIO(image_bytes)) as source:
            if source.width > 12000 or source.height > 12000 or source.width * source.height > 40_000_000: raise ValueError("Image dimensions are too large")
            image = ImageOps.exif_transpose(source).convert("RGBA")
            canvas = Image.new("RGB", image.size, "white"); canvas.paste(image, mask=image.getchannel("A"))
            if max(canvas.size) > 6000:
                scale = 6000 / max(canvas.size); canvas = canvas.resize((max(1,round(canvas.width*scale)),max(1,round(canvas.height*scale))), Image.Resampling.LANCZOS)
            out = BytesIO(); canvas.save(out, format="JPEG", quality=90, optimize=True); canonical = out.getvalue()
    except (OSError, ValueError) as exc: raise ValueError("Malformed or unsupported image") from exc
    key = build_cache_key(image_bytes, config=PipelineConfig(), model=model, detail=detail).as_string() + ":active"
    durable = _load_durable(key)
    if durable is not None:
        _CACHE[key] = (time.monotonic(), durable)
        return durable
    lock = _lock_for(key)
    async with lock:
        durable = _load_durable(key)
        if durable is not None:
            _CACHE[key] = (time.monotonic(), durable)
            return durable
        tile_height=min(2400, canvas.height); boxes=plan_vertical_tiles(height=canvas.height,tile_height=tile_height,overlap=.2)
        crops=[]
        for top,bottom in boxes:
            buf=BytesIO(); canvas.crop((0,top,canvas.width,bottom)).save(buf,format="JPEG",quality=88,optimize=True); crops.append(buf.getvalue())
        passes = await asyncio.gather(
            _request(canonical,mime_type="image/jpeg",model=model,detail=detail,source="full-canonical-a"),
            _request(canonical,mime_type="image/jpeg",model=model,detail=detail,source="full-canonical-b"),
            *[_request(blob,mime_type="image/jpeg",model=model,detail=detail,source=f"crop-{i}") for i,blob in enumerate(crops)],
        )
        result = reconcile_extractions(tuple(passes))
        trace = dict(result.trace)
        trace.update({"pipeline":"evidence","preprocessing":"exif-transpose-alpha-white-jpeg","tile_overlap":.2,"tile_plan":boxes,"submitted_passes":len(passes)})
        result = ReconciliationResult(**{**result.__dict__, "trace": trace, "receipt_level": "L1" if result.accepted else None})
        _CACHE[key] = (time.monotonic(), result)
        _store_durable(key, result)
        return result
