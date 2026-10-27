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
from literal_dates import verified_literal_date
from statement_review import align_review_rows, is_non_operation
from photo_pipeline import Evidence, ExtractionPass, PipelineConfig, ReconciliationResult, build_cache_key, reconcile_extractions, plan_vertical_tiles, parse_raw_amount, assess_image_quality, _visible_amount, _currency, _lex, _STATUS, _TYPE

logger = logging.getLogger("mcf.photo_shadow")
_CACHE_TTL_SECONDS = 45.0
_DURABLE_CACHE_VERSION = 6
_DURABLE_CACHE_PRODUCER = "photo-shadow"
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
    try:
        path = _durable_cache_path(key)
        if not path:
            return None
        if time.time() - os.path.getmtime(path) > max(1, int(getattr(config, "PHOTO_PIPELINE_CACHE_TTL_SECONDS", 86400))):
            return None
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle, object_hook=_json_hook)
        if (not isinstance(payload, dict) or payload.get("version") != _DURABLE_CACHE_VERSION
                or payload.get("producer") != _DURABLE_CACHE_PRODUCER or payload.get("key") != key):
            return None
        result_payload = payload.get("result")
        if not isinstance(result_payload, dict):
            return None
        for field in ("evidence_ids", "findings", "line_items", "raw_evidence"):
            if field in result_payload and isinstance(result_payload[field], list):
                result_payload[field] = tuple(result_payload[field])
        return ReconciliationResult(**result_payload)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def _store_durable(key: str, result: ReconciliationResult) -> None:
    try:
        path = _durable_cache_path(key)
        if not path:
            return
        directory = os.path.dirname(path)
        fd, temporary = tempfile.mkstemp(prefix="photo-cache-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"version": _DURABLE_CACHE_VERSION, "producer": _DURABLE_CACHE_PRODUCER,
                           "key": key, "result": asdict(result)}, handle, default=_json_default, separators=(",", ":"))
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except OSError:
                pass
    except OSError:
        logger.debug("durable photo cache unavailable", exc_info=True)


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
        }}, "mode": {"type": "string", "enum": ["single_tx", "statement_expenses", "receipt", "unsupported"]}, "statement_items": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "properties": {
                "transaction_date": {"type": "string"}, "type": {"type": "string"},
                "amount": {"type": "string"}, "currency": {"type": "string"},
                "category_hint": {"type": "string"}, "comment": {"type": "string"},
                "confidence": {"type": "number"}, "status": {"type": "string"},
                "raw_date": {"type": "string"}, "raw_amount": {"type": "string"},
                "raw_currency": {"type": "string"}, "raw_status": {"type": "string"},
                "raw_type": {"type": "string"}, "classification": {"type": "string"},
                "review_reason": {"type": "string"},
            }, "required": ["transaction_date", "type", "amount", "currency", "category_hint", "comment", "confidence", "status", "raw_date", "raw_amount", "raw_currency", "raw_status", "raw_type", "classification", "review_reason"]
        }}, "skipped_items_count": {"type": "integer"}}, "required": ["evidence", "mode", "statement_items", "skipped_items_count"]
    }


def _schema() -> dict[str, Any]:
    schema = _core_schema()
    schema["properties"].update({
        "transaction_date": {"type": ["string", "null"]},
        "raw_date": {"type": ["string", "null"]},
        "merchant": {"type": ["string", "null"]},
        "category_hint": {"type": ["string", "null"]},
        "receipt_total": {"type": ["string", "null"]},
        "receipt_items": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "properties": {"name": {"type": "string"}, "amount_raw": {"type": "string"}},
            "required": ["name", "amount_raw"],
        }},
    })
    schema["required"] = list(schema["properties"])
    return schema


def _prompt(*, today: date | None = None) -> str:
    return (
        f"Reference date: {(today or date.today()).isoformat()}. "
        "For bank screenshots extract transaction_date as ISO date from the visible date, not only for receipts. "
        "If the year is omitted, never invent a year: leave transaction_date empty, preserve the literal raw date for review, and the reference date cannot fill a missing year. "
        "For single_tx and receipt, put the exact visible date text in top-level raw_date, or null when absent; never add a missing year. "
        "Extract evidence from one banking screenshot. Set mode to single_tx for one transaction, statement_expenses "
        "for a history/statement with multiple rows, receipt for a merchant receipt, or unsupported when unclear. "
        "Classify the document layout BEFORE checking row eligibility. "
        "Repeated transaction rows with date headings MUST use statement_expenses even when no row has an explicit payment status or type label. "
        "Missing status/type labels do not make a history unsupported. In history mode return EVERY visible transaction row, "
        "not the card balance at the top. If a row's status or type is not explicitly printed, "
        "use unknown and leave the corresponding raw field empty; set classification=ambiguous with a review_reason. "
        "Unknown rows are review-only and cannot become confirmed expenses. "
        "Positive or green amounts are not expenses. Preserve income, refund, declined and transfer rows for review too. "
        "For history mode return evidence=[] and statement_items; do not try to summarize the whole history as one transaction. "
        "A transaction history remains statement_expenses even if no row is eligible. "
        "For single_tx and receipt, statement_items MUST be []; put all visible core fields in evidence, never in statement_items. "
        "Do not clear visible amount/currency evidence merely because status or type is missing: omit only the missing field. "
        "Return only structured JSON evidence. "
        "Extract category_hint only as the exact visible bank category label, or null if absent. "
        "Do not infer a category from merchant or items; it is only a suggestion for user review. "
        "Fields are amount, currency, status, and type. Each item must include the exact visible raw_text, a "
        "normalized value and a stable local id. For one transaction use region=null, not invented region names. "
        "amount.value is the positive decimal magnitude, e.g. raw_text '-402.25 UAH' => value '402.25'. "
        "Use the primary settled/account amount, NOT original currency amount, exchange rate, balance or cashback. "
        "currency.value is its explicit ISO code, e.g. UAH. currency.raw_text must quote only the visible currency "
        "token (e.g. UAH), not the entire amount. amount.raw_text must include its sign and suffix currency as visible. "
        "status.value must be completed, declined or refunded: "
        "'Успішно'/'Оплачено' => completed, 'Відхилено' => declined, 'Повернення' => refunded. "
        "type.value must be expense, income, transfer, own_account_transfer or refund. Use visible payment text: "
        "'Покупка'/'Оплата' => expense. raw_text must quote that exact visible label. "
        "Never use the 'Розділити оплату' or 'Повернути кошти' buttons as evidence for ANY type or status: "
        "these are available actions, not historical transaction evidence. Omit type when no other explicit label exists. "
        "Return one type evidence for the transaction, plus any actually conflicting transfer/refund label. "
        "'З картки', merchant names, categories, amount signs and the 'Повернути кошти' action alone are NOT type evidence. "
        "A refund action button is not the current transaction status. Include conflicting evidence for the primary "
        "transaction; never suppress a transfer/refund label. Never infer totals, currency, status, or type. "
        "For a statement/history return ALL visible rows in statement_items, including excluded and ambiguous rows, preserving each visible date. "
        "In statement_expenses mode evidence MUST be an empty array. Every returned row MUST include raw_date, raw_amount, raw_currency, raw_status, raw_type, classification, and review_reason; these are row-specific evidence/metadata, not generic evidence. "
        "Do not omit income, refunds, failed/declined rows, or transfers explicitly shown as between the user's own accounts from statement_items. "
        "These rows are excluded ONLY from expense eligibility by downstream validation, not from extraction. "
        "Keep each as a separate row with its visible raw evidence and classification; count and surface every invalid row for review. "
        "Generic transfer wording is ambiguous: keep that row with classification=ambiguous and review_reason; it must not become an expense. Never fabricate or merge rows. "
        "For a recognizable single transaction, return mode=single_tx even when a core field is missing; "
        "preserve visible fields and omit only missing or ambiguous fields. "
        "Return empty evidence only for genuinely unsupported documents. Do not include account IDs, card numbers, or unrelated text. "
        "The image may be a crop: omit absent fields, never infer them. "
        "For a receipt extract the visible ISO transaction_date, merchant, receipt_total as a decimal string, "
        "and receipt_items with exact visible name and amount_raw. Use null or an empty list if absent. "
        "Receipt line items are optional review metadata, never separate transactions. Do not invent payment status."
    )


def _payload(*, image_bytes: bytes, mime_type: str, model: str, detail: str, today: date | None = None) -> dict[str, Any]:
    data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"
    return {
        # A statement contains multiple structured rows; the legacy single-operation
        # budget truncates valid JSON before reconciliation can inspect the rows.
        "model": model, "store": False, "max_output_tokens": max(4096, config.OPENAI_MAX_OUTPUT_TOKENS),
        "input": [{"role": "user", "content": [
            {"type": "input_text", "text": _prompt(today=today)},
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


def _preprocess_variants(canvas: Image.Image) -> tuple[bytes, ...]:
    """Build bounded deterministic color/contrast/grayscale inputs; retain color first."""
    variants: list[bytes] = []
    for image, quality in ((canvas, 90), (ImageOps.autocontrast(canvas.convert("L")).convert("RGB"), 88)):
        buf = BytesIO(); image.save(buf, format="JPEG", quality=quality, optimize=True); variants.append(buf.getvalue())
    return tuple(variants)


def _lock_for(key: str) -> asyncio.Lock:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return _LOCKS[int.from_bytes(digest[:8], "big") % _LOCK_STRIPES]


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _safe_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _parse_pass(source: str, text: str) -> ExtractionPass:
    raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("Invalid extraction object")

    mode = str(raw.get("mode") or "unsupported").strip().lower()
    rows = raw.get("evidence", []) if isinstance(raw, dict) else []
    if mode == "statement_expenses" and rows:
        raise ValueError("statement mode requires empty generic evidence")
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
    transaction_date = verified_literal_date(raw.get("transaction_date"), raw.get("raw_date"))
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
    mode = str(raw.get("mode") or "unsupported").strip().lower()
    if mode not in {"single_tx", "statement_expenses", "receipt", "unsupported"}:
        mode = "unsupported"
    statement_items = []
    invalid_rows = 0
    review_rows = []
    non_transaction_rows = []
    def exclude_row(index, row, reason):
        nonlocal invalid_rows
        invalid_rows += 1
        candidate = row if isinstance(row, dict) else {}
        visible_amount, visible_currency, visible_direction = _visible_amount(str(candidate.get("raw_amount") or ""))
        tx_date = verified_literal_date(candidate.get("transaction_date"), candidate.get("raw_date"))
        review_rows.append({"row_index": index, "reason": reason, "transaction_date": tx_date,
                            "amount": visible_amount, "currency": visible_currency or _currency(candidate.get("raw_currency")),
                            "direction": visible_direction,
                            "type": candidate.get("type"), "status": candidate.get("status"),
                            "comment": str(candidate.get("comment") or candidate.get("category_hint") or "")[:500] or None,
                            "raw_row": candidate})
    rows = raw.get("statement_items")
    if isinstance(rows, list):
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                exclude_row(index, None, "invalid_row")
                continue
            if is_non_operation(row):
                non_transaction_rows.append(row)
                continue
            if index >= 200:
                exclude_row(index, row, "row_limit")
                continue
            if row.get("type") != "expense" or row.get("status") != "completed":
                exclude_row(index, row, "excluded_status_or_type")
                continue
            amount = parse_raw_amount(row.get("amount"))
            currency = str(row.get("currency") or "").strip().upper()
            tx_date = verified_literal_date(row.get("transaction_date"), row.get("raw_date"))
            if tx_date is None:
                exclude_row(index, row, "missing_or_invalid_date")
                continue
            if amount is None or currency not in {"UAH", "TRY", "EUR", "USD"}:
                exclude_row(index, row, "invalid_amount_or_currency")
                continue
            visible_amount, visible_currency, visible_direction = _visible_amount(str(row.get("raw_amount") or ""))
            if visible_amount != amount or visible_direction == "income" or (visible_currency and visible_currency != currency):
                exclude_row(index, row, "amount_evidence_mismatch")
                continue
            if not str(row.get("raw_date") or "").strip():
                exclude_row(index, row, "missing_date_evidence")
                continue
            if (_currency(str(row.get("raw_currency") or "")) != currency
                    or _lex(str(row.get("raw_status") or ""), _STATUS) != "completed"
                    or _lex(str(row.get("raw_type") or ""), _TYPE) != "expense"
                    or row.get("classification") != "expense"):
                exclude_row(index, row, "status_type_or_currency_evidence_mismatch")
                continue
            statement_items.append({"transaction_date": tx_date, "type": "expense", "amount": amount,
                                    "currency": currency, "category_hint": str(row.get("category_hint") or "")[:120] or None,
                                    "comment": str(row.get("comment") or "")[:500] or None,
                                    "confidence": max(0.0, min(_safe_float(row.get("confidence")), 1.0)),
                                    "raw_date": str(row.get("raw_date") or ""), "raw_amount": str(row.get("raw_amount") or ""),
                                    "raw_currency": str(row.get("raw_currency") or ""), "raw_status": str(row.get("raw_status") or ""),
                                    "raw_type": str(row.get("raw_type") or ""), "classification": str(row.get("classification") or "expense"),
                                    "review_reason": str(row.get("review_reason") or "")})
    return ExtractionPass(
        source=source, evidence=tuple(evidence), line_items=tuple(line_items),
        receipt_total=receipt_total, transaction_date=transaction_date,
        merchant=merchant.strip()[:160] if isinstance(merchant, str) and merchant.strip() else None,
        raw_evidence={"receipt_items_incomplete": isinstance(receipt_rows, list) and len(line_items) != len(receipt_rows),
                      "category_hint": str(raw.get("category_hint") or "").strip()[:120] or None,
                      "raw_date": raw["raw_date"].strip()[:160] if isinstance(raw.get("raw_date"), str) else None,
                      "_mode_explicit": "mode" in raw,
                      "statement_items": statement_items, "statement_review_rows": review_rows,
                      "non_transaction_rows": non_transaction_rows,
                      "skipped_items_count": max(invalid_rows, _safe_non_negative_int(raw.get("skipped_items_count")))},
        mode=mode,
    )


async def _request(image_bytes: bytes, *, mime_type: str, model: str, detail: str, source: str, today: date | None = None) -> ExtractionPass:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    headers = {"Authorization": f"Bearer {config.OPENAI_API_KEY}", "Content-Type": "application/json"}
    timeout = max(config.OPENAI_REQUEST_TIMEOUT_MS, 1_000) / 1000.0
    payload = _payload(image_bytes=image_bytes, mime_type=mime_type, model=model, detail=detail, today=today)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(2):
            response = await client.post("https://api.openai.com/v1/responses", headers=headers, json=payload)
            response.raise_for_status()
            try:
                extraction = _parse_pass(source, _extract_text_payload(response.json()))
            except json.JSONDecodeError:
                logger.warning("photo provider returned truncated JSON", extra={"source": source})
                return ExtractionPass(source=source, raw_evidence={"provider_truncated": True})
            except (ValueError, TypeError, AttributeError):
                logger.warning("photo provider returned invalid evidence", extra={"source": source})
                extraction = ExtractionPass(source=source, raw_evidence={"provider_invalid": True})
            raw = extraction.raw_evidence or {}
            explicit_mode = raw.get('_mode_explicit') is True
            contradictory_layout = explicit_mode and extraction.mode == 'unsupported' and bool(
                extraction.evidence or raw.get('statement_items') or raw.get('statement_review_rows'))
            invalid_token = any(finding.endswith('_evidence_invalid') for finding in
                                reconcile_extractions((extraction,)).findings)
            # Legacy/provider-compatible payloads may omit the additive mode field;
            # do not spend a second provider request on those responses. Retry only
            # when the provider explicitly selected a current schema mode.
            retryable_invalid = (invalid_token and explicit_mode) or raw.get('provider_invalid') is True
            if attempt or not source.startswith('full-') or not (contradictory_layout or retryable_invalid):
                return extraction
            # Re-extract once from the same image; never fix digits, synthesize
            # labels, vote away conflicts, or treat a retry as another source.
            payload['input'][0]['content'][0]['text'] = (
                'Recheck the WHOLE image before reading any individual transaction. '
                'Several separately positioned signed amounts or date headings indicate a history: '
                'use statement_expenses and return every row, including failed payments and incoming transfers. '
                'Do not pick one failed row and describe the whole list as single_tx. '
                'A genuinely single declined transaction remains single_tx and declined. '
                'Quote only exact visible core labels, not a merchant/category or unrelated action. '
                'Unknown fields stay unknown; never invent status/type or alter money. '
                + _prompt(today=today))


async def _collect_passes(*requests):
    """Do not let failed/cancelled uploads leave provider requests running."""
    tasks = [asyncio.ensure_future(request) for request in requests]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


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
            first, second = await _collect_passes(
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


def _reconcile_statement_passes(passes, result):
    """Require full-image agreement on every financial row, retaining duplicates.

    Crops are supplemental: absence outside their bounds is not disagreement.
    Descriptive OCR differences and excluded rows remain explicit review findings.
    """
    from collections import Counter

    full = [p for p in passes if p.source.startswith('full-')]
    if len({p.source for p in full}) < 2 or not any(p.mode == 'statement_expenses' for p in full):
        return result
    layout_conflict = any(p.mode != 'statement_expenses' for p in full)
    rows_by_pass = [(p.raw_evidence or {}).get('statement_items', []) for p in full]
    review_rows = align_review_rows([(p.source, (p.raw_evidence or {}).get('statement_review_rows', [])) for p in full])
    if layout_conflict or not all(rows_by_pass):
        findings = tuple(dict.fromkeys((*result.findings, 'statement_review_only',
            'statement_layout_conflict' if layout_conflict else 'statement_rows_unresolved')))
        return ReconciliationResult(**{**result.__dict__, 'accepted': False, 'requires_review': True,
            'amount': None, 'status': None, 'transaction_type': None, 'evidence_ids': (),
            'findings': findings, 'statement_review_rows': review_rows,
            'skipped_items_count': max((int((p.raw_evidence or {}).get('skipped_items_count', 0)) for p in full), default=0)})

    def financial_key(row):
        return (row['transaction_date'], row['amount'], row['currency'])

    expected = Counter(financial_key(row) for row in rows_by_pass[0])
    if any(Counter(financial_key(row) for row in rows) != expected for rows in rows_by_pass[1:]):
        return ReconciliationResult(**{**result.__dict__, 'findings': tuple(dict.fromkeys((*result.findings, 'statement_financial_conflict')))})
    findings = []
    descriptions = [Counter((financial_key(row), ' '.join(str(row.get('comment') or '').casefold().split())) for row in rows) for rows in rows_by_pass]
    if any(items != descriptions[0] for items in descriptions[1:]):
        findings.append('statement_description_conflict')
    skipped = max((int((p.raw_evidence or {}).get('skipped_items_count', 0)) for p in full), default=0)
    if skipped:
        findings.append('statement_rows_excluded')
    for extraction in passes:
        if extraction.source.startswith('full-'):
            continue
        raw = extraction.raw_evidence or {}
        supplemental = Counter(financial_key(row) for row in raw.get('statement_items', []))
        if supplemental - expected or raw.get('provider_truncated') or raw.get('provider_invalid'):
            findings.append('statement_crop_requires_review')
    findings = tuple(dict.fromkeys(findings))
    rows = rows_by_pass[0]
    return ReconciliationResult(**{**result.__dict__, 'accepted': True, 'requires_review': bool(findings) or bool(review_rows),
        'amount': None, 'currency': rows[0]['currency'], 'status': 'completed', 'transaction_type': 'expense',
        'evidence_ids': (), 'findings': findings, 'statement_items': tuple(rows),
        'statement_review_rows': review_rows, 'skipped_items_count': skipped})


async def analyze_photo(image_bytes: bytes, *, mime_type: str, model: str, detail: str, today: date | None = None) -> ReconciliationResult:
    """Decode, canonicalize, crop, submit, and reconcile real image passes."""
    today = today or date.today()
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
            variants = _preprocess_variants(canvas)
            canonical = variants[0]
    except (OSError, ValueError) as exc: raise ValueError("Malformed or unsupported image") from exc
    key = build_cache_key(image_bytes, config=PipelineConfig(), model=model, detail=detail).as_string() + ":active:statement-row-v4:" + today.isoformat()
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
        quality = assess_image_quality(image_bytes)
        passes = await _collect_passes(
            _request(canonical,mime_type="image/jpeg",model=model,detail=detail,source="full-canonical-a",today=today),
            _request(variants[1],mime_type="image/jpeg",model=model,detail=detail,source="full-enhanced",today=today),
            *[_request(blob,mime_type="image/jpeg",model=model,detail=detail,source=f"crop-{i}",today=today) for i,blob in enumerate(crops)],
        )
        result = reconcile_extractions(tuple(passes), today=today)
        result = _reconcile_statement_passes(tuple(passes), result)
        trace = dict(result.trace)
        trace.update({"pipeline":"evidence","preprocessing":"exif-transpose-alpha-white-jpeg+autocontrast-grayscale","quality":asdict(quality),"tile_overlap":.2,"tile_plan":boxes,"submitted_passes":len(passes),"detail":detail})
        result = ReconciliationResult(**{**result.__dict__, "trace": trace, "receipt_level": "L1" if result.accepted else None})
        _CACHE[key] = (time.monotonic(), result)
        _store_durable(key, result)
        return result
