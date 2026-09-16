from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

import config
from finance import normalize_currency, quantize_money

logger = logging.getLogger("mcf.vision_tx")

_ALLOWED_TYPES = {"expense", "income"}
_ALLOWED_MODES = {"single_tx", "statement_expenses", "unsupported"}
_ALLOWED_DETAILS = {"low", "high", "original", "auto"}
_ALLOWED_CURRENCY_EVIDENCE_KINDS = {"code", "word", "symbol", "mixed", "unknown", ""}
_LEGACY_CURRENCY_ALIASES = {
    "₴": "UAH",
    "ГРН": "UAH",
    "€": "EUR",
    "₺": "TRY",
    "TL": "TRY",
}

# Keep both real Unicode symbols and legacy mis-encoded forms because historical
# tests/payloads have surfaced both shapes.
_CURRENCY_ALIASES = {
    "в‚ґ": "UAH",
    "Р“Р Рќ": "UAH",
    "в‚¬": "EUR",
    "в‚є": "TRY",
    "\u20b4": "UAH",
    "\u0413\u0420\u041d": "UAH",
    "\u20ac": "EUR",
    "\u20ba": "TRY",
    "TL": "TRY",
}
_CONTEXT_CURRENCY_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("UAH", (r"\buah\b", r"\bгрн\b", r"грив", r"hryv")),
    ("TRY", (r"\btry\b", r"\btl\b", r"\blira\b", r"лір", r"лир")),
    ("EUR", (r"\beur\b", r"\beuro\b", r"євро", r"евро")),
    ("USD", (r"\busd\b", r"\bdollar", r"\bдолар", r"\bдоллар")),
)

class OpenAIVisionError(RuntimeError):
    pass


class OpenAIVisionConfigError(OpenAIVisionError):
    pass


class OpenAIVisionInvalidResponseError(OpenAIVisionError):
    pass


@dataclass(frozen=True)
class AccountOption:
    id: int
    label: str


@dataclass(frozen=True)
class CategoryOption:
    id: int
    kind: str
    name: str
    aliases: list[str] | None = None


@dataclass(frozen=True)
class ScreenshotAnalysis:
    transaction_date: date
    type: str | None
    amount: Decimal | None
    currency: str
    account_hint: str | None
    category_hint: str | None
    comment: str | None
    confidence: float
    bank_name: str | None
    card_last4: str | None
    model: str
    detail: str
    used_fallback: bool
    raw_response: dict[str, Any] | None = None
    mode: str = "single_tx"
    dominant_currency: str | None = None
    statement_items: tuple["StatementItemAnalysis", ...] = ()
    skipped_items_count: int = 0
    currency_evidence_kind: str | None = None
    currency_evidence_text: str | None = None
    currency_context_text: str | None = None
    currency_resolution: str | None = None


@dataclass(frozen=True)
class StatementItemAnalysis:
    transaction_date: date
    type: str | None
    amount: Decimal | None
    currency: str
    category_hint: str | None
    comment: str | None
    confidence: float


def is_screenshot_currency_explicit(analysis: ScreenshotAnalysis) -> bool:
    resolution = str(analysis.currency_resolution or "").strip().lower()
    if resolution == "weak_symbol_default":
        return False
    if resolution:
        return True
    evidence_kind = str(analysis.currency_evidence_kind or "").strip().lower()
    return evidence_kind in {"code", "word", "mixed"}


def serialize_miniapp_screenshot_analysis(analysis: ScreenshotAnalysis) -> dict[str, Any]:
    def _item_payload(item: StatementItemAnalysis) -> dict[str, Any]:
        return {
            "transaction_date": item.transaction_date.isoformat(),
            "type": item.type,
            "amount": f"{item.amount:.2f}" if item.amount is not None else None,
            "currency": item.currency,
            "category_hint": item.category_hint,
            "comment": item.comment,
            "confidence": float(item.confidence),
        }

    return {
        "mode": analysis.mode,
        "transaction_date": analysis.transaction_date.isoformat(),
        "type": analysis.type,
        "amount": f"{analysis.amount:.2f}" if analysis.amount is not None else None,
        "currency": analysis.currency,
        "currency_explicit": is_screenshot_currency_explicit(analysis),
        "dominant_currency": analysis.dominant_currency or analysis.currency,
        "account_hint": analysis.account_hint,
        "category_hint": analysis.category_hint,
        "comment": analysis.comment,
        "confidence": float(analysis.confidence),
        "bank_name": analysis.bank_name,
        "card_last4": analysis.card_last4,
        "statement_items": [_item_payload(item) for item in analysis.statement_items],
        "skipped_items_count": int(analysis.skipped_items_count or 0),
    }


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _normalize_text(value: Any, *, limit: int = 500) -> str | None:
    text = str(value or "").strip()
    return text[:limit] or None


def _normalize_type(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text if text in _ALLOWED_TYPES else None


def _normalize_mode(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in _ALLOWED_MODES else "unsupported"


def _normalize_currency_evidence_kind(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text not in _ALLOWED_CURRENCY_EVIDENCE_KINDS:
        return None
    return text or None


def _normalize_amount(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        # Statement screenshots often render expenses with a leading minus sign.
        amount = quantize_money(abs(Decimal(str(value))))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount > 0 else None


def _normalize_currency_value(value: Any, *, default: str = "") -> str:
    normalized = normalize_currency(str(value or "").strip())
    if not normalized:
        return default
    return _CURRENCY_ALIASES.get(normalized, normalized)


def _infer_currency_from_context_text(*values: Any) -> str:
    combined = "\n".join(
        text.lower()
        for value in values
        if (text := _normalize_text(value, limit=500))
    )
    if not combined:
        return ""
    for currency, patterns in _CONTEXT_CURRENCY_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, combined, flags=re.IGNORECASE):
                return currency
    return ""


def _infer_statement_currency(raw_items: Any) -> str:
    if not isinstance(raw_items, list):
        return ""

    currencies: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        amount = _normalize_amount(raw_item.get("amount"))
        if amount is None:
            continue
        tx_type = _normalize_type(raw_item.get("type"))
        if tx_type not in {None, "expense"}:
            continue
        currency = _normalize_currency_value(raw_item.get("currency"))
        if not currency:
            continue
        currencies.add(currency)
        if len(currencies) > 1:
            return ""
    return next(iter(currencies), "")


def _normalize_confidence(value: Any) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(confidence, 1.0))


def _normalize_non_negative_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, number)


def _normalize_date(value: Any, *, today: date) -> date:
    text = str(value or "").strip()
    if not text:
        return today
    try:
        return date.fromisoformat(text)
    except ValueError:
        return today


def _normalize_detail(detail: str | None) -> str:
    normalized = str(detail or "").strip().lower()
    if normalized in _ALLOWED_DETAILS:
        return normalized
    return "low"


def _fallback_detail(detail: str) -> str:
    if detail == "low":
        return "high"
    if detail == "high":
        return "original"
    return detail


def _statement_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "transaction_date": {"type": "string", "description": "ISO date YYYY-MM-DD, or empty string if unknown."},
            "type": {"type": "string", "enum": ["expense", ""], "description": "Expense or empty string."},
            "amount": {"type": ["number", "null"], "description": "Positive expense amount or null."},
            "currency": {"type": "string", "description": "ISO currency code, or empty string if not visible."},
            "category_hint": {
                "type": "string",
                "description": "Short category hint based on the merchant or payment purpose, or empty string.",
            },
            "comment": {
                "type": "string",
                "description": "Short sanitized description for preview. Never include full card numbers or secrets.",
            },
            "confidence": {"type": "number", "description": "Confidence between 0 and 1."},
        },
        "required": [
            "transaction_date",
            "type",
            "amount",
            "currency",
            "category_hint",
            "comment",
            "confidence",
        ],
    }


def _schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "mode_candidate": {
                "type": "string",
                "enum": ["single_tx", "statement_expenses", "unsupported", ""],
                "description": "Best-effort classification of the screenshot.",
            },
            "transaction_date": {"type": "string", "description": "ISO date YYYY-MM-DD, or empty string if unknown."},
            "type": {"type": "string", "enum": ["expense", "income", ""], "description": "Expense, income, or empty string."},
            "amount": {"type": ["number", "null"], "description": "Positive transaction amount or null."},
            "currency": {"type": "string", "description": "ISO currency code, or empty string if not visible."},
            "dominant_currency": {"type": "string", "description": "The dominant currency visible in the screenshot, or empty string."},
            "currency_evidence_kind": {
                "type": "string",
                "enum": ["code", "word", "symbol", "mixed", "unknown", ""],
                "description": "How the screenshot explicitly shows the currency: code, word, symbol, mixed, or unknown.",
            },
            "currency_evidence_text": {
                "type": "string",
                "description": "The exact visible currency token when possible, for example ₴, грн, UAH, ₺, TL, €, or EUR.",
            },
            "currency_context_text": {
                "type": "string",
                "description": "A short visible non-merchant snippet that supports the chosen currency, such as гривневого рахунку, грн, UAH, TL, or EUR.",
            },
            "account_hint": {
                "type": "string",
                "description": "Short hint about the source account, bank, or masked card label. No full card numbers.",
            },
            "category_hint": {
                "type": "string",
                "description": "Short category hint based on the merchant or payment purpose, or empty string.",
            },
            "comment": {
                "type": "string",
                "description": "Short sanitized description for preview. Never include full card numbers or secrets.",
            },
            "confidence": {"type": "number", "description": "Confidence between 0 and 1."},
            "bank_name": {"type": "string", "description": "Visible bank or app name if any, otherwise empty string."},
            "card_last4": {"type": "string", "description": "Only the last 4 digits of a card if visible, otherwise empty string."},
            "skipped_rows_count": {
                "type": "integer",
                "minimum": 0,
                "description": "How many rows were skipped because they were income, transfer, balance, refund, service rows, or unclear.",
            },
            "items": {
                "type": "array",
                "description": "Expense rows extracted from a statement/history screenshot.",
                "items": _statement_item_schema(),
            },
        },
        "required": [
            "mode_candidate",
            "transaction_date",
            "type",
            "amount",
            "currency",
            "dominant_currency",
            "currency_evidence_kind",
            "currency_evidence_text",
            "currency_context_text",
            "account_hint",
            "category_hint",
            "comment",
            "confidence",
            "bank_name",
            "card_last4",
            "skipped_rows_count",
            "items",
        ],
    }


def _prompt(
    *,
    today: date,
    default_currency: str,
    accounts: list[AccountOption],
    expense_categories: list[CategoryOption],
    income_categories: list[CategoryOption],
) -> str:
    account_labels = [item.label for item in accounts][:30]
    expense_names = [item.name for item in expense_categories][:30]
    income_names = [item.name for item in income_categories][:30]
    return (
        "You analyze banking and payment screenshots for a Telegram finance bot.\n"
        "First decide whether the screenshot shows a single transaction, an expense statement/history list, or is unsupported.\n"
        "Set mode_candidate to single_tx, statement_expenses, or unsupported.\n"
        "For single_tx, fill the top-level transaction fields for one transaction.\n"
        "For statement_expenses, return only expense rows in items.\n"
        "Bank history screens with multiple debit rows are supported and should usually be statement_expenses.\n"
        "Skip income, refunds, transfers, cash operations, balances, totals, headers, and unclear rows from items.\n"
        "Count skipped rows in skipped_rows_count.\n"
        "Each statement item must keep its own date and use type expense.\n"
        "If the UI shows debit amounts with a leading minus sign, still return positive amount magnitudes in JSON.\n"
        "If the screenshot uses the hryvnia sign (U+20B4), return UAH. If it uses the Turkish lira sign (U+20BA) or TL, return TRY. If it uses the euro sign (U+20AC), return EUR.\n"
        "Do not confuse U+20B4 hryvnia, U+20BA Turkish lira, and U+20AC euro, even on small screenshots.\n"
        "Do not infer TRY just because merchant names are Turkish. Merchant names, city names, and app language do not count as currency evidence.\n"
        "Set currency_evidence_kind to code, word, symbol, mixed, or unknown.\n"
        "Copy the exact visible currency token into currency_evidence_text whenever possible, for example ₴, грн, UAH, ₺, TL, €, or EUR.\n"
        "If currency is visible only as a small symbol, mark currency_evidence_kind as symbol.\n"
        "If you rely on a symbol or on nearby header/transfer/balance text to choose currency, also copy one short visible non-merchant snippet into currency_context_text, for example гривневого рахунку, грн, UAH, TL, or EUR.\n"
        "If there is only one clear expense on the screenshot, prefer single_tx.\n"
        "If amount or type is not clear, keep amount as null or type as an empty string and lower confidence.\n"
        "Never include full card or account numbers. If visible, keep only last 4 digits in card_last4.\n"
        "Use a short sanitized comment for the preview.\n"
        f"Today is {today.isoformat()}.\n"
        f"Default currency is {default_currency}.\n"
        f"Known account labels: {_json_text(account_labels)}\n"
        f"Known expense categories: {_json_text(expense_names)}\n"
        f"Known income categories: {_json_text(income_names)}\n"
    )


def _build_payload(
    *,
    model: str,
    detail: str,
    prompt: str,
    data_url: str,
) -> dict[str, Any]:
    return {
        "model": model,
        "store": False,
        "max_output_tokens": config.OPENAI_MAX_OUTPUT_TOKENS,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": data_url, "detail": detail},
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "telegram_screenshot_transaction",
                "strict": True,
                "schema": _schema(),
            }
        },
    }


def _extract_text_payload(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = payload.get("output") or []
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()

    raise OpenAIVisionInvalidResponseError("OpenAI response did not contain structured text output")


def _parse_statement_items(
    raw_items: Any,
    *,
    today: date,
    dominant_currency: str,
    default_currency: str,
) -> tuple[tuple[StatementItemAnalysis, ...], int]:
    if not isinstance(raw_items, list):
        return (), 0

    items: list[StatementItemAnalysis] = []
    skipped = 0
    fallback_currency = _normalize_currency_value(dominant_currency or default_currency, default=default_currency) or default_currency
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            skipped += 1
            continue

        tx_type = _normalize_type(raw_item.get("type"))
        if tx_type not in {None, "expense"}:
            skipped += 1
            continue

        amount = _normalize_amount(raw_item.get("amount"))
        if amount is None:
            skipped += 1
            continue

        currency = _normalize_currency_value(raw_item.get("currency"), default=fallback_currency) or fallback_currency
        if currency != fallback_currency:
            skipped += 1
            continue

        category_hint = _normalize_text(raw_item.get("category_hint"), limit=120)
        comment = _normalize_text(raw_item.get("comment"), limit=500) or category_hint
        items.append(
            StatementItemAnalysis(
                transaction_date=_normalize_date(raw_item.get("transaction_date"), today=today),
                type="expense",
                amount=amount,
                currency=currency or default_currency,
                category_hint=category_hint,
                comment=comment,
                confidence=_normalize_confidence(raw_item.get("confidence")),
            )
        )
    return tuple(items), skipped


def _is_valid_analysis(result: ScreenshotAnalysis) -> bool:
    is_statement = result.mode == "statement_expenses" and len(result.statement_items) >= 2
    is_single = result.mode == "single_tx" and result.amount is not None and result.type in _ALLOWED_TYPES
    return is_statement or is_single


def _analysis_currency(result: ScreenshotAnalysis) -> str:
    if result.mode == "statement_expenses" and result.dominant_currency:
        return result.dominant_currency
    return result.currency or result.dominant_currency or ""


def _with_currency(result: ScreenshotAnalysis, *, currency: str, resolution: str | None) -> ScreenshotAnalysis:
    normalized_currency = normalize_currency(currency) or currency
    statement_items = tuple(replace(item, currency=normalized_currency) for item in result.statement_items)
    return replace(
        result,
        currency=normalized_currency,
        dominant_currency=normalized_currency,
        statement_items=statement_items,
        currency_resolution=resolution,
    )


def _needs_currency_retry(result: ScreenshotAnalysis, *, default_currency: str) -> bool:
    normalized_default = normalize_currency(default_currency)
    if normalized_default not in {"UAH", "TRY", "EUR"}:
        return False
    candidate_currency = normalize_currency(_analysis_currency(result))
    if candidate_currency not in {"UAH", "TRY", "EUR"}:
        return False
    return candidate_currency != normalized_default


def _prefer_currency_retry(primary: ScreenshotAnalysis, retry: ScreenshotAnalysis, *, default_currency: str) -> bool:
    if not _is_valid_analysis(retry):
        return False
    normalized_default = normalize_currency(default_currency)
    primary_currency = normalize_currency(_analysis_currency(primary))
    retry_currency = normalize_currency(_analysis_currency(retry))
    if primary_currency == "EUR" and retry_currency in {"UAH", "TRY"}:
        return True
    if (
        normalized_default in {"UAH", "TRY", "EUR"}
        and retry_currency == normalized_default
        and primary_currency != normalized_default
        and retry.confidence >= 0.75
    ):
        return True
    return False


def _should_prefer_default_for_weak_symbol_conflict(result: ScreenshotAnalysis, *, default_currency: str) -> bool:
    normalized_default = normalize_currency(default_currency)
    candidate_currency = normalize_currency(_analysis_currency(result))
    if normalized_default not in {"UAH", "TRY", "EUR"}:
        return False
    if candidate_currency not in {"UAH", "TRY", "EUR"} or candidate_currency == normalized_default:
        return False
    if result.currency_evidence_kind not in {"symbol", "unknown"}:
        return False
    if result.bank_name or result.account_hint or result.card_last4:
        return False
    return True


def _parse_response(
    payload: dict[str, Any],
    *,
    today: date,
    default_currency: str,
    model: str,
    detail: str,
    used_fallback: bool,
) -> ScreenshotAnalysis:
    try:
        raw = json.loads(_extract_text_payload(payload))
    except json.JSONDecodeError as exc:
        raise OpenAIVisionInvalidResponseError(f"OpenAI response was not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise OpenAIVisionInvalidResponseError("OpenAI response JSON was not an object")

    mode_candidate = _normalize_mode(raw.get("mode_candidate"))
    inferred_statement_currency = _infer_statement_currency(raw.get("items"))
    currency_evidence_kind = _normalize_currency_evidence_kind(raw.get("currency_evidence_kind"))
    currency_evidence_text = _normalize_text(raw.get("currency_evidence_text"), limit=32)
    currency_context_text = _normalize_text(raw.get("currency_context_text"), limit=160)
    evidence_currency = _normalize_currency_value(currency_evidence_text)
    dominant_currency = _normalize_currency_value(raw.get("dominant_currency") or raw.get("currency"))
    if evidence_currency:
        dominant_currency = evidence_currency
    if not dominant_currency and inferred_statement_currency:
        dominant_currency = inferred_statement_currency
    effective_dominant_currency = dominant_currency or default_currency
    currency = _normalize_currency_value(raw.get("currency"), default=effective_dominant_currency)
    statement_items, normalized_skipped = _parse_statement_items(
        raw.get("items"),
        today=today,
        dominant_currency=effective_dominant_currency,
        default_currency=default_currency,
    )
    skipped_items_count = _normalize_non_negative_int(raw.get("skipped_rows_count")) + normalized_skipped
    raw_response = raw if config.OPENAI_SCREENSHOT_STORE_RAW else None

    transaction_date = _normalize_date(raw.get("transaction_date"), today=today)
    tx_type = _normalize_type(raw.get("type"))
    amount = _normalize_amount(raw.get("amount"))
    category_hint = _normalize_text(raw.get("category_hint"), limit=120)
    comment = _normalize_text(raw.get("comment"), limit=500)
    context_currency = _infer_currency_from_context_text(
        currency_context_text,
        raw.get("account_hint"),
        raw.get("comment"),
    )

    mode = "unsupported"
    if len(statement_items) >= 2:
        mode = "statement_expenses"
    elif amount is not None and tx_type in _ALLOWED_TYPES:
        mode = "single_tx"
    elif len(statement_items) == 1:
        mode = "single_tx"
        only_item = statement_items[0]
        transaction_date = only_item.transaction_date
        tx_type = "expense"
        amount = only_item.amount
        currency = only_item.currency
        category_hint = category_hint or only_item.category_hint
        comment = comment or only_item.comment
    elif mode_candidate == "single_tx" and amount is not None and tx_type in _ALLOWED_TYPES:
        mode = "single_tx"

    analysis = ScreenshotAnalysis(
        transaction_date=transaction_date,
        type=tx_type,
        amount=amount,
        currency=currency or default_currency,
        account_hint=_normalize_text(raw.get("account_hint"), limit=120),
        category_hint=category_hint,
        comment=comment,
        confidence=_normalize_confidence(raw.get("confidence")),
        bank_name=_normalize_text(raw.get("bank_name"), limit=80),
        card_last4=_normalize_text(raw.get("card_last4"), limit=4),
        model=model,
        detail=detail,
        used_fallback=used_fallback,
        raw_response=raw_response,
        mode=mode,
        dominant_currency=effective_dominant_currency,
        statement_items=statement_items,
        skipped_items_count=skipped_items_count,
        currency_evidence_kind=currency_evidence_kind,
        currency_evidence_text=currency_evidence_text,
        currency_context_text=currency_context_text,
    )
    candidate_currency = normalize_currency(_analysis_currency(analysis))
    item_currency_conflict = any(normalize_currency(item.currency) != context_currency for item in analysis.statement_items)
    if context_currency and (candidate_currency != context_currency or item_currency_conflict):
        resolution = "context_text_override" if currency_evidence_kind == "symbol" else "context_text_inferred"
        return _with_currency(analysis, currency=context_currency, resolution=resolution)
    if context_currency and currency_evidence_kind == "symbol":
        analysis = replace(analysis, currency_resolution="context_text_confirmed")
    if evidence_currency and (
        normalize_currency(_analysis_currency(analysis)) != evidence_currency
        or any(normalize_currency(item.currency) != evidence_currency for item in analysis.statement_items)
    ):
        return _with_currency(analysis, currency=evidence_currency, resolution="evidence_override")
    return analysis


async def _request_analysis(
    image_bytes: bytes,
    *,
    mime_type: str,
    model: str,
    detail: str,
    today: date,
    default_currency: str,
    accounts: list[AccountOption],
    expense_categories: list[CategoryOption],
    income_categories: list[CategoryOption],
    used_fallback: bool,
) -> ScreenshotAnalysis:
    if not config.OPENAI_API_KEY:
        raise OpenAIVisionConfigError("OPENAI_API_KEY is not configured")

    data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"
    prompt = _prompt(
        today=today,
        default_currency=default_currency,
        accounts=accounts,
        expense_categories=expense_categories,
        income_categories=income_categories,
    )
    payload = _build_payload(model=model, detail=detail, prompt=prompt, data_url=data_url)
    headers = {
        "Authorization": f"Bearer {config.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    timeout = max(config.OPENAI_REQUEST_TIMEOUT_MS, 1_000) / 1000.0
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post("https://api.openai.com/v1/responses", headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as exc:
        raise OpenAIVisionError(f"OpenAI vision request failed: {exc}") from exc
    except ValueError as exc:
        raise OpenAIVisionInvalidResponseError(f"OpenAI returned invalid JSON payload: {exc}") from exc

    return _parse_response(
        data,
        today=today,
        default_currency=default_currency,
        model=model,
        detail=detail,
        used_fallback=used_fallback,
    )


def _needs_fallback(result: ScreenshotAnalysis, *, default_currency: str) -> bool:
    if not _is_valid_analysis(result):
        return True
    if _needs_currency_retry(result, default_currency=default_currency):
        return True
    return result.confidence < max(0.0, min(config.OPENAI_SCREENSHOT_CONFIDENCE_THRESHOLD, 1.0))


async def analyze_screenshot(
    image_bytes: bytes,
    *,
    mime_type: str,
    today: date,
    default_currency: str,
    accounts: list[AccountOption],
    expense_categories: list[CategoryOption],
    income_categories: list[CategoryOption],
) -> ScreenshotAnalysis:
    detail = _normalize_detail(config.OPENAI_IMAGE_DETAIL)
    primary_model = config.OPENAI_VISION_MODEL
    fallback_model = config.OPENAI_VISION_FALLBACK_MODEL or primary_model

    result = await _request_analysis(
        image_bytes,
        mime_type=mime_type,
        model=primary_model,
        detail=detail,
        today=today,
        default_currency=default_currency,
        accounts=accounts,
        expense_categories=expense_categories,
        income_categories=income_categories,
        used_fallback=False,
    )
    if not _needs_fallback(result, default_currency=default_currency):
        return result

    retry_detail = _fallback_detail(detail)
    retry_result = await _request_analysis(
        image_bytes,
        mime_type=mime_type,
        model=fallback_model,
        detail=retry_detail,
        today=today,
        default_currency=default_currency,
        accounts=accounts,
        expense_categories=expense_categories,
        income_categories=income_categories,
        used_fallback=True,
    )
    if _prefer_currency_retry(result, retry_result, default_currency=default_currency):
        return retry_result
    if retry_result.confidence >= result.confidence:
        result = retry_result
    elif retry_result.amount is not None and retry_result.type is not None:
        result = retry_result
    if _should_prefer_default_for_weak_symbol_conflict(result, default_currency=default_currency):
        return _with_currency(result, currency=default_currency, resolution="weak_symbol_default")
    logger.info(
        "Vision fallback kept primary result due to weaker retry outcome: primary_conf=%.3f retry_conf=%.3f",
        result.confidence,
        retry_result.confidence,
    )
    return result
