from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import date, datetime
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
    transaction_date: date | None
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
    statement_review_rows: tuple[dict[str, Any], ...] = ()
    skipped_items_count: int = 0
    currency_evidence_kind: str | None = None
    currency_evidence_text: str | None = None
    currency_context_text: str | None = None
    currency_resolution: str | None = None
    currency_candidates: tuple[str, ...] = ()
    receipt_items: tuple["ReceiptItemAnalysis", ...] = ()
    receipt_date: date | None = None
    receipt_merchant: str | None = None
    receipt_total: Decimal | None = None
    field_findings: tuple[str, ...] = ()
    needs_review: bool = False
    arithmetic_warnings: tuple[str, ...] = ()
    source: str = "screenshot"
    field_evidence: dict[str, str | None] | None = None


@dataclass(frozen=True)
class StatementItemAnalysis:
    transaction_date: date | None
    type: str | None
    amount: Decimal | None
    currency: str
    category_hint: str | None
    comment: str | None
    confidence: float
    field_findings: tuple[str, ...] = ()
    needs_review: bool = False
    currency_explicit: bool = False
    field_evidence: dict[str, str | None] | None = None


@dataclass(frozen=True)
class ReceiptItemAnalysis:
    name: str
    quantity: Decimal | None
    unit_price: Decimal | None
    total: Decimal | None
    currency: str
    evidence: str | None
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
            "transaction_date": item.transaction_date.isoformat() if item.transaction_date else None,
            "type": item.type,
            "amount": f"{item.amount:.2f}" if item.amount is not None else None,
            "currency": item.currency,
            "category_hint": item.category_hint,
            "comment": item.comment,
            "confidence": float(item.confidence),
            "field_findings": list(item.field_findings),
            "needs_review": bool(item.needs_review),
            "currency_explicit": bool(item.currency_explicit),
            "field_evidence": dict(item.field_evidence or {}),
        }

    return {
        "mode": analysis.mode,
        "source": analysis.source,
        "field_evidence": dict(analysis.field_evidence or {}),
        "transaction_date": analysis.transaction_date.isoformat() if analysis.transaction_date else None,
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
        "statement_review_rows": [
            {**row,
             "transaction_date": row.get("transaction_date").isoformat() if hasattr(row.get("transaction_date"), "isoformat") else row.get("transaction_date"),
             "amount": f"{row.get('amount'):.2f}" if row.get("amount") is not None else None}
            for row in analysis.statement_review_rows
        ],
        "skipped_items_count": int(analysis.skipped_items_count or 0),
        "receipt_items": [
            {"name": item.name, "quantity": str(item.quantity) if item.quantity is not None else None,
             "unit_price": f"{item.unit_price:.2f}" if item.unit_price is not None else None,
             "amount": f"{item.total:.2f}" if item.total is not None else None,
             "total": f"{item.total:.2f}" if item.total is not None else None,
             "currency": item.currency, "evidence": item.evidence, "confidence": float(item.confidence)}
            for item in analysis.receipt_items
        ],
        "receipt_date": analysis.receipt_date.isoformat() if analysis.receipt_date else None,
        "receipt_merchant": analysis.receipt_merchant,
        "receipt_total": f"{analysis.receipt_total:.2f}" if analysis.receipt_total is not None else None,
        "field_findings": list(analysis.field_findings),
        "needs_review": bool(analysis.needs_review),
        "arithmetic_warnings": list(analysis.arithmetic_warnings),
        "currency_candidates": list(analysis.currency_candidates),
        "field_candidates": dict((analysis.raw_response or {}).get("field_candidates") or {}),
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
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace("−", "-").replace("–", "-").replace("\u00a0", " ").replace("\u202f", " ")
    text = text.removeprefix("+").removeprefix("-")
    # Never silently round, truncate, accept exponents or ambiguous separators.
    if not re.fullmatch(r"[0-9][0-9 .,]*", text):
        return None
    if "," in text and "." in text:
        decimal_sep = "," if text.rfind(",") > text.rfind(".") else "."
        group_sep = "." if decimal_sep == "," else ","
        if not re.fullmatch(rf"[0-9]{{1,3}}(?:{re.escape(group_sep)}[0-9]{{3}})+{re.escape(decimal_sep)}[0-9]{{2}}", text):
            return None
        text = text.replace(group_sep, "").replace(decimal_sep, ".")
    else:
        if " " in text:
            if not re.fullmatch(r"[0-9]{1,3}(?: [0-9]{3})+(?:[.,][0-9]{1,2})?", text):
                return None
            text = text.replace(" ", "")
        if not re.fullmatch(r"[0-9]+(?:[.,][0-9]{1,2})?", text):
            return None
        text = text.replace(",", ".")
    try:
        amount = quantize_money(Decimal(text))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount > 0 else None


def _verified_fields(data: dict[str, Any], *, today: date) -> tuple[Decimal | None, date | None, tuple[str, ...]]:
    amount = _normalize_amount(data.get("amount"))
    tx_date = _normalize_date(data.get("transaction_date"), today=today)
    findings: list[str] = []
    if "amount_text" in data:
        observed = _normalize_amount(data.get("amount_text"))
        if observed is None or amount != observed:
            findings.append("amount_conflict" if observed is not None and amount is not None else "amount_unknown")
            amount = None
    if "date_text" in data:
        evidence = str(data.get("date_text") or "").strip()
        observed_date = None
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                observed_date = datetime.strptime(evidence, fmt).date()
                break
            except ValueError:
                continue
        if observed_date is None and re.fullmatch(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?",
            evidence,
        ):
            try:
                # Preserve the calendar date printed in the evidence, not a
                # timezone-converted date; invalid timestamps remain unknown.
                observed_date = datetime.fromisoformat(evidence).date()
            except ValueError:
                pass
        if observed_date is None:
            observed_date = _normalize_local_date_evidence(evidence)
        if observed_date is None or tx_date != observed_date:
            findings.append("transaction_date_conflict" if observed_date is not None and tx_date is not None else "transaction_date_unknown")
            tx_date = None
    return amount, tx_date, tuple(findings)


def _field_evidence(data: dict[str, Any]) -> dict[str, str | None]:
    return {key: _normalize_text(data.get(key), limit=120)
            for key in ("amount_text", "date_text", "currency_text", "status", "operation_kind")
            if key in data}


def _operation_finding(data: dict[str, Any]) -> str | None:
    kind = str(data.get("operation_kind") or "").strip().lower()
    if kind and kind not in {"expense", "income"}:
        return kind if kind in {"transfer", "refund"} else "type_conflict"
    if kind and kind != _normalize_type(data.get("type")):
        return "type_conflict"
    status = str(data.get("status") or "").strip().lower()
    if status == "unknown":
        return "status_unknown"
    if status and status not in {"posted", "completed", "success", "successful"}:
        return status if status in {"declined", "pending"} else "status_conflict"
    return None


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


def _verified_currency(data: dict[str, Any]) -> tuple[str, tuple[str, ...], str | None]:
    declared = _normalize_currency_value(data.get("currency"))
    evidence = _normalize_currency_value(data.get("currency_text"))
    candidates = tuple(sorted({c for c in (declared, evidence) if c}))
    if not evidence:
        return "", candidates, "currency_unknown"
    if not declared or evidence != declared:
        return "", candidates, "currency_conflict" if declared else "currency_unknown"
    return declared, candidates, None


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


_REJECTED_EVIDENCE = re.compile(
    r"(?i)(?:"
    r"недостатньо\s+коштів|платіж\s+відхилено|платеж\s+отклонен|відхилено|отклонен|"
    r"\binsufficient\s+funds\b|\bnot\s+taken\s+as\s+(?:an?\s+)?expense\b|"
    r"(?:payment|transaction|платіж|платеж)\s+(?:was\s+)?(?:\bdeclined\b|\brejected\b|\bfailed\b)|"
    r"(?:\bdeclined\b|\brejected\b|\bfailed\b)\s+(?:payment|transaction|shown)"
    r")"
)
# Only explicit ownership is safe to discard. Generic card transfers and currency/account
# context are ambiguous in expense-only screenshots and must remain eligible expenses.
_OWN_ACCOUNT_EVIDENCE = re.compile(
    r"(?i)(?:"
    r"поповнення\s+власної\s+банки|на\s+свою\s+картку|на\s+свою\s+карту|"
    r"з\s+білої\s+картки|перевод\s+на\s+свою\s+карту|"
    r"зняття\s+готівки(?:\s+в\s+банкоматі)?|снятие\s+наличных(?:\s+в\s+банкомате)?|"
    r"\bcash\s+withdrawal\b|\bown\s+account\b|\binternal\s+transfer\b"
    r")"
)


def _is_nonexpense_evidence(*values: Any) -> bool:
    evidence = " ".join(str(value or "") for value in values)
    return bool(_REJECTED_EVIDENCE.search(evidence) or _OWN_ACCOUNT_EVIDENCE.search(evidence))


def _normalize_date(value: Any, *, today: date) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_local_date_evidence(text: str) -> date | None:
    """Parse complete literal dates without supplying a missing year or time."""
    match = re.fullmatch(
        r"(?P<day>\d{1,2})(?:\.(?P<month>\d{1,2})\.|\s+(?P<month_name>[а-яіїєґ]+)\s+)"
        r"(?P<year>\d{4})(?:,?\s+(?P<hour>\d{2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?)?",
        text.strip().lower(),
    )
    if match is None:
        return None
    months = ("січня", "лютого", "березня", "квітня", "травня", "червня",
              "липня", "серпня", "вересня", "жовтня", "листопада", "грудня")
    try:
        month = int(match["month"]) if match["month"] else months.index(match["month_name"]) + 1
        return datetime(int(match["year"]), month, int(match["day"]),
                        int(match["hour"] or 0), int(match["minute"] or 0),
                        int(match["second"] or 0)).date()
    except ValueError:
        return None


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


def _evidence_schema() -> dict[str, Any]:
    return {
        "amount_text": {"type": ["string", "null"]},
        "date_text": {"type": ["string", "null"]},
        "currency_text": {"type": ["string", "null"]},
        "status": {"type": "string", "enum": ["posted", "declined", "pending", "unknown"]},
        "operation_kind": {"type": "string", "enum": ["expense", "income", "transfer", "refund", "unknown"]},
    }


def _statement_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "transaction_date": {"type": "string", "description": "ISO date YYYY-MM-DD, or empty string if unknown."},
            **_evidence_schema(),
            "type": {"type": "string", "enum": ["expense", "income", ""], "description": "Candidate classification for this visible row; not permission to book."},
            "amount": {"type": ["number", "null"], "description": "Positive magnitude of this row's booked amount, regardless of operation kind, or null when not legible."},
            "currency": {"type": "string", "description": "ISO currency code for this same row amount, or empty string if not visible."},
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
            *_evidence_schema(),
        ],
    }


def _receipt_item_schema() -> dict[str, Any]:
    return {"type": "object", "additionalProperties": False, "properties": {
        "name": {"type": "string"}, "quantity": {"type": ["number", "null"]},
        "unit_price": {"type": ["number", "null"]}, "total": {"type": ["number", "null"]},
        "currency": {"type": "string"}, "evidence": {"type": "string"}, "confidence": {"type": "number"},
    }, "required": ["name", "quantity", "unit_price", "total", "currency", "evidence", "confidence"]}


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
            **_evidence_schema(),
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
                "description": "How many visible non-transaction rows (for example balances, headers, totals, exchange rates, or service rows) were omitted after exhaustive transcription; income, transfer, refund, pending, and declined operations are not skipped.",
            },
            "items": {
                "type": "array",
                "description": "Every visible transaction row transcribed from a statement/history screenshot before deterministic filtering, including expense, income, transfer, refund, pending, and declined operations.",
                "items": _statement_item_schema(),
            },
            "receipt_date": {"type": "string"}, "receipt_merchant": {"type": "string"},
            "receipt_total": {"type": ["number", "null"]},
            "receipt_items": {"type": "array", "items": _receipt_item_schema()},
            "field_findings": {"type": "array", "items": {"type": "string"}},
            "needs_review": {"type": "boolean"}, "arithmetic_warnings": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "mode_candidate",
            *_evidence_schema(),
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
            "receipt_date", "receipt_merchant", "receipt_total", "receipt_items", "field_findings", "needs_review", "arithmetic_warnings",
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
        "For statement_expenses, transcribe every visible transaction row in items before deterministic filtering, including declined, pending, income, refunds and own-account transfers; classification is not permission to book.\n"
        "Bank history screens with multiple debit rows are supported and should usually be statement_expenses.\n"
        "Never turn balance, cashback accrual, exchange rate, original foreign amount, totals or headers into transaction rows. Count only these excluded non-transaction rows in skipped_rows_count.\n"
        "Copy the visible booked/debited amount digits and separators exactly into amount_text, without the currency; amount must match it without losing zeros.\n"
        "Copy date_text, amount_text, and currency_text as literal verbatim visible source fragments, preserving separators, signs, spacing, and token form; never normalize those evidence fields.\n"
        "Set currency_text to the literal visible currency marker/code for that SAME booked amount; currency is the normalized ISO field. Never pair a local debit with the original foreign currency. If absent use null, never the default or another row's currency.\n"
        "Set date_text to the literal visible date fragment, preserving separators and spacing. Never synthesize a year from today or a day-of-week; if the year is incomplete or absent, keep date_text literal and set transaction_date empty.\n"
        "Set operation_kind to expense, income, transfer, refund or unknown using labels and direction. Set status to posted only for visibly completed bank operations or a completed purchase receipt; pending for uncompleted, declined for rejected/insufficient-funds, otherwise unknown.\n"
        "A red error icon or text such as Недостатньо коштів means rejected/declined: never return it as a completed expense.\n"
        "For explicit own-account transfers (На свою картку, На свою карту, own account, internal transfer), operation_kind=transfer. Generic Переказ на картку is ambiguous: operation_kind=unknown unless other evidence resolves it. Keep these rows for review.\n"
        "Do not classify a row as expense or income from its sign alone.\n"
        "Each statement item must keep its own date, amount and currency. Do not copy document-wide facts over a row.\n"
        "If the UI shows debit amounts with a leading minus sign, still return positive amount magnitudes in JSON.\n"
        "If the screenshot uses the hryvnia sign (U+20B4), return UAH. If it uses the Turkish lira sign (U+20BA) or TL, return TRY. If it uses the euro sign (U+20AC), return EUR.\n"
        "Do not confuse U+20B4 hryvnia, U+20BA Turkish lira, and U+20AC euro, even on small screenshots.\n"
        "Do not infer TRY just because merchant names are Turkish. Merchant names, city names, and app language do not count as currency evidence.\n"
        "Set currency_evidence_kind to code, word, symbol, mixed, or unknown.\n"
        "Copy the exact visible currency token into currency_evidence_text whenever possible, for example ₴, грн, UAH, ₺, TL, €, or EUR.\n"
        "If currency is visible only as a small symbol, mark currency_evidence_kind as symbol.\n"
        "If you rely on a symbol or on nearby header/transfer/balance text to choose currency, also copy one short visible non-merchant snippet into currency_context_text, for example гривневого рахунку, грн, UAH, TL, or EUR.\n"
        "If there is only one clear expense on the screenshot, prefer single_tx.\n"
        "If any fact is missing or unclear, preserve the row and emit null/empty evidence for that fact (amount_text, date_text, currency_text, amount, or type) rather than guessing; lower confidence and keep it for review.\n"
        "Never include full card or account numbers. If visible, keep only last 4 digits in card_last4.\n"
        "Use a short sanitized comment for the preview.\n"
        "For receipts, extract receipt_date, receipt_merchant, receipt_total, and receipt_items only when clearly visible. Never invent missing items. receipt_items is optional L2 evidence and may be empty when valid L1 fields exist. If L2 is unclear or arithmetic does not reconcile, preserve valid L1, set needs_review true, and add a warning.\n"
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
        "max_output_tokens": config.OPENAI_SCREENSHOT_MAX_OUTPUT_TOKENS,
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
    if payload.get("status") == "incomplete":
        raise OpenAIVisionInvalidResponseError("OpenAI returned incomplete financial output")
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
) -> tuple[tuple[StatementItemAnalysis, ...], tuple[dict[str, Any], ...], int]:
    if not isinstance(raw_items, list):
        return (), (), 0

    items: list[StatementItemAnalysis] = []
    review_rows: list[dict[str, Any]] = []
    skipped = 0
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            skipped += 1
            continue

        tx_type = _normalize_type(raw_item.get("type"))
        reason = "type_conflict" if tx_type not in {None, "expense"} else _operation_finding(raw_item)
        if _is_nonexpense_evidence(raw_item.get("category_hint"), raw_item.get("comment")):
            reason = "type_conflict"
        amount, tx_date, findings = _verified_fields(raw_item, today=today)
        currency = _normalize_currency_value(raw_item.get("currency"))
        if "currency_text" in raw_item:
            currency, _, currency_finding = _verified_currency(raw_item)
            if currency_finding:
                findings += (currency_finding,)
        if not currency and "currency_unknown" not in findings and "currency_conflict" not in findings:
            findings += ("currency_unknown",)
        if reason == "status_unknown":
            findings += (reason,)
        elif reason:
            review_rows.append({"transaction_date": tx_date, "amount": amount, "currency": currency,
                                "type": raw_item.get("operation_kind") or tx_type,
                                "comment": _normalize_text(raw_item.get("comment")),
                                "status": _normalize_text(raw_item.get("status")),
                                "included": False, "exclude_reason": reason,
                                "field_evidence": _field_evidence(raw_item),
                                "field_findings": [reason], "needs_review": True})
            skipped += 1
            continue
        category_hint = _normalize_text(raw_item.get("category_hint"), limit=120)
        comment = _normalize_text(raw_item.get("comment"), limit=500) or category_hint
        items.append(
            StatementItemAnalysis(
                transaction_date=tx_date,
                type="expense",
                amount=amount,
                currency=currency,
                category_hint=category_hint,
                comment=comment,
                confidence=_normalize_confidence(raw_item.get("confidence")),
                field_findings=findings,
                needs_review=bool(findings) or amount is None or tx_date is None or not currency,
                currency_explicit="currency_text" in raw_item and bool(currency),
                field_evidence=_field_evidence(raw_item),
            )
        )
    return tuple(items), tuple(review_rows), skipped


def _is_valid_analysis(result: ScreenshotAnalysis) -> bool:
    is_statement = result.mode == "statement_expenses" and bool(result.statement_items or result.statement_review_rows)
    is_single = result.mode == "single_tx" and result.amount is not None and result.type in _ALLOWED_TYPES
    return is_statement or is_single


def _analysis_currency(result: ScreenshotAnalysis) -> str:
    if result.mode == "statement_expenses" and result.dominant_currency:
        return result.dominant_currency
    return result.currency or result.dominant_currency or ""


def _with_currency(result: ScreenshotAnalysis, *, currency: str, resolution: str | None) -> ScreenshotAnalysis:
    normalized_currency = normalize_currency(currency) or currency
    # Document-level evidence cannot establish or overwrite a row's currency.
    statement_items = tuple(
        replace(item, needs_review=True,
                field_findings=tuple(dict.fromkeys((*item.field_findings, "currency_conflict"))))
        if item.currency and item.currency != normalized_currency else item
        for item in result.statement_items
    )
    return replace(
        result,
        currency=normalized_currency,
        dominant_currency=normalized_currency,
        statement_items=statement_items,
        needs_review=result.needs_review or any(item.needs_review for item in statement_items),
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


def _parse_receipt_fields(raw: dict[str, Any], *, today: date, default_currency: str) -> tuple[tuple[ReceiptItemAnalysis, ...], date | None, str | None, Decimal | None, tuple[str, ...], bool, tuple[str, ...]]:
    findings = tuple(str(item).strip()[:200] for item in (raw.get("field_findings") or []) if str(item).strip()) if isinstance(raw.get("field_findings"), list) else ()
    warnings = tuple(str(item).strip()[:200] for item in (raw.get("arithmetic_warnings") or []) if str(item).strip()) if isinstance(raw.get("arithmetic_warnings"), list) else ()
    items: list[ReceiptItemAnalysis] = []
    malformed = False
    for item in raw.get("receipt_items") if isinstance(raw.get("receipt_items"), list) else []:
        if not isinstance(item, dict):
            malformed = True; continue
        name = _normalize_text(item.get("name"), limit=160)
        total = _normalize_amount(item.get("total"))
        if not name or total is None:
            malformed = True; continue
        items.append(ReceiptItemAnalysis(name=name, quantity=_normalize_amount(item.get("quantity")), unit_price=_normalize_amount(item.get("unit_price")), total=total, currency=_normalize_currency_value(item.get("currency"), default=default_currency), evidence=_normalize_text(item.get("evidence"), limit=240), confidence=_normalize_confidence(item.get("confidence"))))
    receipt_date = _normalize_date(raw.get("receipt_date"), today=today) if raw.get("receipt_date") else None
    total = _normalize_amount(raw.get("receipt_total"))
    needs_review = bool(raw.get("needs_review")) or malformed or bool(warnings) or (bool(raw.get("receipt_items")) and total is None)
    if malformed:
        warnings = warnings + ("Some receipt line items were malformed and were omitted.",)
    if items and total is not None:
        summed = sum((item.total for item in items if item.total is not None), Decimal("0"))
        if abs(summed - total) > Decimal("0.02"):
            warnings = warnings + ("Receipt line-item sum does not match receipt total.",)
            needs_review = True
    return tuple(items), receipt_date, _normalize_text(raw.get("receipt_merchant"), limit=160), total, findings, needs_review, warnings


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
    effective_dominant_currency = dominant_currency
    currency = _normalize_currency_value(raw.get("currency"), default=effective_dominant_currency)
    statement_items, statement_review_rows, normalized_skipped = _parse_statement_items(
        raw.get("items"),
        today=today,
        dominant_currency=effective_dominant_currency,
        default_currency=default_currency,
    )
    skipped_items_count = _normalize_non_negative_int(raw.get("skipped_rows_count")) + normalized_skipped
    raw_response = raw if config.OPENAI_SCREENSHOT_STORE_RAW else None
    receipt_items, receipt_date, receipt_merchant, receipt_total, field_findings, needs_review, arithmetic_warnings = _parse_receipt_fields(raw, today=today, default_currency=effective_dominant_currency)

    amount, transaction_date, evidence_findings = _verified_fields(raw, today=today)
    if "date_text" not in raw and transaction_date is None and receipt_date is not None:
        transaction_date = receipt_date
    if mode_candidate == "statement_expenses":
        evidence_findings = tuple(finding for item in statement_items for finding in item.field_findings)
    elif transaction_date is None:
        evidence_findings = tuple(dict.fromkeys((*evidence_findings, "date_unknown")))
    tx_type = _normalize_type(raw.get("type"))
    category_hint = _normalize_text(raw.get("category_hint"), limit=120)
    comment = _normalize_text(raw.get("comment"), limit=500)
    context_currency = _infer_currency_from_context_text(
        currency_context_text,
        raw.get("account_hint"),
        raw.get("comment"),
    )

    if _is_nonexpense_evidence(raw.get("category_hint"), raw.get("comment"), raw.get("account_hint")):
        amount = None
        tx_type = None

    operation_finding = _operation_finding(raw)
    currency_candidates: tuple[str, ...] = ()
    if "currency_text" in raw and mode_candidate != "statement_expenses":
        currency, currency_candidates, currency_finding = _verified_currency(raw)
        effective_dominant_currency = currency
        if currency_finding:
            evidence_findings += (currency_finding,)

    mode = "unsupported"
    if mode_candidate == "statement_expenses" and (statement_items or statement_review_rows):
        mode = "statement_expenses"
    elif len(statement_items) >= 2:
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
    elif mode_candidate == "single_tx" and tx_type in _ALLOWED_TYPES:
        mode = "single_tx"

    if operation_finding and mode_candidate != "statement_expenses":
        if operation_finding != "status_unknown":
            mode, amount = "unsupported", None
        evidence_findings += (operation_finding,)
    field_findings = tuple(dict.fromkeys((*field_findings, *evidence_findings)))

    analysis = ScreenshotAnalysis(
        transaction_date=transaction_date,
        type=tx_type,
        amount=amount,
        currency=currency,
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
        statement_review_rows=statement_review_rows,
        source="statement_transactions" if mode == "statement_expenses" else "screenshot",
        field_evidence=_field_evidence(raw),
        currency_evidence_kind=currency_evidence_kind,
        currency_evidence_text=currency_evidence_text,
        currency_context_text=currency_context_text,
        receipt_items=receipt_items,
        currency_candidates=currency_candidates,
        receipt_date=receipt_date,
        receipt_merchant=receipt_merchant,
        receipt_total=receipt_total,
        field_findings=field_findings,
        needs_review=needs_review or bool(evidence_findings) or any(item.needs_review for item in statement_items),
        arithmetic_warnings=arithmetic_warnings,
    )
    has_financial_amount = analysis.amount is not None or bool(analysis.statement_items) or analysis.receipt_total is not None or bool(analysis.receipt_items)
    if has_financial_amount and not _analysis_currency(analysis):
        analysis = replace(
            analysis,
            needs_review=True,
            field_findings=tuple(dict.fromkeys((*analysis.field_findings, "currency_unknown"))),
        )
    candidate_currency = normalize_currency(_analysis_currency(analysis))
    if "currency_text" in raw or any("currency_text" in row for row in (raw.get("items") or []) if isinstance(row, dict)):
        return analysis
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


def _financial_fingerprint(result: ScreenshotAnalysis) -> tuple[tuple[str, ...], ...]:
    if result.mode != "statement_expenses":
        return ((str(result.type), str(result.amount), result.currency, str(result.transaction_date)),)
    rows = [(str(x.type), str(x.amount), x.currency, str(x.transaction_date), "included") for x in result.statement_items]
    rows.extend((str(x.get("type")), str(x.get("amount")), str(x.get("currency")), str(x.get("transaction_date")), str(x.get("exclude_reason"))) for x in result.statement_review_rows)
    return tuple(sorted(rows))


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
    if config.PHOTO_PIPELINE_ENABLED:
        from photo_shadow import analyze_photo
        evidence = await analyze_photo(image_bytes, mime_type=mime_type, model=config.OPENAI_VISION_MODEL,
                                       detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), today=today)
        receipt_level = evidence.trace.get("document_mode") == "receipt"
        if not evidence.accepted and evidence.trace.get("receipt_review_only") is True:
            # This is a blank manual form, not accepted financial evidence.
            # Do not project disputed amount/date/currency or select an account.
            return ScreenshotAnalysis(
                transaction_date=None, type=None, amount=None, currency=None,
                account_hint=None, category_hint=None, comment=None, confidence=0.0,
                bank_name=None, card_last4=None, model=config.OPENAI_VISION_MODEL,
                detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), used_fallback=False,
                source="receipt", needs_review=True,
                field_findings=tuple(dict.fromkeys((*evidence.findings, "receipt_manual_review"))),
                raw_response={"trace": evidence.trace, "field_candidates": dict(evidence.trace.get("field_candidates") or {})})
        # An unresolved merchant is metadata, not permission to discard two
        # agreeing full-image financial extractions. Keep reconciliation rejected
        # and the merchant blank; only this exact metadata-only conflict can
        # reach a review form. Any financial/status/layout conflict still fails.
        merchant_review = (
            not receipt_level
            and set(evidence.findings) == {"merchant_conflict"}
            and evidence.requires_review
            and evidence.merchant is None
            and evidence.amount is not None
            and evidence.currency is not None
            and evidence.transaction_date is not None
            and evidence.status == "completed"
            and evidence.transaction_type in {"expense", "income"}
        )
        # Rejected bank transactions never become expense suggestions.
        if not evidence.accepted and not merchant_review and evidence.trace.get('screenshot_review_only'):
            candidates = evidence.trace.get('field_candidates') or {}
            amount = None
            if isinstance(candidates.get('amount'), str):
                try:
                    parsed_amount = Decimal(candidates['amount'])
                    if parsed_amount.is_finite() and parsed_amount > 0:
                        amount = parsed_amount
                except (InvalidOperation, ValueError):
                    pass
            currency = candidates.get('currency') if isinstance(candidates.get('currency'), str) else None
            transaction_date = None
            if isinstance(candidates.get('transaction_date'), str):
                try:
                    transaction_date = date.fromisoformat(candidates['transaction_date'])
                except ValueError:
                    pass
            unknown = set(evidence.trace.get('unobserved_fields') or ())
            findings = tuple(dict.fromkeys((*evidence.findings, *(field + '_unknown' for field in sorted(unknown)), 'screenshot_manual_review')))
            return ScreenshotAnalysis(
                transaction_date=transaction_date, type=None, amount=amount, currency=currency,
                account_hint=None, category_hint=candidates.get('category_hint'),
                comment=candidates.get('comment'), confidence=0.0,
                bank_name=None, card_last4=None, model=config.OPENAI_VISION_MODEL,
                detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), used_fallback=False,
                source='screenshot', needs_review=True, field_findings=findings,
                raw_response={'trace': evidence.trace, 'field_candidates': dict(candidates)},
            )
        if not evidence.accepted and not evidence.statement_review_rows and not merchant_review:
            raise OpenAIVisionInvalidResponseError("Photo evidence requires review")
        if not evidence.statement_items and not evidence.statement_review_rows and (evidence.amount is None or evidence.currency is None):
            raise OpenAIVisionInvalidResponseError("Photo evidence requires review")
        if evidence.statement_items:
            items = tuple(StatementItemAnalysis(transaction_date=row["transaction_date"], type="expense", amount=row["amount"],
                currency=row["currency"], category_hint=row.get("category_hint"), comment=row.get("comment"), confidence=row.get("confidence", 0.0))
                for row in evidence.statement_items)
            return ScreenshotAnalysis(transaction_date=items[0].transaction_date, type="expense", amount=None,
                currency=evidence.currency or items[0].currency, account_hint=None, category_hint=None, comment=None,
                confidence=min(item.confidence for item in items), bank_name=None, card_last4=None, model=config.OPENAI_VISION_MODEL,
                detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), used_fallback=False, mode="statement_expenses",
                dominant_currency=evidence.currency or items[0].currency, statement_items=items,
                currency_resolution="full_image_agreement",
                statement_review_rows=evidence.statement_review_rows,
                skipped_items_count=evidence.skipped_items_count, needs_review=evidence.requires_review,
                field_findings=evidence.findings,
                raw_response={"trace": evidence.trace, "raw_evidence": evidence.raw_evidence})
        if evidence.statement_review_rows:
            return ScreenshotAnalysis(
                transaction_date=None, type=None, amount=None, currency=evidence.currency or "",
                account_hint=None, category_hint=None, comment=None, confidence=0.0,
                bank_name=None, card_last4=None, model=config.OPENAI_VISION_MODEL,
                detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), used_fallback=False,
                mode="statement_expenses", dominant_currency=evidence.currency,
                statement_review_rows=evidence.statement_review_rows,
                skipped_items_count=evidence.skipped_items_count, needs_review=True,
                field_findings=evidence.findings,
                raw_response={"trace": evidence.trace, "raw_evidence": evidence.raw_evidence})
        receipt_items = tuple(
            ReceiptItemAnalysis(
                name=str(item.get("description") or item.get("name") or ""),
                quantity=item.get("quantity") if isinstance(item.get("quantity"), Decimal) else None,
                unit_price=item.get("unit_price") if isinstance(item.get("unit_price"), Decimal) else None,
                total=item.get("amount") if isinstance(item.get("amount"), Decimal) else item.get("total") if isinstance(item.get("total"), Decimal) else None,
                currency=str(item.get("currency") or evidence.currency),
                evidence=item.get("evidence") if isinstance(item.get("evidence"), str) else None,
                confidence=1.0,
            )
            for item in evidence.line_items
            if isinstance(item, dict)
        )
        arithmetic_warnings = []
        if any(item.get("receipt_items_incomplete") for item in evidence.raw_evidence):
            arithmetic_warnings.append("receipt_items_incomplete")
        if evidence.receipt_total is not None:
            if evidence.receipt_total != evidence.amount:
                arithmetic_warnings.append("receipt_total_conflict")
            if receipt_items and sum((item.total or Decimal("0") for item in receipt_items), Decimal("0")) != evidence.receipt_total:
                arithmetic_warnings.append("receipt_items_total_mismatch")
        if evidence.transaction_date is None:
            raise OpenAIVisionInvalidResponseError("Photo transaction date requires review")
        return ScreenshotAnalysis(
            transaction_date=evidence.transaction_date, type=evidence.transaction_type,
            amount=evidence.amount, currency=evidence.currency, account_hint=None,
            category_hint=evidence.trace.get("field_candidates", {}).get("category_hint"), comment=evidence.merchant, confidence=1.0,
            bank_name=None, card_last4=None, model=config.OPENAI_VISION_MODEL,
            detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL), used_fallback=False,
            raw_response={"evidence_ids": evidence.evidence_ids, "trace": evidence.trace, "raw_evidence": evidence.raw_evidence},
            source="receipt" if receipt_level else "screenshot",
            currency_resolution="full_image_agreement",
            receipt_items=receipt_items, receipt_date=evidence.transaction_date if receipt_level else None,
            receipt_merchant=evidence.merchant if receipt_level else None, receipt_total=evidence.receipt_total,
            field_findings=evidence.findings, needs_review=evidence.requires_review or bool(arithmetic_warnings),
            arithmetic_warnings=tuple(arithmetic_warnings),
        )
    result = await _analyze_screenshot_legacy(
        image_bytes, mime_type=mime_type, today=today, default_currency=default_currency,
        accounts=accounts, expense_categories=expense_categories, income_categories=income_categories,
    )
    if config.PHOTO_PIPELINE_SHADOW_ENABLED:
        try:
            from photo_shadow import analyze_shadow

            # Bounded, awaited work: no detached tasks retain uploaded photo bytes.
            shadow = await asyncio.wait_for(
                analyze_shadow(image_bytes, mime_type=mime_type, model=config.OPENAI_VISION_MODEL,
                               detail=_normalize_detail(config.OPENAI_IMAGE_DETAIL)),
                timeout=10.0,
            )
            logger.info("Photo shadow accepted=%s findings=%s", shadow.accepted, ",".join(shadow.findings))
        except Exception:
            # Provider errors can contain sensitive source content; never log them.
            logger.warning("Photo shadow unavailable; legacy result preserved")
    return result


async def _analyze_screenshot_legacy(
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
    currency_candidates = list(dict.fromkeys(
        currency for currency in (_analysis_currency(result), _analysis_currency(retry_result)) if currency
    ))
    currency_conflict = len(currency_candidates) > 1 or _should_prefer_default_for_weak_symbol_conflict(result, default_currency=default_currency)
    financial_conflict = _is_valid_analysis(result) and _financial_fingerprint(result) != _financial_fingerprint(retry_result)
    if currency_conflict or financial_conflict:
        # Confidence is not evidence: never silently replace financial facts or
        # lose rows just because another attempt reports a higher confidence.
        # Currency disagreement must not bypass this evidence-preservation rule.
        primary_count = len(result.statement_items) + len(result.statement_review_rows)
        retry_count = len(retry_result.statement_items) + len(retry_result.statement_review_rows)
        chosen = retry_result if _is_valid_analysis(retry_result) and (not _is_valid_analysis(result) or retry_count > primary_count) else result
        flags = tuple(flag for flag, present in (("provider_attempt_conflict", financial_conflict), ("currency_conflict", currency_conflict)) if present)
        changes = {
            "needs_review": True,
            "field_findings": tuple(dict.fromkeys((*chosen.field_findings, *flags))),
            "statement_items": tuple(replace(item, needs_review=True, field_findings=tuple(dict.fromkeys((*item.field_findings, *flags)))) for item in chosen.statement_items),
        }
        if currency_conflict:
            changes.update(currency_evidence_kind="unknown", currency_resolution=None,
                           currency_candidates=tuple(currency_candidates),
                           raw_response={**(chosen.raw_response or {}), "currency_candidates": currency_candidates})
        return replace(chosen, **changes)
    if _prefer_currency_retry(result, retry_result, default_currency=default_currency):
        return retry_result
    if retry_result.confidence >= result.confidence:
        result = retry_result
    elif retry_result.amount is not None and retry_result.type is not None:
        result = retry_result

    logger.info(
        "Vision fallback kept primary result due to weaker retry outcome: primary_conf=%.3f retry_conf=%.3f",
        result.confidence,
        retry_result.confidence,
    )
    return result
