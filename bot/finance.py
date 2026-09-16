from __future__ import annotations

from html import escape as _escape_html
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

SUPPORTED_ACCOUNT_CURRENCIES = ("UAH", "USD", "EUR", "TRY", "USDT")
LEGACY_ACCOUNT_TYPE_ALIASES = {
    "card": "main",
    "bank": "main",
    "crypto": "investment",
}
CREDIT_ACCOUNT_TYPE = "credit"
SUPPORTED_ACCOUNT_TYPES = (
    ("main", "\u0417\u0432\u0438\u0447\u0430\u0439\u043d\u0438\u0439 \u0440\u0430\u0445\u0443\u043d\u043e\u043a"),
    ("cash", "\u0413\u043e\u0442\u0456\u0432\u043a\u0430"),
    ("savings", "\u041d\u0430\u043a\u043e\u043f\u0438\u0447\u0435\u043d\u043d\u044f"),
    ("deposit", "\u0414\u0435\u043f\u043e\u0437\u0438\u0442"),
    ("investment", "\u0406\u043d\u0432\u0435\u0441\u0442\u0438\u0446\u0456\u0439\u043d\u0438\u0439 \u0440\u0430\u0445\u0443\u043d\u043e\u043a"),
    ("credit", "\u041a\u0440\u0435\u0434\u0438\u0442\u043a\u0430"),
    ("other", "\u0406\u043d\u0448\u0435"),
)
ACCOUNT_TYPE_LABELS = {key: label for key, label in SUPPORTED_ACCOUNT_TYPES}
SAVINGS_ACCOUNT_TYPES = ("savings", "deposit", "investment")
ASSET_ACCOUNT_TYPES = ("main", "cash", "savings", "deposit", "investment", "other")
LIABILITY_ACCOUNT_TYPES = (CREDIT_ACCOUNT_TYPE,)

_MONEY_QUANT = Decimal("0.01")
_RATE_QUANT = Decimal("0.0001")
_MAX_MONEY = Decimal("9999999999999999.99")
_CURRENCY_CODE_RX = re.compile(r"^[A-Z]{3,10}$")


@dataclass(frozen=True)
class TransferCalculation:
    target_amount: Decimal
    rate_text: str | None


def normalize_account_name(value: str) -> str:
    return (value or "").strip()


def normalize_currency(value: str) -> str:
    return (value or "").strip().upper()


def is_valid_currency_code(value: str) -> bool:
    return bool(_CURRENCY_CODE_RX.fullmatch(normalize_currency(value)))


def normalize_account_type(value: str) -> str:
    key = (value or "").strip().lower()
    key = LEGACY_ACCOUNT_TYPE_ALIASES.get(key, key)
    return key if key in ACCOUNT_TYPE_LABELS else "other"


def is_credit_account_type(value: str) -> bool:
    return normalize_account_type(value) == CREDIT_ACCOUNT_TYPE


def is_asset_account_type(value: str) -> bool:
    return normalize_account_type(value) in ASSET_ACCOUNT_TYPES


def default_non_negative_account_type(value: str) -> str:
    normalized = normalize_account_type(value)
    if normalized == CREDIT_ACCOUNT_TYPE:
        return "main"
    return normalized


def resolve_runtime_account_type(
    *,
    balance: Decimal | int | float | str,
    non_negative_account_type: str | None,
    current_account_type: str | None = None,
) -> str:
    normalized_balance = quantize_money(balance if isinstance(balance, Decimal) else Decimal(str(balance)))
    if normalized_balance < 0:
        return CREDIT_ACCOUNT_TYPE
    fallback_type = non_negative_account_type or current_account_type or "main"
    return default_non_negative_account_type(fallback_type)


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def quantize_rate(value: Decimal) -> Decimal:
    return value.quantize(_RATE_QUANT, rounding=ROUND_HALF_UP)


def escape_html(text: object) -> str:
    return _escape_html("" if text is None else str(text), quote=False)


def _group_int_part(text: str) -> str:
    sign = ""
    if text.startswith("-"):
        sign = "-"
        text = text[1:]
    if not text:
        return sign + "0"
    return sign + "{:,}".format(int(text)).replace(",", " ")


def format_decimal(value: Decimal | int | float | None, *, places: int = 2) -> str:
    if value is None:
        return "0"
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    quant = _MONEY_QUANT if places == 2 else _RATE_QUANT
    text = format(decimal_value.quantize(quant, rounding=ROUND_HALF_UP), "f")
    if "." not in text:
        return _group_int_part(text)
    int_part, frac_part = text.split(".", 1)
    frac_part = frac_part.rstrip("0")
    if not frac_part:
        return _group_int_part(int_part)
    return f"{_group_int_part(int_part)}.{frac_part}"


def format_money(amount: Decimal | int | float | None, currency: str) -> str:
    return f"{format_decimal(amount)} {normalize_currency(currency) or 'UAH'}"


def format_account_name(account: object) -> str:
    if hasattr(account, "get"):
        label = getattr(account, "get")("label")  # type: ignore[misc]
        currency = getattr(account, "get")("currency")  # type: ignore[misc]
    else:
        label = getattr(account, "label", "")
        currency = getattr(account, "currency", "")
    label_text = escape_html(label).strip() or "—"
    currency_text = normalize_currency(currency)
    if currency_text:
        return f"{label_text} ({currency_text})"
    return label_text


def format_account_line(account: object, base_currency: str | None = None, rate: Decimal | float | int | None = None) -> str:
    if hasattr(account, "get"):
        label = getattr(account, "get")("label")  # type: ignore[misc]
        currency = getattr(account, "get")("currency")  # type: ignore[misc]
        balance = getattr(account, "get")("balance")  # type: ignore[misc]
    else:
        label = getattr(account, "label", "")
        currency = getattr(account, "currency", "")
        balance = getattr(account, "balance", 0)
    currency_text = normalize_currency(currency)
    amount_text = format_money(balance, currency_text)
    line = f"{escape_html(label).strip() or '—'} — <b>{amount_text}</b>"
    base = normalize_currency(base_currency or "")
    if base and base != currency_text and rate is not None:
        converted = quantize_money((balance if isinstance(balance, Decimal) else Decimal(str(balance))) * (rate if isinstance(rate, Decimal) else Decimal(str(rate))))
        line += f" (≈ <b>{format_money(converted, base)}</b>)"
    return line


def format_exchange_rate(from_currency: str, to_currency: str, rate: Decimal | int | float) -> str:
    source = normalize_currency(from_currency)
    target = normalize_currency(to_currency)
    rate_text = format_decimal(rate, places=4)
    if source == "UAH" and target != "UAH":
        return f"1 {target} = <b>{rate_text} UAH</b>"
    if source != "UAH" and target == "UAH":
        return f"1 {source} = <b>{rate_text} UAH</b>"
    return f"1 {source} = <b>{rate_text} {target}</b>"


def parse_decimal_amount(value: str, *, allow_negative: bool = False) -> Decimal | None:
    normalized = (value or "").strip().replace(" ", "").replace(",", ".")
    if not normalized:
        return None
    try:
        amount = Decimal(normalized)
        if not amount.is_finite() or (not allow_negative and amount < 0):
            return None
        amount = quantize_money(amount)
        if abs(amount) > _MAX_MONEY:
            return None
    except InvalidOperation:
        return None
    return amount


def parse_decimal_rate(value: str) -> Decimal | None:
    normalized = (value or "").strip().replace(" ", "").replace(",", ".")
    if not normalized:
        return None
    try:
        rate = Decimal(normalized)
        if not rate.is_finite() or rate <= 0:
            return None
        rate = quantize_rate(rate)
        if rate <= 0:
            return None
    except InvalidOperation:
        return None
    return rate


def account_type_label(account_type: str) -> str:
    return ACCOUNT_TYPE_LABELS.get(normalize_account_type(account_type), ACCOUNT_TYPE_LABELS["other"])


def build_transfer_rate_prompt(source_currency: str, target_currency: str) -> str:
    source = normalize_currency(source_currency)
    target = normalize_currency(target_currency)
    if source == "UAH" and target != "UAH":
        return (
            "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043a\u0443\u0440\u0441.\n\n"
            f"\u0421\u043a\u0456\u043b\u044c\u043a\u0438 UAH \u043a\u043e\u0448\u0442\u0443\u0454 1 {target}?\n\n"
            "\u041d\u0430\u043f\u0440\u0438\u043a\u043b\u0430\u0434:\n40"
        )
    if source != "UAH" and target == "UAH":
        return (
            "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043a\u0443\u0440\u0441.\n\n"
            f"\u0421\u043a\u0456\u043b\u044c\u043a\u0438 UAH \u0434\u0430\u044e\u0442\u044c \u0437\u0430 1 {source}?\n\n"
            "\u041d\u0430\u043f\u0440\u0438\u043a\u043b\u0430\u0434:\n40"
        )
    return (
        "\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u043a\u0443\u0440\u0441 \u0443 \u0444\u043e\u0440\u043c\u0430\u0442\u0456:\n\n"
        f"1 {source} = ? {target}\n\n"
        "\u041d\u0430\u043f\u0440\u0438\u043a\u043b\u0430\u0434:\n0.92"
    )


def build_transfer_rate_text(source_currency: str, target_currency: str, rate: Decimal) -> str:
    return format_exchange_rate(source_currency, target_currency, rate)


def calculate_transfer_amount(
    source_currency: str,
    target_currency: str,
    source_amount: Decimal,
    rate: Decimal | None = None,
) -> TransferCalculation:
    source = normalize_currency(source_currency)
    target = normalize_currency(target_currency)
    amount = quantize_money(source_amount)
    if source == target:
        return TransferCalculation(target_amount=amount, rate_text=None)
    if rate is None or rate <= 0:
        raise ValueError("Rate is required for cross-currency transfers")
    if source == "UAH" and target != "UAH":
        target_amount = quantize_money(amount / rate)
    else:
        target_amount = quantize_money(amount * rate)
    return TransferCalculation(
        target_amount=target_amount,
        rate_text=build_transfer_rate_text(source, target, rate),
    )
