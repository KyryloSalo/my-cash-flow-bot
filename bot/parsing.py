from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta


_AMOUNT_RE = re.compile(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)")
_CURRENCY_RE = re.compile(r"(?P<cur>uah|usd|eur|pln|gbp|грн|₴|\$|€)", re.IGNORECASE)
_DATE_RE = re.compile(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?")


def parse_amount(text: str) -> float | None:
    m = _AMOUNT_RE.search(text or "")
    if not m:
        return None
    raw = m.group("amount")
    s = re.sub(r"\s+", "", raw).replace(",", ".")
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return None


def parse_currency(text: str, default_currency: str) -> str:
    m = _CURRENCY_RE.search(text or "")
    if not m:
        return default_currency
    cur = (m.group("cur") or "").lower()
    if cur in ("грн", "₴", "uah"):
        return "UAH"
    if cur in ("$", "usd"):
        return "USD"
    if cur in ("€", "eur"):
        return "EUR"
    return cur.upper()


def parse_date(text: str, default_date: date) -> date:
    t = (text or "").strip().lower()
    if t in ("сьогодні", "сегодня", "today"):
        return default_date
    if t in ("вчора", "вчера", "yesterday"):
        return default_date - timedelta(days=1)

    m = _DATE_RE.search(t)
    if not m:
        return default_date

    d = int(m.group(1))
    mo = int(m.group(2))
    y_raw = m.group(3)
    if y_raw:
        y = int(y_raw)
        if y < 100:
            y += 2000
    else:
        y = default_date.year

    try:
        return date(y, mo, d)
    except ValueError:
        return default_date


def detect_type(text: str, default_type: str) -> str:
    t = (text or "").lower()

    if any(w in t for w in ("переказ", "перевів", "перевела", "перекинув", "перевод", "transfer")):
        return "transfer"
    if any(w in t for w in ("дохід", "зарплат", "доход", "income")):
        return "income"
    if any(w in t for w in ("витрат", "расход", "expense")):
        return "expense"

    return default_type


@dataclass
class TxDraft:
    date: date
    type: str
    amount: float | None
    currency: str
    category_name: str | None = None
    account_label: str | None = None
    from_account_label: str | None = None
    to_account_label: str | None = None
    comment: str | None = None


def parse_tx(text: str, default_date: date, default_currency: str, default_type: str) -> TxDraft:
    amount = parse_amount(text)
    currency = parse_currency(text, default_currency)
    tx_type = detect_type(text, default_type)
    tx_date = parse_date(text, default_date)
    return TxDraft(
        date=tx_date,
        type=tx_type,
        amount=amount,
        currency=currency,
        comment=(text or "").strip()[:500] or None,
    )
