from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta


_AMOUNT_RE = re.compile(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)")
_CURRENCY_RE = re.compile(r"(?P<cur>uah|usd|eur|pln|gbp|грн|₴|\$|€)", re.IGNORECASE)
_DATE_RE = re.compile(r"(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?")


def _parse_amount_words(text: str) -> float | None:
    """Parse small UA/RU number words from STT (e.g. "двісті гривень")."""
    t = (text or "").lower()
    if not t:
        return None

    t = t.replace("’", "'").replace("`", "'")
    tokens = re.findall(r"[a-zа-яіїєґ']+", t, flags=re.IGNORECASE)
    if not tokens:
        return None

    units = {
        # UA
        "нуль": 0,
        "один": 1,
        "одна": 1,
        "одну": 1,
        "два": 2,
        "дві": 2,
        "три": 3,
        "чотири": 4,
        "п'ять": 5,
        "шість": 6,
        "сім": 7,
        "вісім": 8,
        "дев'ять": 9,
        # RU
        "ноль": 0,
        "одно": 1,
        "две": 2,
        "четыре": 4,
        "пять": 5,
        "шесть": 6,
        "семь": 7,
        "восемь": 8,
        "девять": 9,
    }
    teens = {
        # UA
        "десять": 10,
        "одинадцять": 11,
        "дванадцять": 12,
        "тринадцять": 13,
        "чотирнадцять": 14,
        "п'ятнадцять": 15,
        "шістнадцять": 16,
        "сімнадцять": 17,
        "вісімнадцять": 18,
        "дев'ятнадцять": 19,
        # RU
        "одиннадцать": 11,
        "двенадцать": 12,
        "тринадцать": 13,
        "четырнадцать": 14,
        "пятнадцать": 15,
        "шестнадцать": 16,
        "семнадцать": 17,
        "восемнадцать": 18,
        "девятнадцать": 19,
    }
    tens = {
        # UA
        "двадцять": 20,
        "тридцять": 30,
        "сорок": 40,
        "п'ятдесят": 50,
        "шістдесят": 60,
        "сімдесят": 70,
        "вісімдесят": 80,
        "дев'яносто": 90,
        # RU
        "двадцать": 20,
        "тридцать": 30,
        "сорок": 40,
        "пятьдесят": 50,
        "шестьдесят": 60,
        "семьдесят": 70,
        "восемьдесят": 80,
        "девяносто": 90,
    }
    hundreds = {
        # UA
        "сто": 100,
        "двісті": 200,
        "триста": 300,
        "чотириста": 400,
        "п'ятсот": 500,
        "шістсот": 600,
        "сімсот": 700,
        "вісімсот": 800,
        "дев'ятсот": 900,
        # RU
        "двести": 200,
        "триста": 300,
        "четыреста": 400,
        "пятьсот": 500,
        "шестьсот": 600,
        "семьсот": 700,
        "восемьсот": 800,
        "девятьсот": 900,
    }

    total = 0
    current = 0
    saw_number = False

    for tok in tokens:
        tok = tok.strip("'")
        if tok in ("і", "й", "та", "а"):
            continue

        if tok in hundreds:
            current += hundreds[tok]
            saw_number = True
            continue
        if tok in tens:
            current += tens[tok]
            saw_number = True
            continue
        if tok in teens:
            current += teens[tok]
            saw_number = True
            continue
        if tok in units:
            current += units[tok]
            saw_number = True
            continue

        if tok in ("тисяча", "тисячі", "тысяча", "тысячи"):
            if current == 0:
                current = 1
            total += current * 1000
            current = 0
            saw_number = True
            continue

        if tok.startswith(("грн", "грив", "uah", "дол", "usd", "eur", "євро", "евро")):
            continue

    total += current
    if not saw_number:
        return None
    if total <= 0:
        return None
    return float(total)


def parse_amount(text: str) -> float | None:
    m = _AMOUNT_RE.search(text or "")
    if not m:
        return _parse_amount_words(text or "")
    raw = m.group("amount")
    s = re.sub(r"\s+", "", raw).replace(",", ".")
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return _parse_amount_words(text or "")


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
