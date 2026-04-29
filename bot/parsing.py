from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta


_AMOUNT_RE = re.compile(r"(?P<amount>-?\d+(?:[\\s.,]\d{3})*(?:[.,]\d+)?)")
_CURRENCY_RE = re.compile(r"(?P<cur>uah|usd|eur|pln|gbp|РіСЂРЅ|в‚ґ|\\$|в‚¬)", re.IGNORECASE)


# NOTE: The initial regexes above were generated with double-escaped sequences
# (e.g. `\\s` / `\\d`) and do not work as intended. We override them here with
# correct patterns.
_AMOUNT_RE = re.compile(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)")
_CURRENCY_RE = re.compile(r"(?P<cur>uah|usd|eur|pln|gbp|РіСЂРЅ|в‚ґ|\$|в‚¬)", re.IGNORECASE)


def _parse_currency_fixed(text: str, default_currency: str) -> str:
    m = _CURRENCY_RE.search(text or "")
    if not m:
        return default_currency
    cur = (m.group("cur") or "").lower()
    if cur in ("РіСЂРЅ", "в‚ґ", "uah"):
        return "UAH"
    if cur in ("$", "usd"):
        return "USD"
    if cur in ("в‚¬", "eur"):
        return "EUR"
    return cur.upper()


def _parse_date_fixed(text: str, default_date: date) -> date:
    t = (text or "").strip().lower()
    if t in ("СЃСЊРѕРіРѕРґРЅС–", "today"):
        return default_date
    if t in ("РІС‡РѕСЂР°", "yesterday"):
        return default_date - timedelta(days=1)
    return parse_date(text, default_date)


def _detect_type_fixed(text: str, default_type: str) -> str:
    t = (text or "").lower()
    if "РїРµСЂРµРєР°Р·" in t or "РїРµСЂРµРІС–РІ" in t or "РїРµСЂРµРІРµР»Р°" in t or "РїРµСЂРµРєРёРЅСѓРІ" in t or "transfer" in t:
        return "transfer"
    if "РґРѕС…С–Рґ" in t or "Р·Р°СЂРїР»Р°С‚" in t or "income" in t:
        return "income"
    if "РІРёС‚СЂР°С‚" in t or "expense" in t:
        return "expense"
    return default_type


def parse_amount(text: str) -> float | None:
    m = _AMOUNT_RE.search(text or "")
    if not m:
        return _parse_amount_words(text or "")
    raw = m.group("amount")
    s = raw.replace(" ", "").replace(",", ".")
    # drop thousands separators like 1.000.50 -> naive
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return _parse_amount_words(text or "")


def _parse_amount_words(text: str) -> float | None:
    """
    UA/RU word-to-number fallback for STT like: "РґРІС–СЃС‚С– РіСЂРёРІРµРЅСЊ".
    Cheap/fast on purpose; supports integers up to thousands.
    """
    t = (text or "").lower()
    if not t:
        return None

    t = t.replace("вЂ™", "'").replace("`", "'")
    tokens = re.findall(r"[a-zР°-СЏС–С—С”Т‘']+", t, flags=re.IGNORECASE)
    if not tokens:
        return None

    units = {
        # UA
        "РЅСѓР»СЊ": 0,
        "РѕРґРёРЅ": 1,
        "РѕРґРЅР°": 1,
        "РѕРґРЅСѓ": 1,
        "РґРІР°": 2,
        "РґРІС–": 2,
        "С‚СЂРё": 3,
        "С‡РѕС‚РёСЂРё": 4,
        "Рї'СЏС‚СЊ": 5,
        "С€С–СЃС‚СЊ": 6,
        "СЃС–Рј": 7,
        "РІС–СЃС–Рј": 8,
        "РґРµРІ'СЏС‚СЊ": 9,
        # RU
        "РЅРѕР»СЊ": 0,
        "РѕРґРЅРѕ": 1,
        "РґРІРµ": 2,
        "С‡РµС‚С‹СЂРµ": 4,
        "РїСЏС‚СЊ": 5,
        "С€РµСЃС‚СЊ": 6,
        "СЃРµРјСЊ": 7,
        "РІРѕСЃРµРјСЊ": 8,
        "РґРµРІСЏС‚СЊ": 9,
    }
    teens = {
        # UA
        "РґРµСЃСЏС‚СЊ": 10,
        "РѕРґРёРЅР°РґС†СЏС‚СЊ": 11,
        "РґРІР°РЅР°РґС†СЏС‚СЊ": 12,
        "С‚СЂРёРЅР°РґС†СЏС‚СЊ": 13,
        "С‡РѕС‚РёСЂРЅР°РґС†СЏС‚СЊ": 14,
        "Рї'СЏС‚РЅР°РґС†СЏС‚СЊ": 15,
        "С€С–СЃС‚РЅР°РґС†СЏС‚СЊ": 16,
        "СЃС–РјРЅР°РґС†СЏС‚СЊ": 17,
        "РІС–СЃС–РјРЅР°РґС†СЏС‚СЊ": 18,
        "РґРµРІ'СЏС‚РЅР°РґС†СЏС‚СЊ": 19,
        # RU
        "РѕРґРёРЅРЅР°РґС†Р°С‚СЊ": 11,
        "РґРІРµРЅР°РґС†Р°С‚СЊ": 12,
        "С‚СЂРёРЅР°РґС†Р°С‚СЊ": 13,
        "С‡РµС‚С‹СЂРЅР°РґС†Р°С‚СЊ": 14,
        "РїСЏС‚РЅР°РґС†Р°С‚СЊ": 15,
        "С€РµСЃС‚РЅР°РґС†Р°С‚СЊ": 16,
        "СЃРµРјРЅР°РґС†Р°С‚СЊ": 17,
        "РІРѕСЃРµРјРЅР°РґС†Р°С‚СЊ": 18,
        "РґРµРІСЏС‚РЅР°РґС†Р°С‚СЊ": 19,
    }
    tens = {
        # UA
        "РґРІР°РґС†СЏС‚СЊ": 20,
        "С‚СЂРёРґС†СЏС‚СЊ": 30,
        "СЃРѕСЂРѕРє": 40,
        "Рї'СЏС‚РґРµСЃСЏС‚": 50,
        "С€С–СЃС‚РґРµСЃСЏС‚": 60,
        "СЃС–РјРґРµСЃСЏС‚": 70,
        "РІС–СЃС–РјРґРµСЃСЏС‚": 80,
        "РґРµРІ'СЏРЅРѕСЃС‚Рѕ": 90,
        # RU
        "РґРІР°РґС†Р°С‚СЊ": 20,
        "С‚СЂРёРґС†Р°С‚СЊ": 30,
        "РїСЏС‚СЊРґРµСЃСЏС‚": 50,
        "С€РµСЃС‚СЊРґРµСЃСЏС‚": 60,
        "СЃРµРјСЊРґРµСЃСЏС‚": 70,
        "РІРѕСЃРµРјСЊРґРµСЃСЏС‚": 80,
        "РґРµРІСЏРЅРѕСЃС‚Рѕ": 90,
    }
    hundreds = {
        # UA
        "СЃС‚Рѕ": 100,
        "РґРІС–СЃС‚С–": 200,
        "С‚СЂРёСЃС‚Р°": 300,
        "С‡РѕС‚РёСЂРёСЃС‚Р°": 400,
        "Рї'СЏС‚СЃРѕС‚": 500,
        "С€С–СЃС‚СЃРѕС‚": 600,
        "СЃС–РјСЃРѕС‚": 700,
        "РІС–СЃС–РјСЃРѕС‚": 800,
        "РґРµРІ'СЏС‚СЃРѕС‚": 900,
        # RU
        "РґРІРµСЃС‚Рё": 200,
        "С‡РµС‚С‹СЂРµСЃС‚Р°": 400,
        "РїСЏС‚СЊСЃРѕС‚": 500,
        "С€РµСЃС‚СЊСЃРѕС‚": 600,
        "СЃРµРјСЊСЃРѕС‚": 700,
        "РІРѕСЃРµРјСЊСЃРѕС‚": 800,
        "РґРµРІСЏС‚СЊСЃРѕС‚": 900,
    }

    total = 0
    current = 0
    saw_number = False

    for tok in tokens:
        tok = tok.strip("'")
        if tok in ("С–", "Р№", "С‚Р°", "Р°"):
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

        if tok in ("С‚РёСЃСЏС‡Р°", "С‚РёСЃСЏС‡С–", "С‚С‹СЃСЏС‡Р°", "С‚С‹СЃСЏС‡Рё"):
            if current == 0:
                current = 1
            total += current * 1000
            current = 0
            saw_number = True
            continue

        if tok.startswith(("РіСЂРЅ", "РіРґРёРІ", "uah", "РґРѕР»", "usd", "eur", "С”РІСЂРѕ", "РµРІСЂРѕ")):
            continue

    total += current
    if not saw_number:
        return None
    if total <= 0:
        return None
    return float(total)


def parse_currency(text: str, default_currency: str) -> str:
    m = _CURRENCY_RE.search(text or "")
    if not m:
        return default_currency
    cur = m.group("cur").lower()
    if cur in ("РіСЂРЅ", "в‚ґ", "uah"):
        return "UAH"
    if cur in ("$", "usd"):
        return "USD"
    if cur == "eur" or cur == "в‚¬":
        return "EUR"
    return cur.upper()


def parse_date(text: str, default_date: date) -> date:
    t = (text or "").strip().lower()
    if t in ("СЃСЊРѕРіРѕРґРЅС–", "today"):
        return date.today()
    if t in ("РІС‡РѕСЂР°", "yesterday"):
        return date.today().fromordinal(date.today().toordinal() - 1)
    # dd.mm.yyyy
    m = re.search(r"(\\d{1,2})[./](\\d{1,2})(?:[./](\\d{2,4}))?", t)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = m.group(3)
        if y:
            y = int(y)
            if y < 100:
                y += 2000
        else:
            y = default_date.year
        try:
            return date(y, mo, d)
        except ValueError:
            return default_date
    return default_date


def detect_type(text: str, default_type: str) -> str:
    t = (text or "").lower()
    if "РїРµСЂРµРєР°Р·" in t or "РїРµСЂРµРІС–РІ" in t or "РїРµСЂРµРІРµР»Р°" in t or "transfer" in t:
        return "transfer"
    if "РґРѕС…С–Рґ" in t or "Р·Р°СЂРїР»Р°С‚" in t or "income" in t:
        return "income"
    if "РІРёС‚СЂР°С‚" in t or "expense" in t:
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
    currency = _parse_currency_fixed(text, default_currency)
    tx_type = _detect_type_fixed(text, default_type)
    tx_date = _parse_date_fixed(text, default_date)
    return TxDraft(
        date=tx_date,
        type=tx_type,
        amount=amount,
        currency=currency,
        comment=(text or "").strip()[:500] or None,
    )
