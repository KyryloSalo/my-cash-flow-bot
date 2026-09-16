from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

from normalization_service import match_expense_normalization

Intent = Literal["expense", "income", "transfer", "debt"]
DebtAction = Literal["lend", "borrow", "receive_repayment", "make_repayment"]

AccountKey = Literal["monobank", "privatbank", "raiffeisen", "cash"]
CategoryKey = Literal[
    "other",
    "groceries",
    "cafes_restaurants_delivery",
    "rent",
    "utilities",
    "home_household",
    "transport",
    "taxi",
    "telecom_internet",
    "health",
    "clothes_shoes",
    "beauty_care",
    "subscriptions_services",
    "entertainment_leisure",
    "gifts_donations",
    "salary",
    "client_payment",
]

_GENERIC_CURRENCY_AFTER_AMOUNT_RE = re.compile(
    r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)\s*(?P<currency>[A-Z]{3,10})\b"
)
_GENERIC_CURRENCY_BEFORE_AMOUNT_RE = re.compile(
    r"\b(?P<currency>[A-Z]{3,10})\s*(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)"
)

_AMOUNT_RE = re.compile(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)")
_SIGNED_PREFIX_AMOUNT_RE = re.compile(
    r"^\s*(?P<sign>[+-])\s*(?P<amount>\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)(?=\D|$)"
)

_CURRENCY_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            r"(?i)\b(uah|грн|гривня|гривні|гривню|гривнею|гривень|гривен|гривна|гривны|гривну|гривной)\b"
        ),
        "UAH",
    ),
    (re.compile(r"(?i)(\b(usd|долар(ів)?|бакс(ів)?)\b|\$)"), "USD"),
    (re.compile(r"(?i)(\b(eur|євро)\b|€)"), "EUR"),
    (re.compile(r"(?i)\b(usdt|tether)\b"), "USDT"),
    (re.compile(r"(?i)(\b(try|tl|лір(а|и|у|ам|ами)?|лир(а|ы|у|ам|ами)?|турецька\s+ліра|турецкая\s+лира)\b|₺)"), "TRY"),
]

_CURRENCY_PATTERNS[1] = (
    re.compile(r"(?i)(\b(usd|долар(ів|а)?|доллар(ов|а)?|бакс(ів|ов)?)\b|\$)"),
    "USD",
)
_CURRENCY_PATTERNS[2] = (
    re.compile(r"(?i)(\b(eur|євро|евро)\b|€)"),
    "EUR",
)

_DATE_RELATIVE: dict[str, int] = {
    "today": 0,
    "сьогодні": 0,
    "сегодня": 0,
    "yesterday": -1,
    "вчора": -1,
    "вчера": -1,
    "позавчора": -2,
    "позавчера": -2,
}

MINIAPP_TEXT_MAX_LENGTH = 500

_UA_MONTHS: dict[str, int] = {
    "січня": 1,
    "лютого": 2,
    "березня": 3,
    "квітня": 4,
    "травня": 5,
    "червня": 6,
    "липня": 7,
    "серпня": 8,
    "вересня": 9,
    "жовтня": 10,
    "листопада": 11,
    "грудня": 12,
}

_RU_MONTHS: dict[str, int] = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

_TRANSFER_RE = re.compile(
    r"(?i)\b(?:переказ\w*|перекин\w*|перев(?:ести|еди|ів|ела|од\w*)|transfer\w*)\b"
)
_DEBT_RE = re.compile(r"(?i)\b(борг|в борг)\b")
_INCOME_HINT_RE = re.compile(r"(?i)\b(зарплат[а-яіїєґ]*|дохід|прибуток|клієнт оплатив|оплата від клієнта|зайшло)\b")
_EXPENSE_HINT_RE = re.compile(r"(?i)\b(купив|купила|оплатив|оплатила|витратив|витратила|заплатив|заплатила)\b")

_INCOME_HINT_RE = re.compile(
    r"(?i)\b("
    r"зарплат[а-яієїґ]*|дохід|доход|прибуток|"
    r"клієнт оплатив|клиент оплатил|"
    r"оплата від клієнта|оплата от клиента|"
    r"зайшло|пришло|пришла|"
    r"отримав|отримала|получил|получила"
    r")\b"
)

_ACCOUNT_KEY_PATTERNS: list[tuple[re.Pattern[str], AccountKey]] = [
    (re.compile(r"(?i)\b(моно|монобанк[а-яіїєґ]*|mono|monobank)\b"), "monobank"),
    (re.compile(r"(?i)\b(приват|приватбанк[а-яіїєґ]*|privat|privatbank)\b"), "privatbank"),
    (re.compile(r"(?i)\b(райф|райффайзен[а-яіїєґ]*|raif|raiffeisen)\b"), "raiffeisen"),
    (re.compile(r"(?i)\b(готівка|наличка|кеш|cash)\b"), "cash"),
]

_CATEGORY_KEY_PATTERNS_INCOME: list[tuple[re.Pattern[str], CategoryKey]] = [
    (re.compile(r"(?i)\bзарплат[а-яіїєґ]*\b"), "salary"),
    (re.compile(r"(?i)\b(клієнт оплатив|оплата від клієнта|client)\b"), "client_payment"),
]


_CATEGORY_KEY_PATTERNS_INCOME[1] = (
    re.compile(r"(?i)\b(клієнт оплатив|клиент оплатил|оплата від клієнта|оплата от клиента|client)\b"),
    "client_payment",
)

_CATEGORY_KEY_PATTERNS_EXPENSE: list[tuple[re.Pattern[str], CategoryKey]] = [
    (re.compile(r"(?i)\b(taxi|uber|bolt taxi|bolt|bitaksi|uklon|таксі|такси)\b"), "taxi"),
    (re.compile(r"(?i)\b(internet|mobile|phone|vodafone|kyivstar|lifecell|turkcell|turk telekom|türk telekom|інтернет|зв[’']?язок)\b"), "telecom_internet"),
    (re.compile(r"(?i)\b(subscription|apple|google|netflix|spotify|youtube|chatgpt|openai|hosting|domain|підписк)\b"), "subscriptions_services"),
    (re.compile(r"(?i)\b(pharmacy|аптека|doctor|лікар|clinic|hospital|стоматолог|аналізи)\b"), "health"),
    (re.compile(r"(?i)\b(rent|kira|оренда|аренда|квартира|landlord)\b"), "rent"),
    (re.compile(r"(?i)\b(utilities|fatura|electricity|water|gas|комуналка|коммуналка|світл[ао]?|вода|газ|опалення|квартплата|осбб|aidat)\b"), "utilities"),
    (re.compile(r"(?i)\b(супермаркет|market|grocery|groceries|migros|bim|a101|sok|şok|продукт(и|і)|атб|сільпо|магазин|хліб|хлеб|молоко)\b"), "groceries"),
    (re.compile(r"(?i)\b(cafe|restaurant|restoran|glovo|bolt food|yemeksepeti|кава|кофе|кафе|ресторан|доставка їжі)\b"), "cafes_restaurants_delivery"),
    (re.compile(r"(?i)\b(побут|household|home|ikea|ремонт|посуд|меблі|хімія)\b"), "home_household"),
    (re.compile(r"(?i)\b(metro|автобус|bus|tram|train|fuel|бензин|пальне|parking|паркування|транспорт)\b"), "transport"),
    (re.compile(r"(?i)\b(clothes|clothing|shoes|sneakers|zara|hm|lc waikiki|defacto|одяг|взуття)\b"), "clothes_shoes"),
    (re.compile(r"(?i)\b(barber|barbershop|salon|косметика|манікюр|стрижка|догляд)\b"), "beauty_care"),
    (re.compile(r"(?i)\b(cinema|кіно|games|steam|concert|gym|спорт|хобі|подорож|відпочинок)\b"), "entertainment_leisure"),
    (re.compile(r"(?i)\b(gift|donation|donate|подарунок|донат|благодійність|збір)\b"), "gifts_donations"),
]


def _norm(text: str) -> str:
    return (text or "").strip().lower()


def parse_amount(text: str) -> float | None:
    m = _AMOUNT_RE.search(text or "")
    if not m:
        return _parse_amount_words(text or "")
    raw = m.group("amount")
    s = raw.replace(" ", "").replace(",", ".")
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        value = float(s)
    except ValueError:
        return _parse_amount_words(text or "")
    if value <= 0:
        return None
    return value


def _parse_prefixed_signed_tx_amount(text: str) -> tuple[float, Intent] | None:
    match = _SIGNED_PREFIX_AMOUNT_RE.match(text or "")
    if not match:
        return None

    raw = str(match.group("amount") or "")
    s = raw.replace(" ", "").replace(",", ".")
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        value = float(s)
    except ValueError:
        return None
    if value <= 0:
        return None

    sign = str(match.group("sign") or "")
    if sign == "-":
        return value, "expense"
    if sign == "+":
        return value, "income"
    return None


def _parse_amount_words(text: str) -> float | None:
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
        "один": 1,
        "одна": 1,
        "одну": 1,
        "два": 2,
        "ноль": 0,
        "одно": 1,
        "две": 2,
        "три": 3,
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

        if tok in ("тисяча", "тисячі", "тисячу", "тисяч", "тысяча", "тысячи", "тысячу", "тысяч"):
            if current == 0:
                current = 1
            total += current * 1000
            current = 0
            saw_number = True
            continue

        # ignore currency words / fillers
        if tok.startswith(("грн", "грив", "uah", "дол", "usd", "eur", "євро", "евро", "бакс")):
            continue

    total += current
    if not saw_number:
        return None
    if total <= 0:
        return None
    return float(total)


def parse_currency_info(text: str, default_currency: str = "UAH") -> tuple[str, bool]:
    t = text or ""
    for rx, cur in _CURRENCY_PATTERNS:
        if rx.search(t):
            return cur, True
    for rx in (_GENERIC_CURRENCY_AFTER_AMOUNT_RE, _GENERIC_CURRENCY_BEFORE_AMOUNT_RE):
        match = rx.search(t)
        if match:
            return str(match.group("currency") or "").upper(), True
    return default_currency, False


def parse_currency(text: str, default_currency: str = "UAH") -> str:
    currency, _currency_explicit = parse_currency_info(text, default_currency=default_currency)
    return currency


def _extract_date(text: str, *, today: date) -> tuple[date | None, float]:
    t = _norm(text)
    if not t:
        return today, 1.0

    for key, delta in _DATE_RELATIVE.items():
        if re.search(rf"(?i)\b{re.escape(key)}\b", t):
            return today + timedelta(days=delta), 1.0

    m = re.search(r"(?P<d>\d{1,2})[./](?P<m>\d{1,2})(?:[./](?P<y>\d{2,4}))?", t)
    if m:
        d = int(m.group("d"))
        mo = int(m.group("m"))
        y_raw = m.group("y")
        y = today.year
        if y_raw:
            y = int(y_raw)
            if y < 100:
                y += 2000
        try:
            return date(y, mo, d), 1.0
        except ValueError:
            return None, 0.0

    m2 = re.search(r"(?i)\b(?P<d>\d{1,2})\s+(?P<mon>[a-zа-яіїє]+)\b", t)
    if m2:
        d = int(m2.group("d"))
        mon = m2.group("mon").lower()
        mo = _UA_MONTHS.get(mon) or _RU_MONTHS.get(mon)
        if not mo:
            return None, 0.0
        try:
            return date(today.year, mo, d), 1.0
        except ValueError:
            return None, 0.0

    return today, 1.0


def _detect_account_key(text: str) -> tuple[AccountKey | None, float]:
    t = text or ""
    for rx, key in _ACCOUNT_KEY_PATTERNS:
        if rx.search(t):
            return key, 0.9
    return None, 0.0


def _detect_category_key(text: str, intent: Intent) -> tuple[CategoryKey | None, float]:
    if intent == "expense":
        normalized_match = match_expense_normalization(text)
        if normalized_match is not None:
            return normalized_match.matched_slug, float(normalized_match.confidence)
    patterns = _CATEGORY_KEY_PATTERNS_INCOME if intent == "income" else _CATEGORY_KEY_PATTERNS_EXPENSE
    for rx, key in patterns:
        if rx.search(text or ""):
            return key, 0.9
    return None, 0.0


def _detect_intent(text: str, default_intent: Intent = "expense") -> tuple[Intent, float]:
    t = text or ""
    if _TRANSFER_RE.search(t):
        return "transfer", 0.95
    if _DEBT_RE.search(t) or re.search(r"(?i)\b(дав|дала|взяв|взяла|позичив|позичила|повернув|повернула|віддав|віддала)\b", t):
        return "debt", 0.9
    if _INCOME_HINT_RE.search(t):
        return "income", 0.9
    if _EXPENSE_HINT_RE.search(t):
        return "expense", 0.8
    if parse_amount(t) is not None:
        return default_intent, 0.6
    return default_intent, 0.3


def _strip_extracted_bits(
    text: str,
    *,
    remove_amount: bool,
    remove_currency: bool,
    remove_accounts: bool,
    remove_dates: bool,
) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    if remove_dates:
        for key in _DATE_RELATIVE.keys():
            t = re.sub(rf"(?i)\b{re.escape(key)}\b", " ", t)
        t = re.sub(r"(?i)\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", " ", t)
        t = re.sub(r"(?i)\b\d{1,2}\s+[a-zа-яіїє]+\b", " ", t)

    if remove_amount:
        t = _AMOUNT_RE.sub(" ", t)

    if remove_currency:
        for rx, _cur in _CURRENCY_PATTERNS:
            t = rx.sub(" ", t)

    if remove_accounts:
        for rx, _key in _ACCOUNT_KEY_PATTERNS:
            t = rx.sub(" ", t)
        t = re.sub(r"(?i)\b(з|із|на|у|в)\b", " ", t)

    t = re.sub(r"\s+", " ", t).strip(" ,.;:!?+-\u2013\u2014\t\n\r")
    return t.strip()


def _clean_comment(raw_text: str) -> str | None:
    t = (raw_text or "").strip()
    if not t:
        return None

    has_amount = parse_amount(t) is not None or _parse_prefixed_signed_tx_amount(t) is not None
    cleaned = _strip_extracted_bits(
        t,
        remove_amount=has_amount,
        remove_currency=has_amount,
        remove_accounts=has_amount,
        remove_dates=has_amount,
    )

    if has_amount:
        if not cleaned:
            return None
        if " " not in cleaned:
            return cleaned.lower()
        return cleaned.strip()
    return t.strip()


def _extract_transfer_accounts(text: str) -> tuple[AccountKey | None, AccountKey | None, float]:
    t = text or ""
    m = re.search(r"(?i)\bз\s+(?P<from>[^,;.]+?)\s+на\s+(?P<to>[^,;.]+?)\b", t)
    if m:
        from_key, _ = _detect_account_key(m.group("from"))
        to_key, _ = _detect_account_key(m.group("to"))
        conf = 0.9 if (from_key or to_key) else 0.0
        return from_key, to_key, conf
    return None, None, 0.0


def _normalize_person_name(token: str) -> str | None:
    name = (token or "").strip().strip(",.;:!?'\"“”()[]{}")
    if not name:
        return None
    lower = name.lower()
    if lower.endswith("і") and len(lower) >= 3:
        # "Саші" -> "Саша" (rough UA dative -> nominative).
        name = name[:-1] + "а"
    return name[:1].upper() + name[1:]


def _extract_debt_fields(text: str) -> tuple[DebtAction | None, str | None]:
    t = text or ""
    t_norm = _norm(t)
    if not t_norm:
        return None, None

    if re.search(r"(?i)\bдав\b", t):
        m = re.search(r"(?i)\bдав\s+(?P<name>[a-zа-яіїє]+)", t_norm)
        return "lend", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\bвзяв\b", t):
        m = re.search(r"(?i)\bвзяв\s+у\s+(?P<name>[a-zа-яіїє]+)", t_norm)
        return "borrow", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\bповернув\b", t):
        m = re.search(r"(?i)\bповернув\s+(?P<name>[a-zа-яіїє]+)", t_norm)
        if m:
            return "make_repayment", _normalize_person_name(m.group("name"))
        m2 = re.search(r"(?i)\b(?P<name>[a-zа-яіїє]+)\s+повернув\b", t_norm)
        if m2:
            return "receive_repayment", _normalize_person_name(m2.group("name"))

    m3 = re.search(r"(?i)^(?P<name>[a-zа-яіїє]+)\b", t_norm)
    person = _normalize_person_name(m3.group("name") if m3 else "")
    return None, person


_DEBT_KEYWORDS_SAFE = ("борг", "в борг", "позич", "поверн", "дав", "дала", "взяв", "взяла", "віддав", "віддала")
_DEBT_LEND_WORDS_SAFE = {"дав", "дала", "давав", "давала"}
_DEBT_BORROW_WORDS_SAFE = {"взяв", "взяла", "позичив", "позичила", "позичився", "позичилась"}
_DEBT_REPAY_WORDS_SAFE = {"повернув", "повернула", "віддав", "віддала"}


def _detect_intent_safe(text: str, default_intent: Intent = "expense") -> tuple[Intent, float]:
    raw = text or ""
    lower = raw.lower()
    if _TRANSFER_RE.search(raw):
        return "transfer", 0.95
    if any(keyword in lower for keyword in _DEBT_KEYWORDS_SAFE):
        return "debt", 0.9
    if _INCOME_HINT_RE.search(raw):
        return "income", 0.9
    if _EXPENSE_HINT_RE.search(raw):
        return "expense", 0.8
    if parse_amount(raw) is not None:
        return default_intent, 0.6
    return default_intent, 0.3


def _extract_debt_fields_safe(text: str) -> tuple[DebtAction | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return None, None
    lower = raw.lower()
    if not any(keyword in lower for keyword in _DEBT_KEYWORDS_SAFE):
        return None, None

    tokens = [token.strip(",.;:!?()[]{}\"'") for token in lower.split()]
    tokens = [token for token in tokens if token]
    if not tokens:
        return None, None

    def name_at(index: int) -> str | None:
        if 0 <= index < len(tokens):
            return _normalize_person_name(tokens[index])
        return None

    if tokens[0] in _DEBT_LEND_WORDS_SAFE:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "lend", name_at(2)
        return "lend", name_at(1)

    if tokens[0] in _DEBT_BORROW_WORDS_SAFE:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "borrow", name_at(2)
        return "borrow", name_at(1)

    for index, token in enumerate(tokens):
        if token in _DEBT_REPAY_WORDS_SAFE:
            if index == 0:
                return "make_repayment", name_at(1)
            return "receive_repayment", name_at(index - 1)

    return None, _normalize_person_name(tokens[0])


_DEBT_KEYWORDS_FINAL = ("борг", "в борг", "позич", "поверн", "дав", "дала", "взяв", "взяла", "віддав", "віддала")
_DEBT_LEND_WORDS_FINAL = {"дав", "дала", "давав", "давала"}
_DEBT_BORROW_WORDS_FINAL = {"взяв", "взяла", "позичив", "позичила", "позичився", "позичилась"}
_DEBT_REPAY_WORDS_FINAL = {"повернув", "повернула", "віддав", "віддала"}


def _detect_intent(text: str, default_intent: Intent = "expense") -> tuple[Intent, float]:
    raw = text or ""
    lower = raw.lower()
    if _TRANSFER_RE.search(raw):
        return "transfer", 0.95
    if any(keyword in lower for keyword in _DEBT_KEYWORDS_FINAL):
        return "debt", 0.9
    if _INCOME_HINT_RE.search(raw):
        return "income", 0.9
    if _EXPENSE_HINT_RE.search(raw):
        return "expense", 0.8
    if parse_amount(raw) is not None:
        return default_intent, 0.6
    return default_intent, 0.3


def _extract_debt_fields(text: str) -> tuple[DebtAction | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return None, None
    lower = raw.lower()
    if not any(keyword in lower for keyword in _DEBT_KEYWORDS_FINAL):
        return None, None

    tokens = [token.strip(",.;:!?()[]{}\"'") for token in lower.split()]
    tokens = [token for token in tokens if token]
    if not tokens:
        return None, None

    def name_at(index: int) -> str | None:
        if 0 <= index < len(tokens):
            return _normalize_person_name(tokens[index])
        return None

    if tokens[0] in _DEBT_LEND_WORDS_FINAL:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "lend", name_at(2)
        return "lend", name_at(1)

    if tokens[0] in _DEBT_BORROW_WORDS_FINAL:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "borrow", name_at(2)
        return "borrow", name_at(1)

    for index, token in enumerate(tokens):
        if token in _DEBT_REPAY_WORDS_FINAL:
            if index == 0:
                return "make_repayment", name_at(1)
            return "receive_repayment", name_at(index - 1)

    return None, _normalize_person_name(tokens[0])


_DEBT_RE_EXTENDED = re.compile(r"(?i)\b(борг|в борг|позич|поверн|віддав|віддала)\b")


def _extract_debt_fields(text: str) -> tuple[DebtAction | None, str | None]:
    t = text or ""
    t_norm = _norm(t)
    if not t_norm or not _DEBT_RE_EXTENDED.search(t):
        return None, None

    if re.search(r"(?i)\b(дав|дала|давав|давала)\b", t):
        m = re.search(r"(?i)\b(дав|дала|давав|давала)\s+(?P<name>[a-zа-яіїєґ]+)", t_norm)
        return "lend", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\b(взяв|взяла|позичив|позичила|позичився|позичилась)\b", t):
        m = re.search(
            r"(?i)\b(?:взяв|взяла|позичив|позичила|позичився|позичилась)\s+(?:у\s+)?(?P<name>[a-zа-яіїєґ]+)",
            t_norm,
        )
        return "borrow", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\b(повернув|повернула|віддав|віддала)\b", t):
        m = re.search(
            r"(?i)\b(?:повернув|повернула|віддав|віддала)\s+(?P<name>[a-zа-яіїєґ]+)",
            t_norm,
        )
        if m:
            return "make_repayment", _normalize_person_name(m.group("name"))
        m2 = re.search(
            r"(?i)\b(?P<name>[a-zа-яіїєґ]+)\s+(?:повернув|повернула|віддав|віддала)\b",
            t_norm,
        )
        if m2:
            return "receive_repayment", _normalize_person_name(m2.group("name"))

    m3 = re.search(r"(?i)^(?P<name>[a-zа-яіїєґ]+)\b", t_norm)
    person = _normalize_person_name(m3.group("name") if m3 else "")
    return None, person


@dataclass(frozen=True)
class ParsedMessage:
    intent: Intent
    debt_action: DebtAction | None
    amount: float | None
    currency: str
    currency_explicit: bool
    account_id: AccountKey | None
    from_account_id: AccountKey | None
    to_account_id: AccountKey | None
    category_id: CategoryKey | None
    person_id: str | None
    comment: str | None
    raw_text: str
    date: date | None
    missing_fields: list[str]
    confidence: dict[str, float]

    @property
    def is_candidate_tx(self) -> bool:
        if self.intent in {"income", "transfer", "debt"}:
            return True
        if self.amount is not None:
            return True
        if self.category_id is not None:
            return True
        return self.confidence.get("intent", 0.0) >= 0.75


def parse_message(text: str, *, today: date, default_currency: str = "UAH") -> ParsedMessage:
    raw = (text or "").strip()
    signed_prefixed_amount = _parse_prefixed_signed_tx_amount(raw)
    intent, intent_conf = _detect_intent_safe(raw, default_intent="expense")

    amount = parse_amount(raw)
    if signed_prefixed_amount is not None and intent in {"expense", "income"}:
        signed_amount, signed_intent = signed_prefixed_amount
        intent = signed_intent
        intent_conf = max(intent_conf, 0.95)
        if amount is None:
            amount = signed_amount
    currency, currency_explicit = parse_currency_info(raw, default_currency=default_currency)
    tx_date, date_conf = _extract_date(raw, today=today)

    account_key, account_conf = _detect_account_key(raw)

    from_key = None
    to_key = None
    transfer_acc_conf = 0.0
    if intent == "transfer":
        from_key, to_key, transfer_acc_conf = _extract_transfer_accounts(raw)
        if from_key is None and to_key is None:
            from_key, _ = _detect_account_key(raw)

    category_key = None
    cat_conf = 0.0
    if intent in {"expense", "income"}:
        category_key, cat_conf = _detect_category_key(raw, intent)

    debt_action = None
    person = None
    if intent == "debt":
        debt_action, person = _extract_debt_fields_safe(raw)

    comment = _clean_comment(raw)

    missing: list[str] = []
    if intent in {"expense", "income"}:
        if amount is None:
            missing.append("amount")
        if account_key is None:
            missing.append("account_id")
        if category_key is None:
            missing.append("category_id")
    elif intent == "transfer":
        if amount is None:
            missing.append("amount")
        if from_key is None:
            missing.append("from_account_id")
        if to_key is None:
            missing.append("to_account_id")
    else:
        if debt_action is None:
            missing.append("debt_action")
        if not person:
            missing.append("person_id")
        if amount is None:
            missing.append("amount")
        if account_key is None:
            missing.append("account_id")

    confidence: dict[str, float] = {
        "intent": float(intent_conf),
        "amount": 1.0 if amount is not None else 0.0,
        "currency": 1.0,
        "account": float(account_conf if intent != "transfer" else max(account_conf, transfer_acc_conf)),
        "category": float(cat_conf),
        "date": float(date_conf),
    }

    return ParsedMessage(
        intent=intent,
        debt_action=debt_action,
        amount=amount,
        currency=currency,
        currency_explicit=currency_explicit,
        account_id=account_key if intent in {"expense", "income", "debt"} else None,
        from_account_id=from_key,
        to_account_id=to_key,
        category_id=category_key,
        person_id=person,
        comment=comment,
        raw_text=raw,
        date=tx_date,
        missing_fields=missing,
        confidence=confidence,
    )


def build_miniapp_text_parse_payload(*, text: str, today: date, default_currency: str = "UAH") -> dict:
    raw_text = str(text or "").strip()
    if not raw_text:
        raise ValueError("Text is required")
    if len(raw_text) > MINIAPP_TEXT_MAX_LENGTH:
        raise ValueError("Text is too long")

    normalized_currency = str(default_currency or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{3,10}", normalized_currency):
        normalized_currency = "UAH"

    parsed = parse_message(raw_text, today=today, default_currency=normalized_currency)
    return {
        "is_candidate_tx": bool(parsed.is_candidate_tx),
        "intent": parsed.intent,
        "amount": f"{float(parsed.amount):.2f}" if parsed.amount is not None else None,
        "currency": parsed.currency,
        "currency_explicit": bool(parsed.currency_explicit),
        "account_hint": parsed.account_id,
        "category_slug": parsed.category_id,
        "transaction_date": parsed.date.isoformat() if parsed.date else None,
        "comment": parsed.comment,
        "missing_fields": list(parsed.missing_fields),
        "confidence": {key: float(value) for key, value in parsed.confidence.items()},
    }


# A comma between digits is part of a decimal amount, not a new operation.
_BATCH_SEGMENT_SPLIT_RE = re.compile(r"(?:[;\n]|(?<!\d),|,(?!\d))+")


def _normalize_batch_segment(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip(" \t,;:-–—")


def split_message_into_tx_segments(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    return [segment for part in _BATCH_SEGMENT_SPLIT_RE.split(raw) if (segment := _normalize_batch_segment(part))]


def parse_message_batch(text: str, *, today: date, default_currency: str = "UAH") -> list[ParsedMessage]:
    segments = split_message_into_tx_segments(text)
    if len(segments) < 2:
        return []

    items: list[ParsedMessage] = []
    intents: set[Intent] = set()
    currencies: set[str] = set()
    for segment in segments:
        parsed = parse_message(segment, today=today, default_currency=default_currency)
        if parsed.intent not in {"expense", "income"}:
            return []
        if parsed.amount is None:
            return []
        intents.add(parsed.intent)
        currencies.add(parsed.currency)
        items.append(parsed)

    if len(items) < 2:
        return []
    if len(intents) != 1:
        return []
    if len(currencies) != 1:
        return []
    return items


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
    parsed = parse_message(text, today=default_date, default_currency=default_currency)
    tx_type = parsed.intent if parsed.intent in {"expense", "income", "transfer"} else default_type
    return TxDraft(
        date=(parsed.date or default_date),
        type=tx_type,
        amount=parsed.amount,
        currency=parsed.currency,
        comment=(parsed.comment or (text or "").strip())[:500] or None,
    )


_DEBT_KEYWORDS = ("борг", "в борг", "позич", "поверн", "дав", "дала", "взяв", "взяла", "віддав", "віддала")
_DEBT_LEND_WORDS = {"дав", "дала", "давав", "давала"}
_DEBT_BORROW_WORDS = {"взяв", "взяла", "позичив", "позичила", "позичився", "позичилась"}
_DEBT_REPAY_WORDS = {"повернув", "повернула", "віддав", "віддала"}


def _detect_intent(text: str, default_intent: Intent = "expense") -> tuple[Intent, float]:
    raw = text or ""
    lower = raw.lower()
    if _TRANSFER_RE.search(raw):
        return "transfer", 0.95
    if any(keyword in lower for keyword in _DEBT_KEYWORDS):
        return "debt", 0.9
    if _INCOME_HINT_RE.search(raw):
        return "income", 0.9
    if _EXPENSE_HINT_RE.search(raw):
        return "expense", 0.8
    if parse_amount(raw) is not None:
        return default_intent, 0.6
    return default_intent, 0.3


def _extract_debt_fields(text: str) -> tuple[DebtAction | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return None, None
    lower = raw.lower()
    if not any(keyword in lower for keyword in _DEBT_KEYWORDS):
        return None, None

    tokens = [token.strip(",.;:!?()[]{}\"'") for token in lower.split()]
    tokens = [token for token in tokens if token]
    if not tokens:
        return None, None

    def name_at(index: int) -> str | None:
        if 0 <= index < len(tokens):
            return _normalize_person_name(tokens[index])
        return None

    if tokens[0] in _DEBT_LEND_WORDS:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "lend", name_at(2)
        return "lend", name_at(1)

    if tokens[0] in _DEBT_BORROW_WORDS:
        if len(tokens) >= 3 and tokens[1] == "у":
            return "borrow", name_at(2)
        return "borrow", name_at(1)

    for index, token in enumerate(tokens):
        if token in _DEBT_REPAY_WORDS:
            if index == 0:
                return "make_repayment", name_at(1)
            return "receive_repayment", name_at(index - 1)
        if token == "повернув":
            if index == 0:
                return "make_repayment", name_at(1)
            return "receive_repayment", name_at(index - 1)

    return None, _normalize_person_name(tokens[0])


_DEBT_INTENT_RE = re.compile(r"(?i)\b(борг|в борг|позич|поверн|дав|дала|взяв|взяла|віддав|віддала)\b")


def _detect_intent(text: str, default_intent: Intent = "expense") -> tuple[Intent, float]:
    t = text or ""
    if _TRANSFER_RE.search(t):
        return "transfer", 0.95
    if _DEBT_INTENT_RE.search(t):
        return "debt", 0.9
    if _INCOME_HINT_RE.search(t):
        return "income", 0.9
    if _EXPENSE_HINT_RE.search(t):
        return "expense", 0.8
    if parse_amount(t) is not None:
        return default_intent, 0.6
    return default_intent, 0.3


def _extract_debt_fields(text: str) -> tuple[DebtAction | None, str | None]:
    t = text or ""
    t_norm = _norm(t)
    if not t_norm or not _DEBT_INTENT_RE.search(t_norm):
        return None, None

    if re.search(r"(?i)\b(дав|дала|давав|давала)\b", t_norm):
        m = re.search(r"(?i)\b(?:дав|дала|давав|давала)\s+(?:у\s+)?(?P<name>[a-zа-яіїєґ]+)", t_norm)
        return "lend", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\b(взяв|взяла|позичив|позичила|позичився|позичилась)\b", t_norm):
        m = re.search(r"(?i)\b(?:взяв|взяла|позичив|позичила|позичився|позичилась)\s+(?:у\s+)?(?P<name>[a-zа-яіїєґ]+)", t_norm)
        return "borrow", _normalize_person_name(m.group("name") if m else "")

    if re.search(r"(?i)\b(повернув|повернула|віддав|віддала)\b", t_norm):
        m = re.search(r"(?i)\b(?P<name>[a-zа-яіїєґ]+)\s+(?:повернув|повернула|віддав|віддала)\b", t_norm)
        if m:
            return "receive_repayment", _normalize_person_name(m.group("name"))
        m2 = re.search(r"(?i)\b(?:повернув|повернула|віддав|віддала)\s+(?P<name>[a-zа-яіїєґ]+)", t_norm)
        if m2:
            return "make_repayment", _normalize_person_name(m2.group("name"))

    m3 = re.search(r"(?i)^(?P<name>[a-zа-яіїєґ]+)\b", t_norm)
    person = _normalize_person_name(m3.group("name") if m3 else "")
    return None, person
