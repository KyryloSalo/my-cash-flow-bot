from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import urlopen


_CACHE_TTL_SECONDS = 60 * 60
_NBU_ENDPOINT = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"


@dataclass(frozen=True)
class FxSnapshot:
    base: str
    date: str
    rates: dict[str, Decimal]
    fetched_at: float


_CACHE: dict[str, FxSnapshot] = {}


def _get_uah_rates() -> tuple[str, dict[str, Decimal]]:
    query = urlencode({"json": ""})
    with urlopen(f"{_NBU_ENDPOINT}?{query}", timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rates_to_uah: dict[str, Decimal] = {"UAH": Decimal("1")}
    date_text = ""
    for row in payload or []:
        code = str(row.get("cc") or "").strip().upper()
        rate = row.get("rate")
        if not code or rate in {None, ""}:
            continue
        rates_to_uah[code] = Decimal(str(rate))
        if not date_text:
            date_text = str(row.get("exchangedate") or "")
    if "USD" in rates_to_uah:
        rates_to_uah.setdefault("USDT", rates_to_uah["USD"])
    return date_text, rates_to_uah


def get_latest_rates(base_currency: str) -> FxSnapshot:
    base = (base_currency or "").strip().upper() or "UAH"
    cached = _CACHE.get(base)
    now = time.time()
    if cached and now - cached.fetched_at < _CACHE_TTL_SECONDS:
        return cached

    date_text, rates_to_uah = _get_uah_rates()
    if base not in rates_to_uah:
        raise ValueError(f"Unsupported base currency for FX snapshot: {base}")

    uah_per_base = rates_to_uah[base]
    rates_to_base = {
        currency: (uah_per_currency / uah_per_base)
        for currency, uah_per_currency in rates_to_uah.items()
    }
    rates_to_base[base] = Decimal("1")

    snapshot = FxSnapshot(
        base=base,
        date=date_text,
        rates=rates_to_base,
        fetched_at=now,
    )
    _CACHE[base] = snapshot
    return snapshot
