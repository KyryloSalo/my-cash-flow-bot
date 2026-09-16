from __future__ import annotations

import os
import sys
import types
import unittest
from decimal import Decimal
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")
    httpx_stub.AsyncClient = object
    sys.modules["httpx"] = httpx_stub

import fx_rates  # noqa: E402


class _FakeResponse:
    def __init__(self, payload: list[dict]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict]:
        return list(self._payload)


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs) -> None:
        self._payload = [
            {"cc": "USD", "rate": "41.25", "exchangedate": "20.05.2026"},
            {"cc": "EUR", "rate": "46.10", "exchangedate": "20.05.2026"},
            {"cc": "TRY", "rate": "1.05", "exchangedate": "20.05.2026"},
        ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return _FakeResponse(self._payload)


class FxRatesTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        fx_rates._CACHE.clear()

    async def test_get_uah_rates_adds_usdt_proxy_from_usd(self) -> None:
        with patch.object(fx_rates.httpx, "AsyncClient", _FakeAsyncClient):
            date_text, rates_to_uah = await fx_rates._get_uah_rates()

        self.assertEqual(date_text, "20.05.2026")
        self.assertEqual(rates_to_uah["USD"], Decimal("41.25"))
        self.assertEqual(rates_to_uah["USDT"], Decimal("41.25"))

    async def test_get_latest_rates_supports_usdt_base_currency(self) -> None:
        async def fake_get_uah_rates():
            return (
                "20.05.2026",
                {
                    "UAH": Decimal("1"),
                    "USD": Decimal("41.25"),
                    "USDT": Decimal("41.25"),
                    "EUR": Decimal("46.10"),
                },
            )

        with patch.object(fx_rates, "_get_uah_rates", side_effect=fake_get_uah_rates):
            snapshot = await fx_rates.get_latest_rates("USDT")

        self.assertEqual(snapshot.base, "USDT")
        self.assertEqual(snapshot.rates["USDT"], Decimal("1"))
        self.assertEqual(snapshot.rates["USD"], Decimal("1"))
        self.assertEqual(snapshot.rates["EUR"], Decimal("1.117575757575757575757575758"))


if __name__ == "__main__":
    unittest.main()
