from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class _AsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def post(self, *args, **kwargs):
            raise AssertionError("HTTP client should not be used when billing token is missing")

    httpx_stub.AsyncClient = _AsyncClient
    sys.modules["httpx"] = httpx_stub

import billing_client  # noqa: E402


class BillingInternalClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_post_json_fails_closed_when_internal_token_missing(self) -> None:
        original_token = billing_client.config.BILLING_INTERNAL_TOKEN
        billing_client.config.BILLING_INTERNAL_TOKEN = None
        try:
            with patch.object(billing_client.httpx, "AsyncClient") as mock_client:
                with self.assertRaisesRegex(billing_client.BillingAPIError, "BILLING_INTERNAL_TOKEN not configured"):
                    await billing_client._post_json("/internal/billing/mono/init-bind", {"telegram_user_id": 1})
            mock_client.assert_not_called()
        finally:
            billing_client.config.BILLING_INTERNAL_TOKEN = original_token


if __name__ == "__main__":
    unittest.main()
