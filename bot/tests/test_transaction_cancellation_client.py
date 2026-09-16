from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")
    httpx_stub.HTTPError = Exception
    httpx_stub.AsyncClient = object
    sys.modules["httpx"] = httpx_stub

import transaction_cancellation_client as client  # noqa: E402


class _Response:
    status_code = 200

    def json(self):
        return {"ok": True, "items": []}


class _ClientContext:
    def __init__(self, post: AsyncMock) -> None:
        self.post = post

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class TransactionCancellationClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_token_fails_closed_without_http(self):
        with patch.object(client.config, "BILLING_INTERNAL_TOKEN", None):
            with patch.object(client.httpx, "AsyncClient") as async_client:
                with self.assertRaises(client.TransactionCancellationAPIError):
                    await client.list_recent_transactions(actor_tg_user_id=1)
            async_client.assert_not_called()

    async def test_recent_endpoint_sends_only_actor_and_limit(self):
        post = AsyncMock(return_value=_Response())
        context = _ClientContext(post)
        with (
            patch.object(client.config, "BILLING_INTERNAL_TOKEN", "test-token"),
            patch.object(client.config, "BILLING_INTERNAL_BASE_URL", "http://admin:8080"),
            patch.object(client.httpx, "AsyncClient", return_value=context),
        ):
            payload = await client.list_recent_transactions(actor_tg_user_id=123, limit=20)
        self.assertTrue(payload["ok"])
        _, kwargs = post.call_args
        self.assertEqual(kwargs["json"], {"actor_tg_user_id": 123, "limit": 10})
        self.assertEqual(kwargs["headers"]["X-Internal-Token"], "test-token")


if __name__ == "__main__":
    unittest.main()
