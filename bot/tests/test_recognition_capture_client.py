from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.SimpleNamespace(HTTPError=RuntimeError, AsyncClient=object)

import recognition_capture_client as client


class Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise client.httpx.HTTPError("failure")

    def json(self):
        return self._payload


class FakeAsyncClient:
    def __init__(self, request):
        self.request = request

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class CaptureClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_sends_original_multipart_and_returns_capture_id(self):
        response = Response(payload={"capture_id": 17})
        request = AsyncMock(return_value=response)
        fake_client = FakeAsyncClient(request)
        with patch.object(client.config, "BILLING_INTERNAL_BASE_URL", "http://billing"), patch.object(client.config, "BILLING_INTERNAL_TOKEN", "secret"), patch.object(client.httpx, "AsyncClient", return_value=fake_client):
            result = await client.RecognitionCaptureClient(retries=0).create(user_id=123, attempt_id="tg-77", image_bytes=b"original", mime_type="image/png")
        self.assertEqual(result, 17)
        kwargs = request.call_args.kwargs
        self.assertEqual(kwargs["headers"], {"X-Internal-Token": "secret"})
        self.assertEqual(kwargs["data"], {"action": "create", "user_id": "123", "attempt_id": "tg-77"})
        self.assertEqual(kwargs["files"]["image"][1:], (b"original", "image/png"))

    async def test_complete_sends_normalized_result(self):
        response = Response(payload={"capture_id": 17})
        request = AsyncMock(return_value=response)
        fake_client = FakeAsyncClient(request)
        with patch.object(client.config, "BILLING_INTERNAL_TOKEN", "secret"), patch.object(client.httpx, "AsyncClient", return_value=fake_client):
            result = await client.RecognitionCaptureClient(retries=0).complete(user_id=123, attempt_id="tg-77", outcome="parsed", normalized_result={"amount": "10", "currency": "UAH", "item_count": 1})
        self.assertEqual(result, 17)
        self.assertEqual(request.call_args.kwargs["json"]["outcome"], "parsed")
        self.assertNotIn("error_code", request.call_args.kwargs["json"])
        self.assertEqual(request.call_args.kwargs["json"]["normalized_result"]["item_count"], 1)

    async def test_auth_rejection_is_not_retried(self):
        response = Response(status_code=401)
        request = AsyncMock(return_value=response)
        fake_client = FakeAsyncClient(request)
        with patch.object(client.config, "BILLING_INTERNAL_TOKEN", "secret"), patch.object(client.httpx, "AsyncClient", return_value=fake_client):
            with self.assertRaises(client.RecognitionCaptureError):
                await client.RecognitionCaptureClient(retries=3).complete(user_id=1, attempt_id="tg-1", outcome="failed", error_code="auth")
        request.assert_awaited_once()
