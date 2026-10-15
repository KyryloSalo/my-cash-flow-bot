import asyncio
import json
import sys
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

sys.path.insert(0, "bot")
import config
import photo_shadow


class Response:
    def __init__(self, body): self.body = body
    def raise_for_status(self): pass
    def json(self): return self.body


class Client:
    calls = 0
    fail_once = False
    def __init__(self, **kwargs): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return False
    async def post(self, url, *, headers, json_payload=None, **kwargs):
        type(self).calls += 1
        if type(self).fail_once:
            type(self).fail_once = False
            raise RuntimeError("temporary")
        field_values = {"amount": ("12.34", "12.34"), "currency": ("USD", "USD"),
                        "status": ("paid", "completed"), "type": ("expense", "expense")}
        evidence = [{"id": f"x-{field}", "field": field, "raw_text": raw, "value": value, "region": None}
                    for field, (raw, value) in field_values.items()]
        return Response({"output_text": json.dumps({"evidence": evidence})})


class PhotoShadowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        photo_shadow._CACHE.clear()
        Client.calls = 0
        Client.fail_once = False

    async def test_two_independent_full_passes_reconcile(self):
        with patch.object(config, "OPENAI_API_KEY", "test-key"), patch.object(photo_shadow.httpx, "AsyncClient", Client):
            result = await photo_shadow.analyze_shadow(b"image", mime_type="image/jpeg", model="m", detail="high")
        self.assertTrue(result.accepted)
        self.assertEqual(result.amount, Decimal("12.34"))
        self.assertEqual(Client.calls, 2)

    async def test_concurrent_same_key_is_single_flight_and_cached(self):
        with patch.object(config, "OPENAI_API_KEY", "test-key"), patch.object(photo_shadow.httpx, "AsyncClient", Client):
            results = await asyncio.gather(*[photo_shadow.analyze_shadow(b"same", mime_type="image/jpeg", model="m", detail="high") for _ in range(5)])
        self.assertTrue(all(item.accepted for item in results))
        self.assertEqual(Client.calls, 2)

    async def test_errors_are_not_cached_and_next_attempt_recovers(self):
        Client.fail_once = True
        with patch.object(config, "OPENAI_API_KEY", "test-key"), patch.object(photo_shadow.httpx, "AsyncClient", Client):
            with self.assertRaises(RuntimeError):
                await photo_shadow.analyze_shadow(b"recover", mime_type="image/jpeg", model="m", detail="high")
            result = await photo_shadow.analyze_shadow(b"recover", mime_type="image/jpeg", model="m", detail="high")
        self.assertTrue(result.accepted)
        self.assertEqual(Client.calls, 4)

    async def test_mime_isolation_and_normalization(self):
        with patch.object(config, "OPENAI_API_KEY", "test-key"), patch.object(photo_shadow.httpx, "AsyncClient", Client):
            await photo_shadow.analyze_shadow(b"same", mime_type="IMAGE/JPG; charset=binary", model="m", detail="high")
            await photo_shadow.analyze_shadow(b"same", mime_type="image/png", model="m", detail="high")
        self.assertEqual(Client.calls, 4)

    async def test_image_type_and_size_are_bounded(self):
        with self.assertRaises(ValueError):
            await photo_shadow.analyze_shadow(b"x", mime_type="image/gif", model="m", detail="high")
        with self.assertRaises(ValueError):
            await photo_shadow.analyze_shadow(b"x" * (photo_shadow._MAX_IMAGE_BYTES + 1), mime_type="image/png", model="m", detail="high")

    async def test_lock_registry_is_bounded(self):
        self.assertEqual(len(photo_shadow._LOCKS), photo_shadow._LOCK_STRIPES)
        self.assertTrue(all(isinstance(lock, asyncio.Lock) for lock in photo_shadow._LOCKS))


if __name__ == "__main__":
    unittest.main()
