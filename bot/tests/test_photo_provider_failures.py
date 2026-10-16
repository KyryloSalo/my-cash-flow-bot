"""Provider contract failures must remain bounded, unusable evidence."""
import json
import asyncio
from io import BytesIO
import unittest
from unittest.mock import AsyncMock, patch

from types import SimpleNamespace

# Response fixtures must not depend on suite-global transport stubs.
httpx = SimpleNamespace(Request=lambda *a, **k: None, Response=lambda status, request, json: SimpleNamespace(json=lambda: json, raise_for_status=lambda: None))
from PIL import Image
import photo_shadow
from photo_pipeline import ExtractionPass, reconcile_extractions


class ProviderFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_single_mode_with_invalid_labels_is_reextracted_once(self):
        payload = {'mode': 'single_tx', 'evidence': [
            {'id': 's', 'field': 'status', 'raw_text': 'unrelated action',
             'value': 'completed', 'region': None}], 'statement_items': []}
        response = httpx.Response(200, request=httpx.Request('POST', 'https://example.invalid'),
                                  json={'output_text': json.dumps(payload)})
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.post.return_value = response
        with patch.object(photo_shadow.config, 'OPENAI_API_KEY', 'synthetic-test-value'), \
             patch.object(photo_shadow.httpx, 'AsyncClient', return_value=client):
            result = await photo_shadow._request(b'image', mime_type='image/jpeg',
                                                model='test', detail='high', source='full-a')
        self.assertEqual(client.post.await_count, 2)
        self.assertFalse(reconcile_extractions((result,)).accepted)

    async def test_invalid_full_statement_payload_is_reextracted_once(self):
        invalid = {'mode': 'statement_expenses', 'evidence': [{'field': 'status'}],
                   'statement_items': []}
        corrected = {'mode': 'statement_expenses', 'evidence': [], 'statement_items': [
            {'transaction_date': '2026-10-01', 'amount': '10.00', 'currency': 'UAH',
             'type': 'unknown', 'status': 'unknown'}]}
        responses = [httpx.Response(200, request=httpx.Request('POST', 'https://example.invalid'),
                                   json={'output_text': json.dumps(payload)})
                     for payload in (invalid, corrected)]
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.post.side_effect = responses
        with patch.object(photo_shadow.config, 'OPENAI_API_KEY', 'synthetic-test-value'), \
             patch.object(photo_shadow.httpx, 'AsyncClient', return_value=client):
            result = await photo_shadow._request(b'image', mime_type='image/jpeg',
                                                model='test', detail='high', source='full-a')
        self.assertEqual(client.post.await_count, 2)
        self.assertEqual(result.mode, 'statement_expenses')
        self.assertEqual(len(result.raw_evidence['statement_review_rows']), 1)

    async def test_unsupported_with_core_evidence_rechecks_layout_once(self):
        unsupported = {'mode': 'unsupported', 'evidence': [
            {'id': 'a', 'field': 'amount', 'raw_text': '-10.00 UAH', 'value': '10.00', 'region': None},
            {'id': 'c', 'field': 'currency', 'raw_text': 'UAH', 'value': 'UAH', 'region': None},
            {'id': 's', 'field': 'status', 'raw_text': 'успішно', 'value': 'completed', 'region': None},
            {'id': 't', 'field': 'type', 'raw_text': 'оплата', 'value': 'expense', 'region': None},
        ], 'statement_items': []}
        corrected = {'mode': 'statement_expenses', 'evidence': [], 'statement_items': [
            {'transaction_date': '2026-10-01', 'amount': '10.00', 'currency': 'UAH',
             'type': 'unknown', 'status': 'unknown'}]}
        responses = [httpx.Response(200, request=httpx.Request('POST', 'https://example.invalid'),
                                   json={'output_text': json.dumps(payload)})
                     for payload in (unsupported, corrected)]
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.post.side_effect = responses
        with patch.object(photo_shadow.config, 'OPENAI_API_KEY', 'synthetic-test-value'), \
             patch.object(photo_shadow.httpx, 'AsyncClient', return_value=client):
            result = await photo_shadow._request(b'image', mime_type='image/jpeg',
                                                model='test', detail='high', source='full-a')
        self.assertEqual(client.post.await_count, 2)
        self.assertEqual(result.mode, 'statement_expenses')
        self.assertFalse(result.raw_evidence['statement_items'])
        self.assertEqual(len(result.raw_evidence['statement_review_rows']), 1)

    async def test_contradictory_full_layout_is_reextracted_once_without_promoting_rows(self):
        unsupported = {'mode': 'unsupported', 'evidence': [], 'statement_items': [
            {'transaction_date': '2026-10-01', 'amount': '10.00', 'currency': 'UAH',
             'type': 'unknown', 'status': 'unknown'}]}
        corrected = {**unsupported, 'mode': 'statement_expenses'}
        responses = [httpx.Response(200, request=httpx.Request('POST', 'https://example.invalid'),
                                   json={'output_text': json.dumps(payload)})
                     for payload in (unsupported, corrected)]
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.post.side_effect = responses
        with patch.object(photo_shadow.config, 'OPENAI_API_KEY', 'synthetic-test-value'), \
             patch.object(photo_shadow.httpx, 'AsyncClient', return_value=client):
            result = await photo_shadow._request(b'image', mime_type='image/jpeg',
                                                model='test', detail='high', source='full-a')
        self.assertEqual(client.post.await_count, 2)
        self.assertEqual(result.mode, 'statement_expenses')
        self.assertEqual(result.raw_evidence['statement_items'], [])
        self.assertEqual(len(result.raw_evidence['statement_review_rows']), 1)
        self.assertFalse(reconcile_extractions((result,)).accepted)

    async def test_invalid_statement_crop_does_not_abort_other_extractions(self):
        response = httpx.Response(200, request=httpx.Request('POST', 'https://example.invalid'),
                                  json={'output_text': json.dumps({
                                      'mode': 'statement_expenses',
                                      'evidence': [{'field': 'status'}],
                                      'statement_items': [],
                                  })})
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.post.return_value = response
        with patch.object(photo_shadow.config, 'OPENAI_API_KEY', 'synthetic-test-value'), \
             patch.object(photo_shadow.httpx, 'AsyncClient', return_value=client):
            result = await photo_shadow._request(b'image', mime_type='image/jpeg',
                                                model='test', detail='high', source='crop-0')
        self.assertEqual(result.source, 'crop-0')
        self.assertFalse(result.evidence)
        self.assertTrue(result.raw_evidence.get('provider_invalid'))
        self.assertFalse(reconcile_extractions((result,)).accepted)

    async def test_transport_failure_finishes_sibling_tasks_before_returning(self):
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def request(*args, source, **kwargs):
            if source == 'full-canonical-a':
                await started.wait()
                raise RuntimeError('synthetic transport failure')
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        image = BytesIO()
        Image.new('RGB', (12, 12), 'white').save(image, format='PNG')
        with patch.object(photo_shadow, '_request', side_effect=request), \
             patch.object(photo_shadow, '_load_durable', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'synthetic transport failure'):
                await photo_shadow.analyze_photo(image.getvalue(), mime_type='image/png', model='test', detail='high')
            self.assertTrue(cancelled.is_set(), 'provider tasks escaped their originating request')


if __name__ == '__main__':
    unittest.main()
