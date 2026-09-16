"""Offline regression: no real token, credentials or network."""
import importlib.util
import io
import logging
from pathlib import Path
import unittest

import httpx
from uvicorn.logging import AccessFormatter

CODE = Path(__file__).resolve().parents[2]


class LoggingSafetyTests(unittest.TestCase):
    def test_uvicorn_access_formatter_keeps_structured_arguments(self):
        path = CODE / 'bot/log_safety.py'
        spec = importlib.util.spec_from_file_location('ops_log_safety_uvicorn', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        previous = logging.getLogRecordFactory()
        try:
            module.install_log_safety()
            record = logging.getLogger('uvicorn.access').makeRecord(
                'uvicorn.access',
                logging.INFO,
                __file__,
                1,
                '%s - "%s %s HTTP/%s" %d',
                ('127.0.0.1:1234', 'GET', '/health', '1.1', 200),
                None,
            )
            rendered = AccessFormatter('%(client_addr)s %(request_line)s %(status_code)s').format(record)
            self.assertEqual(rendered, '127.0.0.1:1234 GET /health HTTP/1.1 200 OK')
        finally:
            logging.setLogRecordFactory(previous)

    def test_httpx_retry_and_exception_logs_redact_telegram_url(self):
        path = CODE / 'bot/log_safety.py'
        self.assertTrue(path.exists(), 'central log safety is missing')
        spec = importlib.util.spec_from_file_location('ops_log_safety', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        previous = logging.getLogRecordFactory()
        root = logging.getLogger()
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        marker = '123456789:SYNTHETIC_ONLY_DO_NOT_USE_123456789'
        url = f'https://api.telegram.org/bot{marker}/getUpdates'
        try:
            module.install_log_safety()
            client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(503)))
            for attempt in range(2):
                try:
                    client.get(url).raise_for_status()
                except httpx.HTTPStatusError:
                    logging.getLogger('ops.retry').exception('GET retry=%s url=%s', attempt, url)
            logging.getLogger('httpx').warning('GET %s HTTP/1.1 503', url)
            self.assertNotIn(marker, stream.getvalue())
            self.assertIn('503', stream.getvalue())
            self.assertIn('GET', stream.getvalue())
            self.assertIn('[REDACTED]', stream.getvalue())
            self.assertEqual(logging.getLogger('httpx').level, logging.WARNING)
            self.assertEqual(logging.getLogger('httpcore').level, logging.WARNING)
        finally:
            root.removeHandler(handler)
            logging.setLogRecordFactory(previous)


if __name__ == '__main__':
    unittest.main()
