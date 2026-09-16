"""Private bridge boundary contracts; no application startup or credentials."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

from django.conf import settings
from django.http import HttpResponse
from django.test import RequestFactory

CODE = Path(__file__).resolve().parents[2]
if not settings.configured:
    settings.configure(DEFAULT_CHARSET='utf-8', STATIC_URL='/static/')
spec = importlib.util.spec_from_file_location('ops_middleware', CODE / 'admin_service/common/middleware.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        module.settings = SimpleNamespace(ADMIN_IP_ALLOWLIST=['203.0.113.2'], STATIC_URL='/static/', ADMIN_TRUSTED_PROXY_IPS=['172.29.247.2'])
        self.factory = RequestFactory()

    def test_internal_push_does_not_require_operator_ip(self):
        def protected(req):
            return HttpResponse(status=200 if req.headers.get('X-Internal-Token') == 'synthetic' else 401)
        mw = module.AdminIPAllowlistMiddleware(protected)
        for prefix in ['push/event', 'billing/mono/init-bind', 'transactions/recent']:
            good = self.factory.post('/internal/' + prefix, REMOTE_ADDR='172.18.0.5', HTTP_X_INTERNAL_TOKEN='synthetic')
            bad = self.factory.post('/internal/' + prefix, REMOTE_ADDR='172.18.0.5')
            self.assertEqual(mw(good).status_code, 200)
            self.assertEqual(mw(bad).status_code, 401)

    def test_forwarded_ip_only_trusted_from_explicit_proxy(self):
        mw = module.AdminIPAllowlistMiddleware(lambda req: HttpResponse(status=200))
        for source, expected in [('198.51.100.3',403), ('172.29.247.2',200)]:
            req = self.factory.get('/login/', REMOTE_ADDR=source, HTTP_X_REAL_IP='203.0.113.2')
            self.assertEqual(mw(req).status_code, expected)

    def test_every_public_vhost_denies_internal_before_proxy(self):
        source = (CODE / 'nginx/cashflowbot.conf.template').read_text(encoding='utf-8')
        servers = source.split('server {')[1:]
        self.assertEqual(len(servers), 5)
        for server in servers:
            self.assertIn('location ^~ /internal/ { return 404; }', server)
            self.assertIn('location = /internal { return 404; }', server)


if __name__ == '__main__':
    unittest.main()
