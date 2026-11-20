import json, subprocess, sys, tempfile, unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tls_monitor


class TLSMonitorTests(unittest.TestCase):
    def test_html_login_redirect_is_not_healthy_api(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        def probe(url, timeout=10):
            return 200, b'<html>Login</html>', now + timedelta(days=90)
        result = tls_monitor.check_domain('admin.vydno.capital', probe=probe, now=now)
        self.assertIn('dashboard_contract', result['reasons'])

    def test_api_probe_uses_real_bootstrap_contract(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        urls = []
        def probe(url, timeout=10):
            urls.append(url)
            return ((200, b'app', now + timedelta(days=90)) if url.endswith('/app/')
                    else (401, b'{"error":{"code":"session_required"}}', now + timedelta(days=90)))
        result = tls_monitor.check_domain('admin.vydno.capital', probe=probe, now=now)
        self.assertEqual(result['status'], 'ok')
        self.assertIn('https://admin.vydno.capital/app/api/bootstrap', urls)

    def test_real_paths_http_error_and_expiry_thresholds(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        def probe(url, timeout=10):
            if url.endswith('/app/'):
                return 200, b'ok', now + timedelta(days=20)
            return 401, b'{"error":{"code":"session_required"},"extra":true}', now + timedelta(days=20)
        result = tls_monitor.check_domain('vydno.capital', probe=probe, now=now)
        self.assertEqual(result['status'], 'alert')
        self.assertIn('certificate_expiry_21d', result['reasons'])
        self.assertEqual(result['details']['dashboard_status'], 401)

    def test_expired_certificate_is_alert(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        past = now - timedelta(seconds=1)
        def probe(url, timeout=10):
            return (200, b'ok', past) if url.endswith('/app/') else (401, b'{"error":{"code":"session_required"}}', past)
        result = tls_monitor.check_domain('vydno.capital', probe=probe, now=now)
        self.assertEqual(result['status'], 'alert')
        self.assertIn('certificate_expiry_7d', result['reasons'])

    def test_dedup_increasing_days_and_recovery_notifier_failure(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d) / 'state.json'; alert = {'key':'tls:example.test','message':'down','fingerprint':'certificate_expiry_21d'}
            self.assertTrue(tls_monitor.should_emit(state, alert)); tls_monitor.record_alert(state, alert)
            alert2 = dict(alert, message='down: 20.1 days'); self.assertFalse(tls_monitor.should_emit(state, alert2))
            with self.assertRaises(RuntimeError): tls_monitor.notify([sys.executable, '-c', 'import sys; sys.exit(2)'], 'x')
            self.assertIn('tls:example.test', tls_monitor._load(state)['active'])
            tls_monitor.record_recovery(state, alert['key']); self.assertTrue(tls_monitor.should_emit(state, alert))

    def test_atomic_state_and_partial_alerts(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d) / 'state.json'; tls_monitor.record_alert(state, {'key':'a','message':'a'})
            tls_monitor.record_alert(state, {'key':'b','message':'b'})
            self.assertEqual(set(tls_monitor._load(state)['active']), {'a','b'})

    def test_empty_notifier_rejected(self):
        with self.assertRaises(ValueError): tls_monitor.notify([], 'x')

if __name__ == '__main__': unittest.main()