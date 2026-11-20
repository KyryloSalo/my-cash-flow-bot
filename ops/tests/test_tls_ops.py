import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

OPS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPS))
import tls_ops as tls
import install_tls_ops as installer


class TLSOpsTests(unittest.TestCase):
    def test_probe_requires_exact_body_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            response = MagicMock()
            response.__enter__.return_value = response
            response.status = 200
            response.read.return_value = b'<html>SPA</html>'
            opener = Mock()
            opener.open.return_value = response
            with self.assertRaises(RuntimeError):
                tls.acme_check(root=Path(tmp), domains=('example.test',), opener=opener)
            self.assertEqual(list(Path(tmp).rglob('vydno-*')), [])

    def test_probe_uses_unique_readable_proof(self):
        def fetch(url, timeout):
            file = Path(tmp) / '.well-known/acme-challenge' / url.rsplit('/', 1)[1]
            if os.name != 'nt':
                self.assertEqual(file.stat().st_mode & 0o777, 0o644)
            response = MagicMock()
            response.__enter__.return_value = response
            response.status = 200
            response.read.return_value = file.read_bytes()
            return response
        with tempfile.TemporaryDirectory() as tmp:
            opener = Mock()
            opener.open.side_effect = fetch
            tls.acme_check(root=Path(tmp), domains=('one.test', 'two.test'), opener=opener)
            self.assertEqual(opener.open.call_count, 2)
            self.assertEqual(list(Path(tmp).rglob('vydno-*')), [])

    def test_invalid_nginx_never_reloads(self):
        failed = subprocess.CalledProcessError(1, ['nginx', '-t'])
        with patch.object(tls, 'run', side_effect=failed) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                tls.post_renew()
            self.assertEqual(run.call_args_list[0].args[0][-1], '-t')
            self.assertEqual(run.call_count, 1)

    def test_validated_local_and_public_fingerprints_must_match(self):
        with patch.object(tls, 'DOMAINS', ('example.test',)), patch.object(tls, 'run') as nginx:
            with patch.object(tls, 'certificate', return_value={'sha256': 'new'}) as meta:
                tls.post_renew()
            self.assertEqual([call.args[0][-1] for call in nginx.call_args_list], ['-t', 'reload'])
            self.assertEqual(meta.call_args_list[0].kwargs, {'address': '127.0.0.1'})
            self.assertEqual(meta.call_args_list[1].args, ('example.test',))

    def test_units_use_stable_paths_and_failure_alerts(self):
        units = installer.unit_definitions()
        self.assertIn('ExecStart=\nExecStart=/usr/bin/python3', units['certbot.service.d/vydno.conf'])
        self.assertIn('OnFailure=', units['certbot.service.d/vydno.conf'])
        self.assertIn('Restart=on-failure', units['vydno-tls-alert@.service'])
        self.assertIn('OnUnitActiveSec=5min', units['vydno-public-monitor.timer'])
        self.assertIn('Sun', units['vydno-tls-dry-run.timer'])
        self.assertNotIn('/current/', ''.join(units.values()))


if __name__ == '__main__':
    unittest.main()
