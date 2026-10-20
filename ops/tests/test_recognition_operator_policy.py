"""Production operator safety and private persistent storage wiring."""
from pathlib import Path
import subprocess
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


class RecognitionOperatorPolicyTests(unittest.TestCase):
    def test_admin_and_cleanup_worker_share_configured_private_volume(self):
        services = yaml.safe_load((ROOT / 'compose.yml').read_text())['services']
        for name in ('admin', 'worker'):
            self.assertEqual(services[name]['environment'].get('RECOGNITION_QUALITY_STORAGE_ROOT'),
                             '/var/lib/recognition-quality/raw')
            self.assertIn('recognition_private:/var/lib/recognition-quality', services[name]['volumes'])
        self.assertNotIn('recognition_private:/var/lib/recognition-quality',
                         services['nginx'].get('volumes', []))

    def test_operator_is_protected_and_does_not_export_samples(self):
        workflow = ROOT / '.github/workflows/recognition-operator.yml'
        self.assertTrue(workflow.is_file(), 'protected recognition operator workflow is missing')
        text = workflow.read_text()
        data = yaml.safe_load(text)
        job = data['jobs']['operate']
        self.assertEqual(job['environment'], 'production')
        self.assertEqual(data['concurrency']['group'], 'production')
        self.assertFalse(data['concurrency']['cancel-in-progress'])
        self.assertIn('"$GITHUB_REF" != refs/heads/main', text)
        self.assertIn('refs/heads/main', text)
        self.assertNotIn('recognition_quality_export', text)
        self.assertNotIn('actions/upload-artifact', text)
        script = (ROOT / 'ops/recognition_operator.sh').read_text()
        self.assertIn('current-release', script)
        self.assertIn('recognition_quality_operator', script)
        self.assertNotIn('recognition_quality_export', script)
        self.assertNotIn('docker compose config', script)

    def test_remote_operator_rejects_untrusted_arguments_before_remote_access(self):
        script = ROOT / 'ops/recognition_operator.sh'
        self.assertTrue(script.is_file(), 'operator entrypoint is missing')
        for sha, action in [('bad', 'status'), ('a' * 40, 'export'), ('a' * 40, 'activate; id')]:
            result = subprocess.run(['bash', str(script), sha, action], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Invalid operator request', result.stderr)


if __name__ == '__main__':
    unittest.main()
