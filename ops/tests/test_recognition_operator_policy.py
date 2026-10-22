"""Production operator safety and private persistent storage wiring."""
from pathlib import Path
import os
import shlex
import subprocess
from tempfile import TemporaryDirectory
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


class RecognitionOperatorPolicyTests(unittest.TestCase):
    def _run_synthetic_runtime(self, **overrides):
        services = yaml.safe_load((ROOT / 'compose.yml').read_text())['services']
        script = (ROOT / 'ops/recognition_operator.sh').read_text()
        sha = 'a' * 40
        with TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / 'releases' / sha
            release.mkdir(parents=True)
            (release / 'compose.yml').touch()
            (root / 'current-release').write_text(sha + '\n', newline='\n')
            # Only redirect the fixed production root for this isolated shell test.
            self.assertEqual(script.count('root=/opt/my-cash-flow-bot'), 1)
            operator = root / 'operator.sh'
            operator.write_text(script.replace('root=/opt/my-cash-flow-bot', 'root=' + shlex.quote(root.as_posix())), newline='\n')
            actions = root / 'actions.log'
            env = os.environ.copy()
            env.update(TEST_OPERATOR_ROOT=root.as_posix(), TEST_SHA=sha,
                       TEST_ACTION_LOG=actions.as_posix(), TEST_WORKER_HEALTH='healthy',
                       TEST_WORKER_VOLUME='synthetic-private')
            for name in ('admin', 'worker', 'beat', 'db'):
                env[f'TEST_{name.upper()}_IMAGE'] = services[name]['image'].replace('${RELEASE_ID:-local}', sha)
            env.update(overrides)
            harness = r'''
flock() { return 0; }
readlink() { printf '%s/releases/%s\n' "$TEST_OPERATOR_ROOT" "$TEST_SHA"; }
docker() {
  if [[ "$1" == ps ]]; then
    case "$*" in
      *com.docker.compose.service=admin*) printf 'aaaaaaaaaaaa\n' ;;
      *com.docker.compose.service=worker*) printf 'bbbbbbbbbbbb\n' ;;
      *com.docker.compose.service=beat*) printf 'cccccccccccc\n' ;;
      *com.docker.compose.service=db*) printf 'dddddddddddd\n' ;;
      *) return 2 ;;
    esac
  elif [[ "$1" == inspect ]]; then
    if [[ "$3" == '{{.State.Health.Status}}' ]]; then
      if [[ "$4" == bbbbbbbbbbbb ]]; then printf '%s\n' "$TEST_WORKER_HEALTH"; else printf 'healthy\n'; fi
    elif [[ "$3" == '{{.Config.Image}}' ]]; then
      case "$4" in
        aaaaaaaaaaaa) printf '%s\n' "$TEST_ADMIN_IMAGE" ;;
        bbbbbbbbbbbb) printf '%s\n' "$TEST_WORKER_IMAGE" ;;
        cccccccccccc) printf '%s\n' "$TEST_BEAT_IMAGE" ;;
        dddddddddddd) printf '%s\n' "$TEST_DB_IMAGE" ;;
        *) return 2 ;;
      esac
    elif [[ "$3" == *'.NetworkSettings.Networks'* ]]; then
      printf '172.30.0.2\n'
    elif [[ "$3" == *'.Mounts'* ]]; then
      if [[ "$4" == bbbbbbbbbbbb ]]; then printf '%s\n' "$TEST_WORKER_VOLUME"; else printf 'synthetic-private\n'; fi
    else return 2; fi
  elif [[ "$1" == exec ]]; then
    if [[ "$*" == *recognition_database_readiness* ]]; then printf 'READY\n'; else printf '%s\n' "${@: -1}" >> "$TEST_ACTION_LOG"; printf '{}\n'; fi
  else return 2; fi
}
source "$1" "$2" "$3"
'''
            result = subprocess.run(['bash', '-c', harness, 'synthetic-runtime', operator.as_posix(), sha, 'activate'],
                                    env=env, capture_output=True, text=True)
            calls = actions.read_text().splitlines() if actions.exists() else []
            return result, calls

    def test_operator_accepts_canonical_compose_images_and_reads_back(self):
        result, calls = self._run_synthetic_runtime()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ['activate', 'status'])

    def test_operator_still_rejects_wrong_worker_sha_before_mutation(self):
        result, calls = self._run_synthetic_runtime(TEST_WORKER_IMAGE='mcf-admin:' + 'b' * 40)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Runtime service identity or health mismatch', result.stderr)
        self.assertEqual(calls, [])

    def test_operator_still_rejects_unhealthy_worker_before_mutation(self):
        result, calls = self._run_synthetic_runtime(TEST_WORKER_HEALTH='unhealthy')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Runtime service identity or health mismatch', result.stderr)
        self.assertEqual(calls, [])

    def test_operator_still_rejects_unshared_private_volume_before_mutation(self):
        result, calls = self._run_synthetic_runtime(TEST_WORKER_VOLUME='different-private')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Private persistent shared volume is not verified', result.stderr)
        self.assertEqual(calls, [])

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
