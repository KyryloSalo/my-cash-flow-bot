from pathlib import Path
import unittest

CODE=Path(__file__).resolve().parents[2]


class DeployPolicyTests(unittest.TestCase):
    def test_client_requires_pinned_host_for_both_transports(self):
        text=(CODE/'ops/deploy.ps1').read_text(encoding='utf-8')
        self.assertNotIn('StrictHostKeyChecking=no',text)
        self.assertIn('StrictHostKeyChecking=yes',text)
        self.assertIn('UserKnownHostsFile=$KnownHostsPath',text)
        self.assertIn('ConfirmProduction',text)
        self.assertNotIn('Get-KeyCandidates',text)
        self.assertIn('$windowsOpenSsh = Join-Path $env:SystemRoot "System32\\OpenSSH"', text)
        self.assertIn('& $scpClient @transportArgs', text)
        self.assertIn('& $sshClient @transportArgs', text)
        self.assertNotIn('& scp @transportArgs', text)
        self.assertNotIn('& ssh @transportArgs', text)
        self.assertIn("--source-sha256 '$sourceSha'", text)
        self.assertIn('[string]$BackupReceiptPath', text)
        self.assertIn('off_host_verified', text)
        self.assertIn('backup-receipt.json', text)

    def test_rollout_requires_immutable_images_and_readiness(self):
        text=(CODE/'deploy_v0_on_vps.sh').read_text(encoding='utf-8')
        for token in ['--confirm-production', '--source-sha256', '--backup-receipt', 'backup_receipt.py', 'sha256sum', 'docker load', '--no-build', '--wait', 'migrate --check', 'readiness_smoke.py', 'RELEASES="$ROOT/releases"', 'CURRENT="$ROOT/current"']:
            self.assertIn(token,text)
        self.assertIn('exec 9> "$ROOT/.deploy.lock"', text)
        self.assertIn('legacy-source.tar.gz', text)
        self.assertIn("--exclude='.env*'", text)
        self.assertIn('--env-file "$HOST_ENV_FILE"', text)
        self.assertIn('DJANGO_SECRET_HOST_PATH', text)
        self.assertIn('compose_at "$OLD_PROJECT" up -d --no-build', text)
        self.assertIn('current-release', text)
        self.assertIn('Release tree is not readable by unprivileged containers', text)
        self.assertLess(
            text.index('[[ ! -e "$RELEASE_DIR" ]]'),
            text.index('stop nginx bot api admin worker beat'),
        )
        self.assertLess(
            text.index('release_artifact.py" install'),
            text.index('stop nginx bot api admin worker beat'),
        )
        self.assertNotIn('cp -a "$SRC/."', text)
        self.assertNotIn('getMe', text)

    def test_django_secret_rotation_uses_a_host_owned_file(self):
        compose = (CODE / 'compose.yml').read_text(encoding='utf-8')
        settings = (CODE / 'admin_service' / 'config' / 'settings.py').read_text(encoding='utf-8')
        self.assertGreaterEqual(compose.count('DJANGO_SECRET_KEY_FILE: /run/secrets/django_secret_key'), 4)
        self.assertGreaterEqual(compose.count('DJANGO_SECRET_HOST_PATH'), 4)
        self.assertIn('env_file_secret("DJANGO_SECRET_KEY_FILE")', settings)
        self.assertIn('SECRET_KEY_FALLBACKS', settings)

    def test_test_runners_disable_https_redirect_without_weakening_deploy_check(self):
        validator=(CODE/'ops/validate_release.sh').read_text(encoding='utf-8')
        workflow=(CODE/'.github/workflows/deploy.yml').read_text(encoding='utf-8')
        self.assertIn('python manage.py check --deploy --fail-level WARNING', validator)
        self.assertIn('DJANGO_DEBUG=true python manage.py test --noinput', validator)
        self.assertIn('-e DJANGO_DEBUG=true', workflow)


if __name__=='__main__': unittest.main()
