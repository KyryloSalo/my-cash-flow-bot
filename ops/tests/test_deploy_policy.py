import unittest
from pathlib import Path

CODE=Path(__file__).resolve().parents[2]


class DeployPolicyTests(unittest.TestCase):
    def test_client_requires_pinned_host_for_both_transports(self):
        text=(CODE/'ops/deploy.ps1').read_text(encoding='utf-8')
        self.assertNotIn('StrictHostKeyChecking=no',text)
        self.assertIn('StrictHostKeyChecking=yes',text)
        self.assertIn('UserKnownHostsFile=$KnownHostsPath',text)
        self.assertIn('ConfirmProduction',text)
        self.assertIn('[ValidateRange(1, 65535)][int]$ServerPort', text)
        self.assertNotIn('Get-KeyCandidates',text)
        self.assertIn('$windowsOpenSsh = Join-Path $env:SystemRoot "System32\\OpenSSH"', text)
        self.assertIn('& $scpClient @scpArgs', text)
        self.assertIn('& $sshClient @sshArgs', text)
        self.assertNotIn('& scp @transportArgs', text)
        self.assertNotIn('& ssh @transportArgs', text)
        self.assertIn("--source-sha256 '$sourceSha'", text)
        self.assertIn('[string]$BackupReceiptPath', text)
        self.assertIn('off_host_verified', text)
        self.assertIn('backup-receipt.json', text)
        self.assertIn('$previousErrorActionPreference = $ErrorActionPreference', text)
        self.assertIn('$rolloutExitCode = $LASTEXITCODE', text)
        self.assertIn('$ErrorActionPreference = $previousErrorActionPreference', text)
        self.assertIn('if ($rolloutExitCode -ne 0)', text)

    def test_github_production_deploy_is_manual_exact_sha_and_pinned(self):
        workflow = (CODE / '.github' / 'workflows' / 'production.yml').read_text(encoding='utf-8')
        known_hosts = (CODE / 'ops' / 'known_hosts.production').read_text(encoding='utf-8')

        self.assertIn('workflow_dispatch:', workflow)
        self.assertNotIn('pull_request:', workflow)
        self.assertNotIn('branches: [main]', workflow)
        self.assertIn('environment: production', workflow)
        self.assertIn('[[ "$GITHUB_REF" == "refs/heads/main" ]]', workflow)
        self.assertIn('github.event.inputs.confirm', workflow)
        self.assertIn('deploy-production', workflow)
        self.assertIn('validated-$RELEASE_SHA', workflow)
        self.assertIn('release_body=', workflow)
        self.assertIn('Validated by GitHub Actions run $validation_run.', workflow)
        self.assertIn('gh run download "$VALIDATION_RUN_ID"', workflow)
        self.assertIn('--name "validated-release-$RELEASE_SHA"', workflow)
        self.assertNotIn('gh release download', workflow)
        self.assertIn('ops/release_evidence.py verify', workflow)
        self.assertIn('ops/deploy.ps1', workflow)
        self.assertIn('/opt/my-cash-flow-bot/current-release', workflow)
        self.assertIn('ops/readiness_smoke.py', workflow)
        self.assertIn('SSH_HOST: 31.131.31.117', workflow)
        self.assertIn("SSH_PORT: '22'", workflow)
        self.assertNotIn('secrets.SSH_HOST', workflow)
        self.assertNotIn('secrets.SSH_PORT', workflow)
        self.assertNotIn('ssh-keyscan', workflow)
        self.assertNotIn('StrictHostKeyChecking=no', workflow)
        self.assertIn('31.131.31.117 ssh-ed25519 ', known_hosts)

    def test_github_production_deploy_creates_verified_off_host_backup(self):
        workflow = (CODE / '.github' / 'workflows' / 'production.yml').read_text(encoding='utf-8')
        deploy_client = (CODE / 'ops' / 'deploy.ps1').read_text(encoding='utf-8')

        self.assertIn('secrets.BACKUP_ENCRYPTION_PASSPHRASE', workflow)
        self.assertIn('pg_dump', workflow)
        self.assertIn('project_name=${COMPOSE_PROJECT_NAME:-my-cash-flow-bot}', workflow)
        self.assertIn('--project-directory "$project_dir"', workflow)
        self.assertNotIn('project="mcf_${release_id:0:12}"', workflow)
        self.assertIn('openssl enc -aes-256-cbc -salt -pbkdf2', workflow)
        self.assertIn('id: database-backup', workflow)
        self.assertIn('production-db-backup-', workflow)
        self.assertIn('steps.database-backup.outputs.artifact-id', workflow)
        self.assertIn('actions/artifacts/$BACKUP_ARTIFACT_ID/zip', workflow)
        self.assertIn('OFF_HOST_BACKUP_SHA256', workflow)
        self.assertIn('"off_host_verified": True', workflow)
        self.assertIn('-BackupReceiptPath "$GITHUB_WORKSPACE/backup-evidence/backup-receipt.json"', workflow)
        self.assertIn('$backupCiphertext = Join-Path', deploy_client)
        self.assertIn('& $scpClient @scpArgs $backupCiphertext', deploy_client)

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

    def test_rollback_restarts_the_previous_release_images(self):
        text = (CODE / 'deploy_v0_on_vps.sh').read_text(encoding='utf-8')

        self.assertIn('PREVIOUS_RELEASE_ID=$(basename -- "$PREVIOUS_CURRENT")', text)
        self.assertIn(
            'RELEASE_ID="$PREVIOUS_RELEASE_ID" compose_at "$OLD_PROJECT" up -d --no-build',
            text,
        )

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
