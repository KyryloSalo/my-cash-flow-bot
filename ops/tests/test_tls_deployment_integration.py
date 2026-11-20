from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


class TLSDeploymentIntegrationTests(unittest.TestCase):
    def test_acme_webroot_is_persistent_and_read_only(self):
        compose = (ROOT / 'compose.yml').read_text(encoding='utf-8')
        self.assertIn('/opt/my-cash-flow-bot/shared/acme-webroot:/var/www/certbot:ro', compose)
        self.assertNotIn('./nginx/www:/var/www/certbot', compose)

    def test_release_checks_acme_and_installs_controls(self):
        deploy = (ROOT / 'deploy_v0_on_vps.sh').read_text(encoding='utf-8')
        self.assertIn('tls_ops.py', deploy)
        self.assertIn('acme-check', deploy)
        self.assertIn('tls_installer install', deploy)
        self.assertLess(deploy.index('acme-check'), deploy.rindex('> "$CURRENT_RELEASE"'))


if __name__ == '__main__':
    unittest.main()
