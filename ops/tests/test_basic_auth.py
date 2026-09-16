import os
from pathlib import Path
import subprocess
import tempfile
import unittest

CODE = Path(__file__).resolve().parents[2]


class BasicAuthTests(unittest.TestCase):
    def test_enabled_missing_credentials_fails_closed(self):
        source = (CODE / 'nginx/docker-entrypoint.d/10-basic-auth.sh').read_text(encoding='utf-8')
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory)
            # Relocate only generated output paths, never read host secrets.
            source = source.replace('/etc/nginx/conf.d/basic_auth.inc', (dest/'basic.inc').as_posix())
            source = source.replace('/etc/nginx/.htpasswd', (dest/'auth.fixture').as_posix())
            for enabled,user,password,expected in [('true','','',False),('true','fixture','',False),('true','','fixture',False),('true','fixture','SYNTHETIC_PASSWORD',True),('false','','',True),('typo','','',False)]:
                env = dict(os.environ, NGINX_BASIC_AUTH_ENABLED=enabled, NGINX_BASIC_AUTH_USER=user, NGINX_BASIC_AUTH_PASSWORD=password)
                run = subprocess.run(['bash','-c',source],env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(run.returncode==0, expected, (enabled,bool(user),bool(password),run.stderr))
                self.assertNotIn('SYNTHETIC_PASSWORD',run.stdout+run.stderr)
                if expected and enabled=='true':
                    self.assertIn('auth_basic "Restricted";', (dest/'basic.inc').read_text())
                    self.assertTrue((dest/'auth.fixture').read_text().startswith('fixture:$6$'))
                if expected and enabled=='false':
                    self.assertNotIn('auth_basic "', (dest/'basic.inc').read_text())


if __name__ == '__main__':
    unittest.main()
