from pathlib import Path
import re
import unittest

CODE=Path(__file__).resolve().parents[2]

class BuildInputTests(unittest.TestCase):
    def test_supported_framework_and_hashed_locks(self):
        self.assertIn('Django>=5.2,<5.3',(CODE/'admin_service/requirements.txt').read_text())
        for service in ['bot','admin_service']:
            lock=CODE/service/'requirements.lock'
            self.assertTrue(lock.exists())
            text=lock.read_text()
            self.assertIn('--hash=sha256:',text)
            docker=(CODE/service/'Dockerfile').read_text()
            self.assertIn('--require-hashes',docker)
            self.assertRegex(docker,r'FROM [^\n]+@sha256:[a-f0-9]{64}')
        self.assertRegex((CODE/'nginx/Dockerfile').read_text(),r'FROM [^\n]+@sha256:[a-f0-9]{64}')
        self.assertNotIn('apk add',(CODE/'nginx/Dockerfile').read_text())


if __name__=='__main__': unittest.main()
