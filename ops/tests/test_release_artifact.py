import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

CODE = Path(__file__).resolve().parents[2]


class ReleaseArtifactTests(unittest.TestCase):
    def test_allowlist_and_install_preserve_host_owned_files(self):
        path = CODE / 'ops/release_artifact.py'
        self.assertTrue(path.exists(), 'allowlisted artifact builder missing')
        self.assertIn('directory.chmod(0o755)', path.read_text(encoding='utf-8'))
        spec = importlib.util.spec_from_file_location('release_artifact', path)
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        good = ['compose.yml','deploy_v0_on_vps.sh','bot/app.py','bot/bot_main.py','bot/requirements.lock','bot/normalization_runtime.json','admin_service/manage.py','nginx/landing/index.html','ops/validate_release.sh','runtime_schema/__init__.py','runtime_schema/statements.py']
        bad = ['.env','bot/.env.local','bot/private.pem','bot/x.key','bot/credentials.json','../bot/app.py','bot\\app.py','nginx/www/proof.txt','bot/__pycache__/app.py','bot/.venv/a.py','admin_service/staticfiles/a.css','unknown.py','bot/data.dump','bot/test.sqlite3','.codex-deploy-keys/x']
        self.assertTrue(all(m.allowed_path(p) for p in good))
        self.assertFalse(any(m.allowed_path(p) for p in bad))
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'source'; dst=Path(tmp)/'target'; src.mkdir()
            for name in good:
                p=src/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_text('synthetic source')
            # Host-owned sentinel is not a credential and never appears in archive.
            sentinel=Path(tmp)/'host-owned.fixture'; sentinel.write_text('KEEP')
            archive=Path(tmp)/'release.zip'
            expected_sha=m.build_archive(src,archive)
            names=zipfile.ZipFile(archive).namelist()
            self.assertEqual(set(names),set(good)|{'release-manifest.json'})
            m.install_archive(archive,dst,expected_sha256=expected_sha)
            self.assertEqual(sentinel.read_text(),'KEEP')
            self.assertEqual((dst/'bot/app.py').read_text(),'synthetic source')
            with zipfile.ZipFile(archive,'a') as z: z.writestr('../escape.py','bad')
            tampered_sha = m.hashlib.sha256(archive.read_bytes()).hexdigest()
            with self.assertRaises(ValueError):
                m.install_archive(archive,Path(tmp)/'tampered-target',expected_sha256=tampered_sha)
            self.assertFalse((Path(tmp)/'escape.py').exists())

    def test_install_requires_external_digest_and_is_atomic(self):
        path = CODE / 'ops/release_artifact.py'
        spec = importlib.util.spec_from_file_location('release_artifact_atomic', path)
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'source'; src.mkdir()
            for name in m.REQUIRED:
                p=src/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_text('source')
            archive=Path(tmp)/'release.zip'; expected_sha=m.build_archive(src,archive)
            with self.assertRaises(ValueError):
                m.install_archive(archive,Path(tmp)/'wrong-hash',expected_sha256='0'*64)
            with self.assertRaises(TypeError):
                m.install_archive(archive,Path(tmp)/'missing-hash')
            occupied=Path(tmp)/'occupied'; occupied.mkdir()
            with self.assertRaises(ValueError):
                m.install_archive(archive,occupied,expected_sha256=expected_sha)


if __name__=='__main__': unittest.main()
