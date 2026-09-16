import copy
import hashlib
import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

CODE=Path(__file__).resolve().parents[2]

class GateTests(unittest.TestCase):
    def _portable_image_fixture(self, path):
        release_id = 'a' * 40
        configs = {}
        manifest = []
        images = {}
        actual = {}
        for name in ['bot', 'admin', 'nginx']:
            ref = f'mcf-{name}:{release_id}'
            config = {
                'architecture': 'amd64',
                'os': 'linux',
                'config': {
                    'Cmd': ['run', name],
                    'Entrypoint': None,
                    'Env': ['APP_ENV=test'],
                    'Labels': {'fixture': name},
                    'User': '',
                    'WorkingDir': '/app',
                },
                'rootfs': {'type': 'layers', 'diff_ids': [f'sha256:{name:0<64}']},
                'history': [{'created_by': f'fixture {name}'}],
            }
            payload = json.dumps(config, sort_keys=True, separators=(',', ':')).encode()
            digest = hashlib.sha256(payload).hexdigest()
            config_path = f'blobs/sha256/{digest}'
            configs[config_path] = payload
            manifest.append({'Config': config_path, 'RepoTags': [ref], 'Layers': []})
            images[ref] = f'sha256:{digest}'
            actual[ref] = {
                'Id': 'sha256:' + 'f' * 64,
                'Architecture': config['architecture'],
                'Os': config['os'],
                'Config': copy.deepcopy(config['config']),
                'RootFS': {
                    'Type': config['rootfs']['type'],
                    'Layers': list(config['rootfs']['diff_ids']),
                },
            }
        release = path / 'release.zip'
        release.write_bytes(b'SYNTHETIC_SOURCE')
        image_archive = path / 'images.tar'
        with tarfile.open(image_archive, 'w') as stream:
            for member_name, payload in {
                'manifest.json': json.dumps(manifest).encode(),
                **configs,
            }.items():
                info = tarfile.TarInfo(member_name)
                info.size = len(payload)
                stream.addfile(info, io.BytesIO(payload))
        evidence = {
            'validation': 'passed',
            'release_id': release_id,
            'source_sha256': hashlib.sha256(release.read_bytes()).hexdigest(),
            'images_sha256': hashlib.sha256(image_archive.read_bytes()).hexdigest(),
            'images': images,
        }
        (path / 'release-evidence.json').write_text(json.dumps(evidence))
        return actual

    def test_evidence_cannot_pass_failed_validation_or_changed_artifact(self):
        path=CODE/'ops/release_evidence.py'
        self.assertTrue(path.exists(),'immutable artifact gate missing')
        spec=importlib.util.spec_from_file_location('ops_evidence',path)
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)
            for f in ['release.zip','images.tar']: (p/f).write_bytes(b'SYNTHETIC_GATE_FIXTURE')
            sha=hashlib.sha256(b'SYNTHETIC_GATE_FIXTURE').hexdigest()
            evidence={'validation':'failed','release_id':'a'*40,'source_sha256':sha,'images_sha256':sha,'images':{'mcf-'+name+':'+'a'*40:'sha256:'+'b'*64 for name in ['bot','admin','nginx']}}
            (p/'release-evidence.json').write_text(json.dumps(evidence))
            with self.assertRaises(ValueError):m.verify(p)
            evidence['validation']='passed';(p/'release-evidence.json').write_text(json.dumps(evidence))
            self.assertEqual(m.verify(p)['release_id'],'a'*40)
            (p/'images.tar').write_bytes(b'ALTERED')
            with self.assertRaises(ValueError):m.verify(p)

    def test_loaded_images_accept_runtime_equivalence_after_daemon_normalization(self):
        path = CODE / 'ops/release_evidence.py'
        spec = importlib.util.spec_from_file_location('ops_evidence', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            actual = self._portable_image_fixture(directory)

            module.verify_images(directory, inspector=lambda ref: actual[ref])

    def test_loaded_images_reject_runtime_config_changes(self):
        path = CODE / 'ops/release_evidence.py'
        spec = importlib.util.spec_from_file_location('ops_evidence', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            actual = self._portable_image_fixture(directory)
            ref = f"mcf-bot:{'a' * 40}"
            actual[ref]['Config']['Cmd'] = ['tampered']

            with self.assertRaises(ValueError):
                module.verify_images(directory, inspector=lambda image: actual[image])

if __name__=='__main__':unittest.main()
