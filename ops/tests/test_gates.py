from pathlib import Path
import importlib.util
import tempfile
import json
import hashlib
import unittest

CODE=Path(__file__).resolve().parents[2]

class GateTests(unittest.TestCase):
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

if __name__=='__main__':unittest.main()
