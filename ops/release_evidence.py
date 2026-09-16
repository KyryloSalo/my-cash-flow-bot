"""Bind successful CI validation to exact source archive and tested image IDs."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def verify(directory):
    p=Path(directory); d=json.loads((p/'release-evidence.json').read_text())
    if d.get('validation')!='passed' or not re.fullmatch('[a-f0-9]{40}',d.get('release_id','')):
        raise ValueError('Successful immutable CI evidence required')
    for file,key in [('release.zip','source_sha256'),('images.tar','images_sha256')]:
        if sha(p/file)!=d.get(key): raise ValueError('Artifact checksum mismatch')
    expected={'mcf-'+n+':'+d['release_id'] for n in ['bot','admin','nginx']}
    if set(d.get('images',{}))!=expected or not all(re.fullmatch('sha256:[a-f0-9]{64}',v) for v in d['images'].values()):
        raise ValueError('Tested image identities missing')
    return d


def main():
    action=sys.argv[1];p=Path(sys.argv[2])
    if action=='create':
        release_id=sys.argv[3]
        if not re.fullmatch('[a-f0-9]{40}',release_id): raise ValueError('Release must be commit SHA')
        refs=['mcf-'+n+':'+release_id for n in ['bot','admin','nginx']]
        images={ref:subprocess.check_output(['docker','image','inspect','--format={{.Id}}',ref],text=True).strip() for ref in refs}
        d={'validation':'passed','release_id':release_id,'source_sha256':sha(p/'release.zip'),'images_sha256':sha(p/'images.tar'),'images':images}
        (p/'release-evidence.json').write_text(json.dumps(d,indent=2))
    d=verify(p)
    if action=='verify-images':
        for ref,expected in d['images'].items():
            actual=subprocess.check_output(['docker','image','inspect','--format={{.Id}}',ref],text=True).strip()
            if actual!=expected: raise ValueError('Loaded image differs from tested image')
    (p/'checksums.txt').write_text(d['source_sha256']+'  release.zip\n'+d['images_sha256']+'  images.tar\n')
    print(d['release_id'])


if __name__=='__main__':main()
