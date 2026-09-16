"""Allowlisted, deterministic release archive. Never traverse credential paths.

Both builder and installer enforce the same policy; host configuration is never
an archive member, deletion target, backup input, or synchronization target.
"""
from __future__ import annotations
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
import zipfile

ROOT_FILES = {'compose.yml', 'deploy_v0_on_vps.sh'}
ROOT_DIRS = {'bot', 'admin_service', 'nginx', 'ops', 'runtime_schema', '.github'}
BLOCKED_DIRS = {'__pycache__', '.git', '.venv', 'venv', '.venv-admin', 'node_modules', 'staticfiles', 'www', 'Archive', 'local_artifacts', 'certs', 'letsencrypt', 'secrets'}
SUFFIXES = {'.service', '.timer', '.ps1', '.py', '.txt', '.lock', '.sh', '.yml', '.yaml', '.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.ico', '.woff', '.woff2', '.ttf', '.webmanifest', '.po', '.mo', '.template', '.md'}
REQUIRED = {
    'compose.yml',
    'deploy_v0_on_vps.sh',
    'bot/app.py',
    'bot/bot_main.py',
    'bot/normalization_runtime.json',
    'admin_service/manage.py',
    'runtime_schema/__init__.py',
    'runtime_schema/statements.py',
}
MANIFEST = 'release-manifest.json'


def blocked_component(name: str) -> bool:
    low = name.lower()
    return (name in BLOCKED_DIRS or low.startswith(('.env', '.codex', '.pytest', '.mypy', '.tmp'))
            or any(word in low for word in ('credential', 'private', 'secret', 'htpasswd', 'id_rsa', 'id_ed25519'))
            or low.endswith(('.pem', '.key', '.sqlite', '.sqlite3', '.dump', '.zip', '.pyc')))


def allowed_path(value: str) -> bool:
    if not value or '\\' in value or ':' in value or value.startswith('/'):
        return False
    p = PurePosixPath(value)
    if p.as_posix() != value or any(part in {'.','..'} or blocked_component(part) for part in p.parts):
        return False
    if value in REQUIRED:
        return True
    if value in ROOT_FILES:
        return True
    if len(p.parts) < 2 or p.parts[0] not in ROOT_DIRS:
        return False
    if p.parts[0] == '.github' and p.parts[1] != 'workflows':
        return False
    return p.name in {'Dockerfile', '.dockerignore'} or p.suffix.lower() in SUFFIXES


def source_paths(root: Path):
    for name in sorted(ROOT_FILES):
        p = root/name
        if p.is_file() and not p.is_symlink():
            yield p
    for name in sorted(ROOT_DIRS):
        base=root/name
        if not base.is_dir() or base.is_symlink():
            continue
        for directory, dirs, files in os.walk(base, followlinks=False):
            dirs[:] = sorted(d for d in dirs if not blocked_component(d) and not (Path(directory)/d).is_symlink())
            for name in sorted(files):
                p=Path(directory)/name
                relative=p.relative_to(root).as_posix()
                if allowed_path(relative) and not p.is_symlink():
                    yield p


def build_archive(root: Path, output: Path) -> str:
    root=Path(root).resolve(); output=Path(output)
    paths=sorted(source_paths(root))
    names={p.relative_to(root).as_posix() for p in paths}
    if REQUIRED - names:
        raise ValueError('Required release source is missing')
    output.parent.mkdir(parents=True, exist_ok=True)
    hashes={}
    with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            name=path.relative_to(root).as_posix()
            content=path.read_bytes()
            hashes[name]=hashlib.sha256(content).hexdigest()
            info=zipfile.ZipInfo(name,date_time=(2020,1,1,0,0,0))
            info.compress_type=zipfile.ZIP_DEFLATED
            info.external_attr=(0o100755 if name.endswith('.sh') else 0o100644) << 16
            archive.writestr(info,content)
        archive.writestr(zipfile.ZipInfo(MANIFEST,date_time=(2020,1,1,0,0,0)),json.dumps({'version':1,'files':hashes},sort_keys=True))
    return hashlib.sha256(output.read_bytes()).hexdigest()


def validate_archive(archive: zipfile.ZipFile) -> dict:
    infos=archive.infolist(); names=[i.filename for i in infos]
    if len(names)!=len(set(names)) or MANIFEST not in names or len(names)>20000:
        raise ValueError('Invalid release manifest/duplicate members')
    if sum(i.file_size for i in infos)>500*1024*1024:
        raise ValueError('Archive too large')
    for info in infos:
        if info.filename != MANIFEST and not allowed_path(info.filename):
            raise ValueError('Archive contains a non-release path')
        if stat.S_ISLNK(info.external_attr >> 16):
            raise ValueError('Archive symlinks forbidden')
    manifest=json.loads(archive.read(MANIFEST))
    hashes=manifest.get('files',{})
    if manifest.get('version')!=1 or set(hashes)!=set(names)-{MANIFEST} or REQUIRED-set(hashes):
        raise ValueError('Manifest membership mismatch')
    for name,expected in hashes.items():
        if hashlib.sha256(archive.read(name)).hexdigest()!=expected:
            raise ValueError('Release content checksum mismatch')
    return hashes


def install_archive(archive_path: Path, target: Path, *, expected_sha256: str) -> None:
    expected = str(expected_sha256 or '').strip().lower()
    if len(expected) != 64 or any(ch not in '0123456789abcdef' for ch in expected):
        raise ValueError('A valid external SHA256 is required')
    actual = hashlib.sha256(Path(archive_path).read_bytes()).hexdigest()
    if not hmac.compare_digest(actual, expected):
        raise ValueError('Artifact SHA256 mismatch')
    target=Path(target).resolve()
    if target.exists():
        raise ValueError('Release target must not already exist')
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f'.{target.name}.release-stage-', dir=target.parent)).resolve()
    try:
        with zipfile.ZipFile(archive_path) as archive:
            hashes=validate_archive(archive)  # Validate the WHOLE archive before any write.
            for name in hashes:
                dest=stage/name
                if not dest.resolve().is_relative_to(stage):
                    raise ValueError('Unsafe destination')
            for name in hashes:
                dest=stage/name; dest.parent.mkdir(parents=True,exist_ok=True)
                dest.write_bytes(archive.read(name))
                dest.chmod(0o755 if name.endswith('.sh') else 0o644)
        # Release files contain no host-owned secrets and several directories are
        # intentionally bind-mounted into unprivileged runtime containers.
        for current, _, _ in os.walk(stage):
            directory = Path(current)
            directory.chmod(0o755)
        os.replace(stage, target)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['build','install','verify'])
    parser.add_argument('--source',type=Path)
    parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--target',type=Path)
    parser.add_argument('--sha256')
    args=parser.parse_args()
    if args.action=='build':
        if args.source is None: parser.error('--source required')
        print(build_archive(args.source,args.archive)); return
    if args.sha256 and hashlib.sha256(args.archive.read_bytes()).hexdigest()!=args.sha256:
        raise SystemExit('Artifact SHA256 mismatch')
    if args.action=='verify':
        with zipfile.ZipFile(args.archive) as z: validate_archive(z)
        print('Artifact manifest verified'); return
    if args.target is None: parser.error('--target required')
    if not args.sha256: parser.error('--sha256 required for install')
    install_archive(args.archive,args.target,expected_sha256=args.sha256)
    print('Allowlisted source installed; host-owned files untouched')


if __name__=='__main__': main()
