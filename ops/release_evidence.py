"""Bind successful CI validation to exact source and runtime-equivalent images."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Callable


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify(directory):
    path = Path(directory)
    evidence = json.loads((path / "release-evidence.json").read_text())
    if evidence.get("validation") != "passed" or not re.fullmatch(
        "[a-f0-9]{40}", evidence.get("release_id", "")
    ):
        raise ValueError("Successful immutable CI evidence required")
    for filename, key in [
        ("release.zip", "source_sha256"),
        ("images.tar", "images_sha256"),
    ]:
        if sha(path / filename) != evidence.get(key):
            raise ValueError("Artifact checksum mismatch")
    expected = {
        f"mcf-{name}:{evidence['release_id']}" for name in ["bot", "admin", "nginx"]
    }
    if set(evidence.get("images", {})) != expected or not all(
        re.fullmatch("sha256:[a-f0-9]{64}", value)
        for value in evidence["images"].values()
    ):
        raise ValueError("Tested image identities missing")
    return evidence


def _saved_runtime_images(archive):
    images = {}
    with tarfile.open(archive) as stream:
        manifest_file = stream.extractfile("manifest.json")
        if manifest_file is None:
            raise ValueError("Saved image manifest is missing")
        manifest = json.load(manifest_file)
        for item in manifest:
            config_path = str(item.get("Config", ""))
            match = re.fullmatch(r"blobs/sha256/([a-f0-9]{64})", config_path)
            if match is None:
                raise ValueError("Saved image config path is unsafe")
            config_file = stream.extractfile(config_path)
            if config_file is None:
                raise ValueError("Saved image config is missing")
            payload = config_file.read()
            if hashlib.sha256(payload).hexdigest() != match.group(1):
                raise ValueError("Saved image config checksum mismatch")
            config = json.loads(payload)
            for ref in item.get("RepoTags") or []:
                if ref in images:
                    raise ValueError("Duplicate saved image tag")
                images[ref] = {
                    "id": f"sha256:{match.group(1)}",
                    "architecture": config.get("architecture"),
                    "os": config.get("os"),
                    "variant": config.get("variant"),
                    "config": config.get("config") or {},
                    "rootfs_type": (config.get("rootfs") or {}).get("type"),
                    "rootfs_layers": (config.get("rootfs") or {}).get("diff_ids") or [],
                }
    return images


def _inspect_image(ref):
    output = subprocess.check_output(["docker", "image", "inspect", ref], text=True)
    inspected = json.loads(output)
    if len(inspected) != 1:
        raise ValueError("Loaded image inspection is ambiguous")
    return inspected[0]


def _runtime_equivalent(expected, actual):
    rootfs = actual.get("RootFS") or {}
    return (
        actual.get("Architecture") == expected["architecture"]
        and actual.get("Os") == expected["os"]
        and actual.get("Variant") == expected["variant"]
        and (actual.get("Config") or {}) == expected["config"]
        and rootfs.get("Type") == expected["rootfs_type"]
        and (rootfs.get("Layers") or []) == expected["rootfs_layers"]
    )


def verify_images(directory, *, inspector: Callable[[str], dict] = _inspect_image):
    path = Path(directory)
    evidence = verify(path)
    saved = _saved_runtime_images(path / "images.tar")
    if set(saved) != set(evidence["images"]):
        raise ValueError("Saved image tags differ from tested evidence")
    for ref, tested_id in evidence["images"].items():
        expected = saved[ref]
        if expected["id"] != tested_id:
            raise ValueError("Saved image config differs from tested image")
        actual = inspector(ref)
        if actual.get("Id") != tested_id and not _runtime_equivalent(expected, actual):
            raise ValueError("Loaded image differs from tested image")


def main():
    action = sys.argv[1]
    path = Path(sys.argv[2])
    if action == "create":
        release_id = sys.argv[3]
        if not re.fullmatch("[a-f0-9]{40}", release_id):
            raise ValueError("Release must be commit SHA")
        refs = [f"mcf-{name}:{release_id}" for name in ["bot", "admin", "nginx"]]
        images = {
            ref: subprocess.check_output(
                ["docker", "image", "inspect", "--format={{.Id}}", ref], text=True
            ).strip()
            for ref in refs
        }
        evidence = {
            "validation": "passed",
            "release_id": release_id,
            "source_sha256": sha(path / "release.zip"),
            "images_sha256": sha(path / "images.tar"),
            "images": images,
        }
        (path / "release-evidence.json").write_text(json.dumps(evidence, indent=2))
    evidence = verify(path)
    if action == "verify-images":
        verify_images(path)
    (path / "checksums.txt").write_text(
        evidence["source_sha256"]
        + "  release.zip\n"
        + evidence["images_sha256"]
        + "  images.tar\n"
    )
    print(evidence["release_id"])


if __name__ == "__main__":
    main()
