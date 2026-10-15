"""Private, offline readiness adapter for external photo labels.

The labels file stays outside the repository. This adapter validates metadata only;
it never loads image bytes or invokes a provider. Prose labels are provisional
AI-assisted review notes, not machine-comparable ground truth.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_labels(labels_path: str | Path, image_root: str | Path | None = None) -> dict[str, Any]:
    path = Path(labels_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        raise ValueError("labels.items must be a list")
    files = [name for item in items for name in item.get("files", [])]
    hashes = [str(item.get("sha256", "")).lower() for item in items]
    valid_hashes = all(len(value) == 64 and all(c in "0123456789abcdef" for c in value) for value in hashes)
    report: dict[str, Any] = {
        "status": "ready_metadata_only",
        "labels_path": str(path),
        "file_count": len(files),
        "unique_sha256_count": len(set(hashes)),
        "declared_file_count": data.get("file_count"),
        "declared_unique_sha256_count": data.get("unique_sha256_count"),
        "sha256_format_valid": valid_hashes,
        "originals_unchanged": data.get("originals_unchanged") is True,
        "ground_truth": "provisional AI-assisted labels; not verified ground truth",
        "provider_calls": 0,
        "image_bytes_read": False,
        "comparison_metrics": "not computed",
        "exclusions": [
            "No external provider calls",
            "No credentials or env files read",
            "No private images copied or transmitted",
            "Prose labels are not sufficient for exact field-level scoring",
        ],
    }
    if image_root is not None:
        root = Path(image_root)
        available = 0
        verified = 0
        for item in items:
            for name in item.get("files", []):
                image = root / name
                if image.is_file():
                    available += 1
                    if _sha256(image).lower() == str(item.get("sha256", "")).lower():
                        verified += 1
        report.update({"image_root": str(root), "available_images": available, "sha256_verified_images": verified,
                       "image_bytes_read": available > 0,
                       "status": "ready_offline_corpus" if available == len(files) and verified == len(files) else "blocked_incomplete_corpus"})
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("labels", type=Path)
    parser.add_argument("--image-root", type=Path)
    args = parser.parse_args()
    print(json.dumps(inspect_labels(args.labels, args.image_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
