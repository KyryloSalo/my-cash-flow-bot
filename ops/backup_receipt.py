"""Verify a fresh operator-attested off-host encrypted backup receipt."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
from pathlib import Path
import re


MAX_RECEIPT_AGE = dt.timedelta(hours=3)
SHA256_RE = re.compile(r"[0-9a-f]{64}")
SAFE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def _parse_time(value: object) -> dt.datetime:
    if not isinstance(value, str):
        raise ValueError("Backup receipt verification time is missing")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Backup receipt verification time must include a timezone")
    return parsed.astimezone(dt.timezone.utc)


def verify(receipt_path: Path, *, now: dt.datetime | None = None) -> Path:
    receipt_path = Path(receipt_path).resolve()
    data = json.loads(receipt_path.read_text(encoding="utf-8"))
    if data.get("version") != 1:
        raise ValueError("Unsupported backup receipt version")
    if data.get("off_host_verified") is not True:
        raise ValueError("A verified off-host backup copy is required")
    copy_id = data.get("off_host_copy_id")
    if not isinstance(copy_id, str) or not copy_id.strip() or len(copy_id) > 256:
        raise ValueError("Off-host backup copy identifier is missing")
    filename = data.get("backup_filename")
    if not isinstance(filename, str) or not SAFE_NAME_RE.fullmatch(filename):
        raise ValueError("Unsafe backup filename")
    backup_path = (receipt_path.parent / filename).resolve()
    if backup_path.parent != receipt_path.parent or not backup_path.is_file() or backup_path.is_symlink():
        raise ValueError("Encrypted backup file is missing or unsafe")
    expected = data.get("server_backup_sha256")
    external = data.get("off_host_sha256")
    if not isinstance(expected, str) or not SHA256_RE.fullmatch(expected):
        raise ValueError("Server backup checksum is invalid")
    if not isinstance(external, str) or not SHA256_RE.fullmatch(external):
        raise ValueError("Off-host backup checksum is invalid")
    actual = hashlib.sha256(backup_path.read_bytes()).hexdigest()
    if not hmac.compare_digest(actual, expected) or not hmac.compare_digest(actual, external):
        raise ValueError("Backup checksum mismatch")
    if data.get("bytes") != backup_path.stat().st_size:
        raise ValueError("Backup size mismatch")
    current = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    verified_at = _parse_time(data.get("verified_at"))
    age = current - verified_at
    if age < dt.timedelta(minutes=-5) or age > MAX_RECEIPT_AGE:
        raise ValueError("A fresh backup receipt is required")
    return backup_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["verify"])
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    print(verify(args.receipt))


if __name__ == "__main__":
    main()
