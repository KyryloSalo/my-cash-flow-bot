import datetime as dt
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "backup_receipt.py"
spec = importlib.util.spec_from_file_location("backup_receipt", MODULE_PATH)
backup_receipt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backup_receipt)


class BackupReceiptTests(unittest.TestCase):
    def _fixture(self, root: Path, *, age_minutes: int = 5):
        backup = root / "production.dump.p7m"
        backup.write_bytes(b"synthetic-encrypted-backup")
        digest = hashlib.sha256(backup.read_bytes()).hexdigest()
        now = dt.datetime(2026, 9, 15, 5, 0, tzinfo=dt.timezone.utc)
        receipt = {
            "version": 1,
            "backup_filename": backup.name,
            "server_backup_sha256": digest,
            "off_host_sha256": digest,
            "off_host_verified": True,
            "off_host_copy_id": "operator-vault/vydno/predeploy-20260915",
            "verified_at": (now - dt.timedelta(minutes=age_minutes)).isoformat(),
            "bytes": backup.stat().st_size,
        }
        receipt_path = root / "backup-receipt.json"
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        return receipt_path, backup, now, receipt

    def test_recent_matching_off_host_receipt_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt_path, backup, now, _ = self._fixture(Path(directory))
            self.assertEqual(backup_receipt.verify(receipt_path, now=now), backup.resolve())

    def test_mismatched_ciphertext_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt_path, backup, now, _ = self._fixture(Path(directory))
            backup.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "checksum"):
                backup_receipt.verify(receipt_path, now=now)

    def test_stale_or_unverified_off_host_receipt_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt_path, _, now, receipt = self._fixture(Path(directory), age_minutes=181)
            with self.assertRaisesRegex(ValueError, "fresh"):
                backup_receipt.verify(receipt_path, now=now)
            receipt["verified_at"] = now.isoformat()
            receipt["off_host_verified"] = False
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "off-host"):
                backup_receipt.verify(receipt_path, now=now)


if __name__ == "__main__":
    unittest.main()
