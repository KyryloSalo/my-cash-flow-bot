import sys
import unittest
from unittest.mock import patch

from ops import readiness_smoke


class _Response:
    status = 403

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class ReadinessSmokeTests(unittest.TestCase):
    def test_route_failure_names_the_exact_url(self):
        with patch.object(readiness_smoke.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value = _Response()
            with self.assertRaisesRegex(RuntimeError, "https://example.test/blocked"):
                readiness_smoke.check_url("https://example.test/blocked", {200})

    def test_ip_allowlisted_admin_may_deny_an_unauthorized_probe(self):
        calls = []
        with patch.object(readiness_smoke, "check_url", side_effect=lambda url, expected: calls.append((url, expected))):
            with patch.object(sys, "argv", ["readiness_smoke.py"]):
                readiness_smoke.main()
        admin_call = next(call for call in calls if call[0] == "https://admin.vydno.capital/")
        self.assertEqual(admin_call[1], {200, 302, 401, 403})


if __name__ == "__main__":
    unittest.main()
