# Android pilot/privacy runbook

1. Use an opt-in owner of a test account; record app/build/parser/schema versions and device/OS, never raw pushes or credentials.
2. Verify the real installed bank package ID and observed fields on-device. Add it only to a server-delivered allowlist after review.
3. Explain that Notification Access can expose notifications broadly; request no SMS, contacts, accessibility, or bank login.
4. Pair from the authenticated Vydno web UI. Confirm the adapter receives only a scoped submit credential.
5. Test app closed, locked screen, offline/reconnect, reboot, duplicate delivery, notification update, revoke, kill switch, and account deletion.
6. Confirm a purchase creates one review item and only explicit user confirmation changes the ledger. Test OTP/transfer/refund/income and unknown packages are not expenses.
7. Capture only non-sensitive IDs/status/timestamps and whether the result was accepted, review, conflict, or revoked.

No physical-device or production evidence is included in this repository.
