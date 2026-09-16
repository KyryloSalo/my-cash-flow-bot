#!/usr/bin/env bash
# Server-only, operator-configured. No plaintext dump is written to disk.
set -euo pipefail
umask 077
: "${BACKUP_AGE_RECIPIENT:?Configure an independently recoverable age recipient}"
: "${BACKUP_REMOTE_PREFIX:?Configure an off-host rclone destination dedicated to this DB}"
: "${OPS_ALERT_EXECUTABLE:?Configure a local alert sender, independently monitored}"
[[ -x "$OPS_ALERT_EXECUTABLE" ]] || exit 2
for cmd in docker age rclone sha256sum; do command -v "$cmd" >/dev/null; done
OUT=${1:?Output directory required}
mkdir -p "$OUT"
name="mcf-$(date -u +%Y%m%dT%H%M%SZ).dump.age"
trap 'printf "%s\n" "Vydno database backup failed" | "$OPS_ALERT_EXECUTABLE"; exit 1' ERR
docker compose exec -T db sh -ec 'pg_dump --format=custom --no-owner --no-privileges -U "$POSTGRES_USER" "$POSTGRES_DB"' | age --recipient "$BACKUP_AGE_RECIPIENT" --output "$OUT/$name"
rclone copyto "$OUT/$name" "${BACKUP_REMOTE_PREFIX%/}/$name"
expected=$(sha256sum "$OUT/$name" | cut -d' ' -f1)
actual=$(rclone cat "${BACKUP_REMOTE_PREFIX%/}/$name" | sha256sum | cut -d' ' -f1)
[[ "$actual" == "$expected" ]]
printf '%s  %s\n' "$expected" "$name" > "$OUT/backup-checksum.txt"
printf '%s\n' 'Encrypted off-host backup uploaded and ciphertext checksum verified; restore not implied.'
