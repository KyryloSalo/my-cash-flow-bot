#!/usr/bin/env bash
# Run ONLY after new human production approval. No implicit git pull/build.
set -euo pipefail
umask 077
[[ "${1:-}" == --confirm-production && "${3:-}" == --source-sha256 && ( $# == 4 || ( $# == 6 && "${5:-}" == --backup-receipt ) ) ]] || { printf '%s\n' 'Explicit production confirmation, artifact directory, trusted source SHA256 and optional backup receipt required' >&2; exit 2; }
ARTIFACT=$(cd "$2" && pwd)
SOURCE_SHA256=$4
[[ "$SOURCE_SHA256" =~ ^[0-9a-fA-F]{64}$ ]] || { printf '%s\n' 'Invalid source SHA256' >&2; exit 2; }
BACKUP_RECEIPT=''
if [[ $# == 6 ]]; then
    [[ -f "$6" && ! -L "$6" ]] || { printf '%s\n' 'Backup receipt is missing or unsafe' >&2; exit 2; }
    BACKUP_RECEIPT=$(readlink -f "$6")
fi
ROOT=/opt/my-cash-flow-bot
RELEASES="$ROOT/releases"
CURRENT="$ROOT/current"
CURRENT_RELEASE="$ROOT/current-release"
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
OLD_PROJECT="$ROOT"
PREVIOUS_CURRENT=''
PREVIOUS_RELEASE_ID=''
if [[ -L "$CURRENT" && -f "$CURRENT_RELEASE" ]]; then
    candidate_current=$(readlink -f "$CURRENT")
    IFS= read -r recorded_release < "$CURRENT_RELEASE" || true
    if [[ -f "$candidate_current/compose.yml" && "$(basename -- "$candidate_current")" == "$recorded_release" ]]; then
        OLD_PROJECT=$candidate_current
        PREVIOUS_CURRENT=$candidate_current
        PREVIOUS_RELEASE_ID=$(basename -- "$PREVIOUS_CURRENT")
    fi
fi
PROJECT_NAME=${COMPOSE_PROJECT_NAME:-my-cash-flow-bot}
compose_at() {
    local project=$1
    shift
    docker compose --env-file "$HOST_ENV_FILE" -p "$PROJECT_NAME" --project-directory "$project" -f "$project/compose.yml" "$@"
}
export HOST_ENV_FILE="$ROOT/.env"
cd "$OLD_PROJECT"
# Serialize deployments; preserve all host-owned configuration/certificates.
exec 9> "$ROOT/.deploy.lock"
flock -n 9 || { printf '%s\n' 'Another deployment owns the lock' >&2; exit 2; }
RELEASE_ID=$(python3 "$SCRIPT_DIR/ops/release_evidence.py" verify "$ARTIFACT")
[[ "$RELEASE_ID" =~ ^[A-Za-z0-9._-]{1,128}$ ]] || { printf '%s\n' 'Invalid release ID' >&2; exit 2; }
export RELEASE_ID
# Checksums are checked again on the exact remote target before docker load.
(cd "$ARTIFACT" && sha256sum --check checksums.txt)
python3 "$SCRIPT_DIR/ops/release_artifact.py" verify --archive "$ARTIFACT/release.zip" --sha256 "$SOURCE_SHA256"
RELEASE_DIR="$RELEASES/$RELEASE_ID"
mkdir -p "$RELEASES"
[[ ! -e "$RELEASE_DIR" ]] || { printf '%s\n' 'Release ID is already installed; no services were stopped' >&2; exit 2; }
DJANGO_SECRET_DIR="$ROOT/runtime-secrets"
export DJANGO_SECRET_HOST_PATH="$DJANGO_SECRET_DIR/django_secret_key"
export TELEGRAM_OIDC_CLIENT_ID_HOST_PATH="$DJANGO_SECRET_DIR/telegram_oidc_client_id"
export TELEGRAM_OIDC_CLIENT_SECRET_HOST_PATH="$DJANGO_SECRET_DIR/telegram_oidc_client_secret"
if [[ ! -e "$DJANGO_SECRET_HOST_PATH" ]]; then
    install -d -m 700 "$DJANGO_SECRET_DIR"
    secret_tmp="$DJANGO_SECRET_DIR/.django-secret-$RELEASE_ID"
    openssl rand -base64 48 > "$secret_tmp"
    chmod 600 "$secret_tmp"
    mv -T "$secret_tmp" "$DJANGO_SECRET_HOST_PATH"
fi
[[ -f "$DJANGO_SECRET_HOST_PATH" && ! -L "$DJANGO_SECRET_HOST_PATH" && -s "$DJANGO_SECRET_HOST_PATH" ]] || { printf '%s\n' 'Django secret file is missing or unsafe' >&2; exit 2; }
[[ -f "$TELEGRAM_OIDC_CLIENT_ID_HOST_PATH" && ! -L "$TELEGRAM_OIDC_CLIENT_ID_HOST_PATH" && -s "$TELEGRAM_OIDC_CLIENT_ID_HOST_PATH" ]] || { printf '%s\n' 'Telegram OIDC client ID file is missing or unsafe' >&2; exit 2; }
[[ -f "$TELEGRAM_OIDC_CLIENT_SECRET_HOST_PATH" && ! -L "$TELEGRAM_OIDC_CLIENT_SECRET_HOST_PATH" && -s "$TELEGRAM_OIDC_CLIENT_SECRET_HOST_PATH" ]] || { printf '%s\n' 'Telegram OIDC client secret file is missing or unsafe' >&2; exit 2; }
compose_at "$OLD_PROJECT" version >/dev/null
compose_at "$OLD_PROJECT" config --quiet
BACKUP_DIR="$ROOT/Archive/$(date -u +%Y%m%dT%H%M%SZ)-$RELEASE_ID"
mkdir -p "$BACKUP_DIR"
if ! python3 "$SCRIPT_DIR/ops/release_artifact.py" build --source "$OLD_PROJECT" --archive "$BACKUP_DIR/source.zip"; then
    legacy_items=()
    for item in bot admin_service nginx ops runtime_schema compose.yml requirements.txt deploy_v0_on_vps.sh; do
        [[ -e "$OLD_PROJECT/$item" ]] && legacy_items+=("$item")
    done
    (( ${#legacy_items[@]} > 0 )) || { printf '%s\n' 'No safe legacy source paths found for backup' >&2; exit 2; }
    (
        cd "$OLD_PROJECT"
        tar \
            --exclude='.env*' \
            --exclude='*.pem' \
            --exclude='*.key' \
            --exclude='*.sqlite3' \
            --exclude='__pycache__' \
            --exclude='.pytest_cache' \
            --exclude='staticfiles' \
            -czf "$BACKUP_DIR/legacy-source.tar.gz" \
            "${legacy_items[@]}"
    )
fi
compose_at "$OLD_PROJECT" images --format json > "$BACKUP_DIR/images-before.json"
# Encrypted off-host backup must be verified BEFORE stopping services or migrating.
if [[ -n "$BACKUP_RECEIPT" ]]; then
    VERIFIED_BACKUP=$(python3 "$SCRIPT_DIR/ops/backup_receipt.py" verify "$BACKUP_RECEIPT")
    cp -- "$VERIFIED_BACKUP" "$BACKUP_DIR/"
    cp -- "$BACKUP_RECEIPT" "$BACKUP_DIR/backup-receipt.json"
    sha256sum "$BACKUP_DIR/$(basename -- "$VERIFIED_BACKUP")" > "$BACKUP_DIR/backup-checksum.txt"
    printf '%s\n' 'Fresh encrypted off-host backup receipt verified.'
else
    bash "$SCRIPT_DIR/ops/backup_database.sh" "$BACKUP_DIR"
fi
python3 "$SCRIPT_DIR/ops/release_artifact.py" install --archive "$ARTIFACT/release.zip" --target "$RELEASE_DIR" --sha256 "$SOURCE_SHA256"
python3 - "$RELEASE_DIR" <<'PY'
from pathlib import Path
import stat
import sys

root = Path(sys.argv[1])
directories = [root, root / 'nginx', root / 'nginx' / 'landing', root / 'nginx' / 'docker-entrypoint.d']
files = [root / 'nginx' / 'landing' / 'index.html', root / 'nginx' / 'cashflowbot.conf.template']
if any((stat.S_IMODE(path.stat().st_mode) & 0o005) != 0o005 for path in directories) or any(
    (stat.S_IMODE(path.stat().st_mode) & 0o004) != 0o004 for path in files
):
    raise SystemExit('Release tree is not readable by unprivileged containers')
PY
rollback_stop() {
    printf '%s\n' 'RELEASE FAILED. Stopping candidate and restoring the previous application release; database is never auto-restored.' >&2
    compose_at "${ACTIVE_PROJECT:-$OLD_PROJECT}" stop nginx bot api admin worker beat || true
    if [[ "${ACTIVE_PROJECT:-}" == "$CURRENT" ]]; then
        if [[ -n "$PREVIOUS_CURRENT" ]]; then
            ln -s "$PREVIOUS_CURRENT" "$ROOT/.rollback-current-$RELEASE_ID"
            mv -Tf "$ROOT/.rollback-current-$RELEASE_ID" "$CURRENT"
        else
            rm -f "$CURRENT"
        fi
    fi
    if [[ -n "$PREVIOUS_RELEASE_ID" ]]; then
        RELEASE_ID="$PREVIOUS_RELEASE_ID" compose_at "$OLD_PROJECT" up -d --no-build || true
    else
        compose_at "$OLD_PROJECT" up -d --no-build || true
    fi
}
trap rollback_stop ERR
ACTIVE_PROJECT=$OLD_PROJECT
compose_at "$OLD_PROJECT" stop nginx bot api admin worker beat
docker load --input "$ARTIFACT/images.tar" >/dev/null
python3 "$SCRIPT_DIR/ops/release_evidence.py" verify-images "$ARTIFACT"
ln -s "$RELEASE_DIR" "$ROOT/.current-$RELEASE_ID"
mv -Tf "$ROOT/.current-$RELEASE_ID" "$CURRENT"
ACTIVE_PROJECT=$CURRENT
cd "$CURRENT"
compose_at "$CURRENT" config --quiet
# The schema job is one-shot and MUST be recreated for each version.
compose_at "$CURRENT" rm -sf schema
compose_at "$CURRENT" up -d --no-build --wait --wait-timeout 240 db redis
# Serving/worker/beat depend on schema service_completed_successfully.
compose_at "$CURRENT" up -d --no-build --wait --wait-timeout 300
# Fail even if the HTTP server is running with an outdated database.
compose_at "$CURRENT" exec -T admin python manage.py migrate --check
compose_at "$CURRENT" exec -T admin python manage.py check --deploy --fail-level WARNING
python3 "$CURRENT/ops/readiness_smoke.py"
printf '%s\n' "$RELEASE_ID" > "$CURRENT_RELEASE"
trap - ERR
printf '%s\n' 'Release readiness passed; production restore is NOT verified by this deployment.'
