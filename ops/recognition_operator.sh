#!/usr/bin/env bash
# Fixed-purpose operator; never export raw samples or inspect container environments.
set -euo pipefail
expected_sha=${1:-}
action=${2:-}
if [[ ! "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || [[ ! "$action" =~ ^(status|activate|disable)$ ]]; then
  printf 'Invalid operator request\n' >&2
  exit 2
fi
trap 'printf "Recognition operator failed; capture activation is not confirmed.\n" >&2' ERR
root=/opt/my-cash-flow-bot
# Same host lock as deployment; never operate across a runtime switch.
exec 9> "$root/.deploy.lock"
flock -n 9 || { printf 'Production runtime is busy\n' >&2; exit 2; }
IFS= read -r deployed_sha < "$root/current-release"
[[ "$deployed_sha" == "$expected_sha" ]] || { printf 'Production SHA mismatch\n' >&2; exit 2; }
project_dir=$(readlink -f "$root/current")
[[ "$project_dir" == "$root/releases/$expected_sha" && -f "$project_dir/compose.yml" ]] || {
  printf 'Production release path mismatch\n' >&2; exit 2;
}
container_for() {
  docker ps --filter label=com.docker.compose.project=my-cash-flow-bot \
    --filter "label=com.docker.compose.service=$1" --format '{{.ID}}' 2>/dev/null
}
verify_container() {
  local id=$1 image=$2 state actual_image
  [[ "$id" =~ ^[0-9a-f]{12,64}$ ]] || { printf 'Runtime service missing or ambiguous\n' >&2; exit 2; }
  state=$(docker inspect --format '{{.State.Health.Status}}' "$id" 2>/dev/null)
  actual_image=$(docker inspect --format '{{.Config.Image}}' "$id" 2>/dev/null)
  [[ "$state" == healthy && "$actual_image" == "$image:$expected_sha" ]] || {
    printf 'Runtime service identity or health mismatch\n' >&2; exit 2;
  }
}
# Query only identity, mounts and health; never Config.Env or resolved compose.
admin_id=$(container_for admin)
verify_container "$admin_id" mcf-admin
# Disable remains available even if private storage prerequisites are broken.
if [[ "$action" != disable ]]; then
  worker_id=$(container_for worker)
  beat_id=$(container_for beat)
  verify_container "$worker_id" mcf-worker
  verify_container "$beat_id" mcf-worker
  admin_volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/var/lib/recognition-quality"}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}{{end}}' "$admin_id" 2>/dev/null)
  worker_volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/var/lib/recognition-quality"}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}{{end}}' "$worker_id" 2>/dev/null)
  [[ -n "$admin_volume" && "$admin_volume" == "$worker_volume" ]] || {
    printf 'Private persistent shared volume is not verified\n' >&2; exit 2;
  }
fi
printf 'Verified production runtime and private mount prerequisites for action=%s\n' "$action"
docker exec "$admin_id" python manage.py recognition_quality_operator --action "$action" 2>/dev/null
# Separate readback; never infer active behavior from a successful activation write.
if [[ "$action" != status ]]; then
  docker exec "$admin_id" python manage.py recognition_quality_operator --action status 2>/dev/null
fi
