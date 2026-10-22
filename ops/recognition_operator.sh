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
  if [[ "$image" == *@* ]]; then
    [[ "$state" == healthy && "$actual_image" == "$image" ]] || {
      printf 'Runtime service identity or health mismatch\n' >&2; exit 2;
    }
  elif [[ "$state" != healthy || "$actual_image" != "$image:$expected_sha" ]]; then
    printf 'Runtime service identity or health mismatch\n' >&2; exit 2;
  fi
}
# Query only identity, mounts, health and network addresses; never Config.Env.
admin_id=$(container_for admin)
verify_container "$admin_id" mcf-admin
# Disable remains available even if private storage or database prerequisites are broken.
if [[ "$action" != disable ]]; then
  db_id=$(container_for db)
  verify_container "$db_id" 'postgres:16-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825'
  db_ips=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{.GlobalIPv6Address}}{{end}}' "$db_id" 2>/dev/null)
  db_ips=${db_ips// /,}
  [[ -n "$db_ips" ]] || { printf 'PostgreSQL network address is not verified\n' >&2; exit 2; }
  worker_id=$(container_for worker)
  beat_id=$(container_for beat)
  verify_container "$worker_id" mcf-admin
  verify_container "$beat_id" mcf-admin
  docker exec "$worker_id" python -m common.recognition_database_readiness "$db_ips" 5432 2>/dev/null | grep -qx READY || {
    printf 'Worker PostgreSQL direct-connection readiness is not verified\n' >&2; exit 2;
  }
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
