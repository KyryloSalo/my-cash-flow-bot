#!/usr/bin/env bash
# Send only to configured bot administrators; never print credentials.
set -euo pipefail
ROOT=/opt/my-cash-flow-bot
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if (( $# )); then
    text="[vydno.capital ops] $*"
else
    text=$(</dev/stdin)
fi
# Script travels as code via -c, message via stdin. Runtime config stays in place.
printf '%s' "$text" | docker exec -i my-cash-flow-bot-bot-1 python -c "$(<"$SCRIPT_DIR/notify_ops.py")"
