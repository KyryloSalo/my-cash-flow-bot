#!/bin/sh
set -eu
umask 077

AUTH_INCLUDE="/etc/nginx/conf.d/basic_auth.inc"
HTPASSWD_FILE="/etc/nginx/.htpasswd"

case "${NGINX_BASIC_AUTH_ENABLED:-false}" in
  true)
    if [ -z "${NGINX_BASIC_AUTH_USER:-}" ] || [ -z "${NGINX_BASIC_AUTH_PASSWORD:-}" ]; then
      printf '%s\n' 'ERROR: Basic Auth enabled but credentials are incomplete' >&2
      exit 1
    fi
    # Reject record separators; never put the password in a process argument.
    case "$NGINX_BASIC_AUTH_USER" in
      *:*|*'
'*) printf '%s\n' 'ERROR: Invalid Basic Auth username' >&2; exit 1;;
    esac
    HASH=$(printf '%s\n' "$NGINX_BASIC_AUTH_PASSWORD" | openssl passwd -6 -stdin)
    test -n "$HASH"
    printf '%s:%s\n' "$NGINX_BASIC_AUTH_USER" "$HASH" > "$HTPASSWD_FILE"
    # Workers need read permission; the parent directory is not public HTTP content.
    chmod 644 "$HTPASSWD_FILE"
    printf '%s\n' 'auth_basic "Restricted";' 'auth_basic_user_file /etc/nginx/.htpasswd;' > "$AUTH_INCLUDE"
    unset HASH NGINX_BASIC_AUTH_PASSWORD
    ;;
  false)
    printf '%s\n' '# basic auth explicitly disabled' > "$AUTH_INCLUDE"
    rm -f "$HTPASSWD_FILE"
    ;;
  *) printf '%s\n' 'ERROR: NGINX_BASIC_AUTH_ENABLED must be true or false' >&2; exit 1;;
esac
