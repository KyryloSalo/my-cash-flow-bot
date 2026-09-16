FROM nginx:stable-bookworm@sha256:552e7481ca93ffccd046aa658dbbed22caefbc09c66fa7cd247cbb90b8a5c609

# OpenSSL is already a ca-certificates dependency in this pinned Debian image.
# Do not add floating apk/apt build inputs. Build fails if that contract changes.
RUN command -v openssl
COPY docker-entrypoint.d/ /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/*.sh
