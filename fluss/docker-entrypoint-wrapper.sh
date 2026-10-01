#!/bin/bash
# Sources /polaris/creds.env (if present and non-empty) before handing off
# to the base apache/fluss image's own /docker-entrypoint.sh. See the
# comment in fluss/Dockerfile for why this has to happen at container start
# rather than relying on Compose's `env_file:`.
if [ -s /polaris/creds.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /polaris/creds.env
  set +a
fi

exec /docker-entrypoint.sh "$@"
