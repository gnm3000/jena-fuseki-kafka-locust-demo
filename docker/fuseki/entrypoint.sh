#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${FUSEKI_BASE:-/fuseki}/databases" "${FUSEKI_BASE:-/fuseki}/state" "${FUSEKI_BASE:-/fuseki}/extra"
export JVM_ARGS="${JAVA_OPTIONS:-${JVM_ARGS:--Xmx4G}}"
export MAIN="${MAIN:-server-plain}"

if [[ "${ENV_FUSEKI_GROUP_ID:-}" == *'${HOSTNAME}'* ]]; then
  export ENV_FUSEKI_GROUP_ID="${ENV_FUSEKI_GROUP_ID//'${HOSTNAME}'/${HOSTNAME:-unknown}}"
fi

exec "${FUSEKI_HOME:-/opt/fuseki}/fuseki-server" "$@"
