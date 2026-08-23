#!/usr/bin/env bash
set -euo pipefail

exec java ${JAVA_OPTIONS:--Xmx1536m -XX:+ExitOnOutOfMemoryError -Duser.timezone=UTC} \
    -jar /fuseki/delta-fuseki.jar "$@"
