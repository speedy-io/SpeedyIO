#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${CASSANDRA_HOME_DIR:-}" ]]; then
  echo "ERROR: CASSANDRA_HOME_DIR env var must be set" >&2
  exit 1
fi
NODETOOL="${CASSANDRA_HOME_DIR}/bin/nodetool"
if [[ ! -x "$NODETOOL" ]]; then
  echo "ERROR: nodetool not found or not executable at: $NODETOOL" >&2
  exit 1
fi

while :; do
  TS="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "===== compactionstats @ $TS ====="
  "$NODETOOL" compactionstats 2>&1 || echo "[WARN] nodetool compactionstats failed at $TS" >&2
  echo
  sleep 30  # Nodetool is expensive (RPC, parsing full table metadata). 5 seconds is aggressive, 30–60 seconds is more common
done
