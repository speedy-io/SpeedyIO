#!/bin/bash
# vmstat_command_logger.sh
# Prints vmstat with wide columns, vmstat's timestamp, a single header,
# and (when supported) skips the initial since-boot row.
#
# Output is exactly what vmstat prints; we only skip the since-boot row on old builds via awk.

set -euo pipefail

# Hard-coded sampling interval (seconds)
readonly INTERVAL=1

# Ensure vmstat exists
if ! command -v vmstat >/dev/null 2>&1; then
  echo "ERROR: vmstat not found in PATH" >&2
  exit 127
fi

# Detect whether this vmstat supports --no-first (aka skip since-boot first row).
# If true, we can let vmstat handle it; otherwise we fall back to awk to drop the first data row.
HAS_NO_FIRST=false
if vmstat --help 2>&1 | grep -q -- '--no-first'; then
  HAS_NO_FIRST=true
fi

if $HAS_NO_FIRST; then
  # Use full flag names; skip since-boot row natively.
  vmstat --wide --timestamp --one-header --no-first "$INTERVAL"
else
  # Fallback: keep both header lines, drop the first data row.
  vmstat --wide --timestamp --one-header "$INTERVAL" | awk '
    NR==1 { print; next }          # header line 1
    NR==2 { print; skip=1; next }  # header line 2; mark to skip next data line
    skip  { skip=0; next }         # drop since-boot row
          { print }                # print subsequent instantaneous rows
  '
fi
