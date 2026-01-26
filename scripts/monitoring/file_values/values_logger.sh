#!/usr/bin/env bash
# values_logger.sh
# Log values of specific files at fixed intervals in CSV.
# Header = timestamp + full file paths. Each value is CSV-escaped and quoted.

set -uo pipefail

# === Config ===
INTERVAL=30  # seconds
FILES=(
  "/sys/kernel/mm/transparent_hugepage/khugepaged/pages_collapsed"
  # Add more full paths here, one per line
)

# === Helpers ===
csv_escape() {
  # Always quote; escape embedded quotes; strip newlines/CRs.
  local s="$1"
  s=${s//$'\n'/}
  s=${s//$'\r'/}
  s=${s//\"/\"\"}
  printf '"%s"' "$s"
}

read_value() {
  local f="$1"
  if [[ -r "$f" ]]; then
    tr -d '\n' < "$f"
  else
    printf 'warn: cannot read %s\n' "$f" >&2
    printf 'NA'
  fi
}

# === Header ===
if ((${#FILES[@]} == 0)); then
  echo "error: FILES array is empty" >&2
  exit 1
fi

printf "timestamp"
for f in "${FILES[@]}"; do
  printf ",%s" "$f"
done
printf "\n"

# Clean exit on Ctrl+C / kill
trap 'exit 0' INT TERM

# === Loop ===
while :; do
  ts=$(date +"%Y-%m-%d %H:%M:%S")
  printf "%s" "$ts"
  for f in "${FILES[@]}"; do
    val=$(read_value "$f")
    printf ","
    csv_escape "$val"
  done
  printf "\n"
  sleep "$INTERVAL"
done
