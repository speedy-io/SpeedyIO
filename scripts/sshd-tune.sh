#!/usr/bin/env bash
# sshd-tune.sh  — persistently raise sshd limits and connection caps
# Usage:
#   sudo bash sshd-tune.sh           # apply
#   sudo bash sshd-tune.sh --check   # show current/effective values; no changes

set -euo pipefail

# ---- Desired settings ----
LIMIT_NOFILE=1048576
MAXSTARTUPS="400:50:800"
MAXSESSIONS=500
LOGINGRACETIME=30
USEDNS=no

MAIN=/etc/ssh/sshd_config
DIR=/etc/ssh/sshd_config.d
FINAL="$DIR/zz-final-tuning.conf"

detect_unit() {
  if systemctl cat sshd >/dev/null 2>&1; then
    echo sshd
  elif systemctl cat ssh >/dev/null 2>&1; then
    echo ssh
  else
    echo "ERROR: neither sshd nor ssh service found" >&2; exit 1
  fi
}

show_effective() {
  local unit="$1"
  echo "== Effective =="
  systemctl show "$unit" | grep -iE 'LimitNOFILE(Soft)?=' || true
  sshd -T | egrep -i '^(maxstartups|maxsessions|logingracetime|usedns) ' || true
  echo "== Config sources =="
  grep -RInE '^\s*(MaxStartups|MaxSessions|LoginGraceTime|UseDNS)\b' /etc/ssh/sshd_config{,.d/*} 2>/dev/null || true
}

require_root() {
  if [[ $EUID -ne 0 ]]; then
    echo "Run as root" >&2; exit 1
  fi
}

UNIT="$(detect_unit)"

if [[ "${1-}" == "--check" ]]; then
  show_effective "$UNIT"
  exit 0
fi

require_root

# 1) Write/overwrite final tuning include
mkdir -p "$DIR"
cat >"$FINAL" <<EOF
# Auto-added final overrides (persist across reboot)
MaxStartups $MAXSTARTUPS
MaxSessions $MAXSESSIONS
LoginGraceTime $LOGINGRACETIME
UseDNS $USEDNS
EOF

# 2) Ensure it's included at the very end of the main config
if ! grep -qE "^\s*Include\s+$FINAL\s*$" "$MAIN"; then
  printf '\nInclude %s\n' "$FINAL" >> "$MAIN"
fi

# 3) Raise RLIMIT_NOFILE for the ssh daemon via systemd drop-in
mkdir -p /etc/systemd/system/${UNIT}.service.d
cat >/etc/systemd/system/${UNIT}.service.d/10-limits.conf <<EOF
[Service]
LimitNOFILE=$LIMIT_NOFILE
EOF

# 4) Validate and apply
sshd -t
systemctl daemon-reload
systemctl restart "$UNIT"

# 5) Show results and flag duplicates (harmless but noisy)
show_effective "$UNIT"
DUPS=$(grep -RInE '^\s*(MaxStartups|MaxSessions|LoginGraceTime|UseDNS)\b' /etc/ssh/sshd_config.d 2>/dev/null | wc -l || true)
if (( DUPS > 1 )); then
  echo "NOTE: Multiple files set the same directives. Keep one (e.g., $FINAL) and remove extras." >&2
fi
