#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# generate_fstab.sh
#
# Prints fstab-compatible lines for currently-mounted filesystems with UUIDs.
# With --apply (run as root), appends only lines whose (UUID, mountpoint) pair
# is not already present in /etc/fstab — and never twice within the same run.
# -----------------------------------------------------------------------------

set -euo pipefail

FSTAB="/etc/fstab"
APPLY=0
[[ ${1:-} == "--apply" ]] && { APPLY=1; (( EUID == 0 )) || { echo "Need sudo/root"; exit 1; }; }

# We'll dedup by key: "<UUID>|||<mountpoint>"
declare -A WANT=()
declare -A HAVE=()

# -------- Load existing pairs from /etc/fstab (UUID + mountpoint) ------------
# Handles any whitespace; normalizes swap mountpoint to literal "swap".
if [[ -r "$FSTAB" ]]; then
  while read -r f1 f2 f3 _; do
    # skip blanks/comments
    [[ -z "${f1:-}" || "${f1:0:1}" == "#" ]] && continue
    # only consider UUID= lines
    [[ "${f1:-}" == UUID=* ]] || continue

    uuid="${f1#UUID=}"
    mnt="${f2:-}"
    fstype="${f3:-}"

    # normalize swap mountpoint
    if [[ "$fstype" == "swap" ]]; then
      mnt="swap"
    fi

    # guard against empty fields
    [[ -n "$uuid" && -n "$mnt" ]] || continue

    HAVE["$uuid|||$mnt"]=1
  done < "$FSTAB"
fi

# ------------------ Collect desired entries from lsblk -----------------------
# Use lsblk; it may list the same fs via multiple device names -> dedup by key.
while read -r dev mountpoint fstype uuid; do
  # need all fields
  [[ -n "${mountpoint:-}" && -n "${uuid:-}" && -n "${fstype:-}" ]] || continue

  # normalize swap mountpoint
  if [[ "$fstype" == "swap" ]]; then
    mountpoint="swap"
  fi

  key="${uuid}|||${mountpoint}"
  WANT["$key"]=1
done < <(lsblk -pn -o NAME,MOUNTPOINT,FSTYPE,UUID)

# Keep a stable output order: sort by key (uuid|||mountpoint)
mapfile -t ORDERED_KEYS < <(printf '%s\n' "${!WANT[@]}" | sort)

# ----------------------- Emit and optionally apply ---------------------------
for key in "${ORDERED_KEYS[@]}"; do
  uuid="${key%%|||*}"
  mountpoint="${key#*|||}"

  entry="UUID=${uuid}  ${mountpoint}  $(lsblk -no FSTYPE "$(findfs UUID=$uuid 2>/dev/null)" 2>/dev/null | head -n1)  defaults 0 0"

  # If we couldn't resolve fstype back from uuid (rare), fallback to heuristics:
  if [[ "${entry##*  }" == "  defaults 0 0" ]]; then
    # last FSTYPE extraction failed; guess swap vs non-swap by mountpoint
    if [[ "$mountpoint" == "swap" ]]; then
      entry="UUID=${uuid}  swap  swap  defaults 0 0"
    else
      # best effort: ext4 default; user can edit later
      entry="UUID=${uuid}  ${mountpoint}  ext4  defaults 0 0"
    fi
  fi

  echo "$entry"

  if (( APPLY )); then
    if [[ -n "${HAVE[$key]:-}" ]]; then
      echo "ℹ️  Skipped (already present)"
      continue
    fi
    echo "$entry" >> "$FSTAB"
    # mark as present so duplicates in the same run are also skipped
    HAVE["$key"]=1
    echo "✅ Added"
  fi
done
