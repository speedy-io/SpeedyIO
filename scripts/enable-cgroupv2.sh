#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# enable-cgv2.sh : Switch EL 8 hosts (CentOS / RHEL / Stream / Rocky / Alma)
#                  to a pure cgroup v2 hierarchy that supports NVMe I/O
#                  throttling via the v2 'io' controller.
#
# WHAT IT DOES
#   1. Ensures 'grubby' is installed.
#   2. Detects whether the system boots with a Boot-Loader-Spec (BLS) menu
#      or a legacy /boot/grub2/grub.cfg menu.
#   3. Appends the two required kernel flags to *every* kernel stanza:
#          systemd.unified_cgroup_hierarchy=1 cgroup_no_v1=all
#      and removes any conflicting flags (=0 variants).
#   4. Strips the macros $kernelopts and $tuned_params, so literal flags win.
#   5. Clears kernelopts / saved_entry / tuned_params from grubenv.
#   6. If menu is legacy, remounts the filesystem read-write, regenerates the
#      *real* grub.cfg (auto-discovered path), then remounts read-only again.
#
# USAGE
#   sudo bash enable-cgv2.sh
#   sudo reboot
#
# AFTER REBOOT
#   grep -E 'cgroup_no_v1|unified_cgroup_hierarchy' /proc/cmdline
#   mount | grep 'cgroup2 on /sys/fs/cgroup'
#
#   You should see both flags exactly once and a single cgroup2 mount.
#
# ---------------------------------------------------------------------------
set -euo pipefail

# literal flags we want to appear LAST on the command line
FLAGS="systemd.unified_cgroup_hierarchy=1 cgroup_no_v1=all"
GRUBENV=/boot/grub2/grubenv          # works for BIOS & UEFI (symlinked)
BOOT_MODE=""                         # will be set to BLS or LEGACY
GRUBCFG=""                           # path to grub.cfg when LEGACY
BOOTDIR=""                           # filesystem containing GRUBCFG

die() { echo "fatal: $*" >&2; exit 1; }

# remount helpers (no-op for read-write fs)
rw() { mountpoint -q "$1" && mount -o remount,rw "$1" 2>/dev/null || true; }
ro() { mountpoint -q "$1" && mount -o remount,ro "$1" 2>/dev/null || true; }

[[ $(id -u) -eq 0 ]] || die "run as root (sudo)"

echo "[1/9]  Ensuring 'grubby' is present …"
command -v grubby &>/dev/null || yum -y install grubby

echo "[2/9]  Detecting boot menu style …"
if grep -q '^GRUB_ENABLE_BLSCFG=.*true' /etc/default/grub 2>/dev/null &&
   grep -q blscfg /boot/grub2/grub.cfg 2>/dev/null; then
    BOOT_MODE=BLS
    echo "         → BLS menu (boot/loader/entries)"
else
    BOOT_MODE=LEGACY
    echo "         → Legacy GRUB menu (classic grub.cfg)"
fi

# --------------------------------------------------------------------
# 3. Add literal flags & strip conflicting arguments for ALL kernels
# --------------------------------------------------------------------
echo "[3/9]  Updating kernel stanzas (all installed kernels) …"
grubby --update-kernel=ALL \
       --args="$FLAGS" \
       --remove-args="systemd.unified_cgroup_hierarchy=0 cgroup_no_v1="

# Remove macros ($kernelopts and $tuned_params) so literal flags are last
grubby --update-kernel=ALL \
       --remove-args='\$kernelopts \$tuned_params' \
       --args="$FLAGS"

# --------------------------------------------------------------------
# 4. Clean overrides stored in grubenv
# --------------------------------------------------------------------
echo "[4/9]  Cleaning grubenv variables …"
for var in kernelopts saved_entry tuned_params; do
    if grub2-editenv "$GRUBENV" list | grep -q "^${var}="; then
        rw /boot
        grub2-editenv "$GRUBENV" unset "$var"
        ro /boot
        echo "         unset $var"
    fi
done

# --------------------------------------------------------------------
# 5. Additional cleanup for legacy menus (no blscfg)
# --------------------------------------------------------------------
if [[ $BOOT_MODE == LEGACY ]]; then
    echo "[5/9]  Removing \$tuned_params macros from legacy menu entries …"
    grubby --update-kernel=ALL --remove-args='\$tuned_params'
fi

# --------------------------------------------------------------------
# 6. Find the *real* grub.cfg when boot mode is LEGACY
# --------------------------------------------------------------------
if [[ $BOOT_MODE == LEGACY ]]; then
    echo "[6/9]  Locating active grub.cfg …"
    GRUBCFG=$(find /boot /boot/efi -maxdepth 4 -name grub.cfg -printf '%s %p\n' \
              | sort -nr | awk 'NR==1{print $2}')
    [[ -z $GRUBCFG ]] && die "cannot locate grub.cfg under /boot or /boot/efi"

    BOOTDIR=$(df --output=target "$GRUBCFG" | awk 'NR==2')
    echo "         found $GRUBCFG on $BOOTDIR"

    echo "[7/9]  Regenerating menu …"
    rw "$BOOTDIR"
    grub2-mkconfig -o "$GRUBCFG"
    ro "$BOOTDIR"
else
    echo "[6/9]  BLS menu – grub.cfg regeneration not required"
fi

# --------------------------------------------------------------------
# 8. Sync disks (useful on read-only images)
# --------------------------------------------------------------------
echo "[8/9]  Syncing filesystem buffers …"
sync

echo "[9/9]  ✔ All done – reboot to activate cgroup v2"

cat <<'EOF'

Next steps
==========

1) Reboot:   sudo reboot
2) Verify:

   grep -E 'cgroup_no_v1|unified_cgroup_hierarchy' /proc/cmdline
   mount | grep 'cgroup2 on /sys/fs/cgroup'

   • Both flags must appear exactly once in /proc/cmdline
   • mount should show ONE line: "cgroup2 on /sys/fs/cgroup …"

You are then running a pure cgroup v2 hierarchy and can use
   echo '+io' > /sys/fs/cgroup/cgroup.subtree_control
   echo '259:0 riops=100 wiops=50' > /sys/fs/cgroup/slow/io.max
to throttle NVMe I/O.
EOF
