#!/usr/bin/env bash

#############################
# Check for pure cgroup v2
#############################

check_pure_cgroupv2() {
  # Print parent's shell PID for reference
  echo "DEBUG: [check_pure_cgroupv2] Called in shell PID=$$ (BASHPID=$BASHPID)."

  # 1) Must see 'cgroup2 on /sys/fs/cgroup'
  if ! mount | grep -q '^cgroup2 on /sys/fs/cgroup '; then
    echo "ERROR: cgroup2 is NOT the primary mount on /sys/fs/cgroup."
    echo "       Make sure you have 'systemd.unified_cgroup_hierarchy=1' and 'cgroup_no_v1=all' set, then reboot."
    exit 1
  fi

  # 2) Ensure no cgroup v1 lines
  if mount | grep -q '^cgroup on /sys/fs/cgroup/'; then
    echo "ERROR: Found cgroup v1 mounts. Not a pure cgroup v2 environment."
    exit 1
  fi

  echo "OK: Pure cgroup v2 environment detected. No cgroup v1 mounts."
}


#############################
# Main Script Flow
#############################

echo "DEBUG: [main script] Starting in shell PID=$$ (BASHPID=$BASHPID)."

check_pure_cgroupv2
echo "Continuing script..."

# Example usage of caching / big file read:
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

echo "DEBUG: [main script] Throttled read about to start..."
start=$(date +%s)

# 1) Run the subscript with the command you want throttled
#    Capture the cgroup name from its output
CGROUP_NAME="test_cg"
./throttle_subscript.sh "$CGROUP_NAME" "259:0" "max" "max" "1000" "5" -- /usr/bin/time -v cat testfile > /dev/null


# At this point, 'throttle_subscript.sh' is done, so the cgroup *should* be empty.

echo "DEBUG: [main script] The subscript created cgroup: $CGROUP_NAME"

# 2) Remove that cgroup directory
sudo rmdir "/sys/fs/cgroup/${CGROUP_NAME}" \
  && echo "DEBUG: [main script] Removed cgroup: $CGROUP_NAME" \
  || echo "WARNING: Could not remove cgroup: $CGROUP_NAME"

end=$(date +%s)
elapsed=$(( end - start ))
echo "Command 1 took ${elapsed} seconds."

sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

echo "DEBUG: [main script] Doing normal read..."
start=$(date +%s)
/usr/bin/time -v cat testfile > /dev/null
end=$(date +%s)
elapsed=$(( end - start ))
echo "Command 2 took ${elapsed} seconds."

sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

echo "DEBUG: [main script] All done."
