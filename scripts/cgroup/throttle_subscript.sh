#!/usr/bin/env bash
#
# throttle_subscript.sh
# Usage:
#   throttle_subscript.sh <CGROUP_NAME> <DEVICE> <RBPS> <WBPS> <RIOPS> <WIOPS> -- <command...>
#
# Example:
#   throttle_subscript.sh "test_cg" "259:0" "max" "max" "10" "5" -- /usr/bin/time -v dd if=/path/to/file of=/dev/null bs=1M
#
# This script:
#   1) Creates/enters a cgroup named CGROUP_NAME
#   2) Sets io.max for <DEVICE> to <RBPS>, <WBPS>, <RIOPS>, <WIOPS>
#   3) Runs <command...>
#   4) Prints the cgroup name at the end
#   5) Does NOT remove the cgroup so that the caller (main script) can remove it.

set -e

if [ "$#" -lt 7 ]; then
  echo "Usage: $0 <CGROUP_NAME> <DEVICE> <RBPS> <WBPS> <RIOPS> <WIOPS> -- <command...>"
  exit 1
fi

CGROUP_NAME="$1"
DEVICE="$2"
RBPS="$3"
WBPS="$4"
RIOPS="$5"
WIOPS="$6"
shift 6

# Now the next argument should be "--"
if [ "$1" != "--" ]; then
  echo "Usage error: missing '--' before command." >&2
  exit 1
fi
shift 1  # remove the "--" from $@

# The rest of $@ is the command we want to run
CMD=( "$@" )

#################################################################
# 1) Create the cgroup
#################################################################

CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"

# Enable the io controller if not already
echo "+io" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null 2>&1 || true

# Create directory for the cgroup
sudo mkdir -p "${CGROUP_PATH}"

#################################################################
# 2) Set the io.max for that device
#################################################################

# Example: "259:0 rbps=100 wbps=100 riops=10 wiops=5"
echo "${DEVICE} rbps=${RBPS} wbps=${WBPS} riops=${RIOPS} wiops=${WIOPS}" \
  | sudo tee "${CGROUP_PATH}/io.max"

#################################################################
# 3) Move THIS script (PID) into that cgroup
#################################################################

echo "$$" | sudo tee "${CGROUP_PATH}/cgroup.procs"

echo "DEBUG: [throttle_subscript.sh] cgroup='${CGROUP_NAME}', device='${DEVICE}'," \
     "io.max='rbps=${RBPS} wbps=${WBPS} riops=${RIOPS} wiops=${WIOPS}'"
echo "DEBUG: [throttle_subscript.sh] Running command as PID=$$ => ${CMD[*]}"

#################################################################
# 4) Run the user's command
#################################################################
# The command's output will appear on this script's stdout/stderr
# unless you or the main script redirect it.
"${CMD[@]}"

#################################################################
# 5) Print cgroup name for the main script to see
#################################################################
# The main script can capture this line if needed. It's the last line
# on stdout (assuming no more prints below).
echo "${CGROUP_NAME}"

# Done. We do NOT remove the cgroup. The main script can do:
#   sudo rmdir "/sys/fs/cgroup/${CGROUP_NAME}"
# once the script has exited (i.e., no process remains in the cgroup).
