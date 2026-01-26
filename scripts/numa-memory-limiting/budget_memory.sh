#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# NUMA-aware RAM reservation utility (page-watermark aware)
#
#   budget <GB_left_on_target> [NUMANODE|all]
#       • If NUMANODE = N
#           – leave GB_left_on_target free on node N
#           – on every OTHER node leave
#                 high_watermark_pages(node) × PAGE_SIZE × CUSHION_FACTOR
#             and consume the rest.
#       • If NUMANODE = all (default)
#           – leave GB_left_on_target free on *each* node equally.
#
#   cleanup
#       – unmount / remove every ram-disk created by this script
#
#   EXIT CODES:
#       0 – success
#       1 – failure (bad args or insufficient memory)
# ---------------------------------------------------------------------------

set -euo pipefail

# ---------- tweakable ----------
CUSHION_FACTOR=5           # float multiplier for each node's summed “high” watermark
PAGE_KB=4                    # system page size in KB

# ---------- argument parsing ----------
usage() {
    echo "Usage:"
    echo "  $0 budget <GB_left> [NUMANODE|all]"
    echo "  $0 cleanup"
    exit 1
}

[[ $# -gt 0 ]] || usage
MODE=$1

case $MODE in
    budget)
        [[ $# -ge 2 ]] || { echo "Error: 'budget' needs <GB_left>"; usage; }
        GB_LEFT=$2
        TARGET=${3:-all}
        [[ $GB_LEFT =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "Error: GB must be numeric"; exit 1; }
        ;;
    cleanup)
        [[ $# -eq 1 ]] || { echo "Error: 'cleanup' takes no extra args"; usage; }
        ;;
    *) usage ;;
esac

# ---------- helpers ----------
get_node_cnt() {
    numactl --hardware | awk '/available:/ {print $2}'
}

node_free_mb() {
    numactl --hardware | awk -v n="$1" '$0 ~ ("node "n" free:") {print $4}'
}

node_high_pages() {
    local node=$1
    awk -v n="$node" '
        $1=="Node" && $2==n"," { in_node=1; next }
        $1=="Node" && $2!=n"," { in_node=0 }
        in_node && $1=="high"  { sum+=$2 }
        END { print sum }
    ' /proc/zoneinfo
}

node_cushion_mb() {
    local high_pages=$1
    local pages_needed
    pages_needed=$(echo "scale=4; $high_pages * $CUSHION_FACTOR" | bc)
    pages_needed=$(printf "%.0f" "$(echo "$pages_needed + 0.999" | bc)")  # ceil
    echo $(( (pages_needed * PAGE_KB + 1023) / 1024 ))                    # ceil to MB
}

# ---------- RAM reservation ----------
reserve_on_node() {
    local mb=$1 node=$2
    (( mb <= 0 )) && { echo "Node $node → nothing to reserve"; return; }

    sudo umount /mnt/ext4ramdisk$node 2>/dev/null || true
    sudo rm -f /mnt/ramdisk$node/ext4.image || true
    sudo umount /mnt/ramdisk$node 2>/dev/null || true

    sudo mkdir -p /mnt/ramdisk$node /mnt/ext4ramdisk$node
    sudo numactl --cpunodebind=$node --membind=$node \
         mount -t ramfs ramfs /mnt/ramdisk$node

    sudo numactl --cpunodebind=$node --membind=$node \
         dd if=/dev/zero of=/mnt/ramdisk$node/ext4.image bs=1M count=$mb status=none

    sudo mkfs.ext4 -q -F /mnt/ramdisk$node/ext4.image
    sudo numactl --cpunodebind=$node --membind=$node \
         mount -o loop /mnt/ramdisk$node/ext4.image /mnt/ext4ramdisk$node
    sudo chown -R "$USER" /mnt/ext4ramdisk$node
    echo "● node $node – consumed $mb MB"
}

global_cleanup() {
    local nodes
    nodes=$(get_node_cnt)
    for n in $(seq 0 $((nodes - 1))); do
        sudo umount /mnt/ext4ramdisk$n 2>/dev/null || true
        sudo rm -f /mnt/ramdisk$n/ext4.image || true
        sudo umount /mnt/ramdisk$n 2>/dev/null || true
        echo "✔ cleaned node $n"
    done
    return 0
}

budget_memory() {
    local gb_left=$1 target=$2
    local mb_left_total
    mb_left_total=$(echo "$gb_left * 1024" | bc | awk '{printf "%d",$1}')
    local nodes
    nodes=$(get_node_cnt)

    if [[ $target == "all" ]]; then
        local mb_each=$(( mb_left_total / nodes ))
        for n in $(seq 0 $((nodes - 1))); do
            local free
            free=$(node_free_mb "$n")
            (( free >= mb_each )) || { echo "Error: Node $n has $free MB free, needs $mb_each MB"; exit 1; }
        done
        for n in $(seq 0 $((nodes - 1))); do
            local free
            free=$(node_free_mb "$n")
            reserve_on_node $(( free - mb_each )) "$n"
        done
    else
        [[ $target =~ ^[0-9]+$ ]] || { echo "Error: NUMANODE must be numeric"; exit 1; }
        (( target < nodes ))     || { echo "Error: NUMANODE $target outside 0-$((nodes-1))"; exit 1; }

        local free_target
        free_target=$(node_free_mb "$target")
        (( free_target >= mb_left_total )) \
            || { echo "Error: Node $target has only $free_target MB (needs $mb_left_total MB)"; exit 1; }
        reserve_on_node $(( free_target - mb_left_total )) "$target"

        for n in $(seq 0 $((nodes - 1))); do
            [[ $n == "$target" ]] && continue
            local free high_pages cushion_mb
            free=$(node_free_mb "$n")
            high_pages=$(node_high_pages "$n")
            cushion_mb=$(node_cushion_mb "$high_pages")
            (( free > cushion_mb )) || { echo "Warning: Node $n already ≤ cushion (${cushion_mb} MB)"; continue; }
            reserve_on_node $(( free - cushion_mb )) "$n"
        done
    fi

    return 0
}

# ---------- dispatch ----------
case $MODE in
    budget)  budget_memory "$GB_LEFT" "$TARGET" ;;
    cleanup) global_cleanup ;;
esac

exit 0
