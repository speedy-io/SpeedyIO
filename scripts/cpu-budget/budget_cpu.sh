#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# smt_numa.sh — Keep/offline CPUs by whole physical cores, evenly across NUMA.
#
# Motivation:
#   For benchmarking or power isolation, you may want to reduce the number of
#   *online* logical CPUs in a way that:
#     - never leaves a single hyperthread running alone on a core (whole-core)
#     - keeps an even distribution across NUMA nodes (important on EPYC)
#     - validates inputs and fails fast with clear exit codes
#
# What this script does:
#   - Validates your target "N hyperthreads to keep" (N ≥ 2, multiple of SMT,
#     and multiple of NUMA node count).
#   - Computes how many physical cores to keep per NUMA node:
#       cores_to_keep = N / SMT
#       cores_per_node = cores_to_keep / NUMA_nodes
#   - Enumerates CPU↔CORE↔NODE topology using `lscpu -p`.
#   - Keeps *all threads* of selected cores online; offlines threads of others.
#   - Prints resulting topology (lscpu) for verification.
#
# Exit codes (by convention):
#   0 = success
#   1 = validation/user error (bad input, divisibility rules not met, etc.)
#   2 = system/internal error (missing commands, unreadable sysfs, write fails)
#
# Requirements:
#   - Linux with CPU hotplug via sysfs (/sys/devices/system/cpu/cpu*/online)
#   - bash, awk, coreutils, and `lscpu` available in PATH
#   - root privileges (or sudo permission to write via tee to the sysfs files)
#
# Usage:
#   ./smt_numa.sh set <HYPERTHREADS_TO_KEEP>
#   ./smt_numa.sh restore
#   ./smt_numa.sh status
#
# Notes:
#   - Some platforms do not expose cpu0/online. The script won't force it.
#   - IRQ affinity or pinned RT threads can block offlining. We warn, do not
#     treat as fatal (you can drain IRQs and retry if needed).
#   - Changes are not persistent across reboot. Use a systemd unit to reapply.
# -----------------------------------------------------------------------------

set -euo pipefail

# --------------------
# Messaging & exit helpers
# --------------------

# vdie: Validation/user error — caller passed bad input, violated constraints, etc.
vdie() { echo "VALIDATION ERROR: $*" >&2; exit 1; }

# sdie: System/internal error — missing deps, unreadable sysfs, failed writes, etc.
sdie() { echo "SYS ERROR: $*" >&2; exit 2; }

# note: Non-fatal informational message to stderr
note() { echo "NOTE: $*" >&2; }

# --------------------
# Preflight checks
# --------------------

preflight() {
        # Ensure lscpu is available early; many steps rely on it
        if ! command -v lscpu >/dev/null 2>&1; then
                sdie "Required command 'lscpu' not found in PATH."
        fi

        # Ensure CPU sysfs directory exists
        local cpu_sys="/sys/devices/system/cpu"
        [[ -d "$cpu_sys" ]] || sdie "Missing $cpu_sys (kernel CPU sysfs not available)."

        # At least one cpu directory
        ls "$cpu_sys"/cpu[0-9]* >/dev/null 2>&1 || sdie "No CPU directories under $cpu_sys."

        # Optional: warn if not root and no sudo available (writes may fail)
        if [[ $EUID -ne 0 ]] && ! command -v sudo >/dev/null 2>&1; then
                note "Not running as root and 'sudo' not found; writes to CPU 'online' may fail."
        fi
}

# --------------------
# Low-level helpers
# --------------------

# write_knob: Write a value to a sysfs knob (uses sudo tee if direct write not allowed).
# Fails with exit 2 (system error) if the write itself fails.
write_knob() {
        local val="$1" knob="$2"
        if [[ -w "$knob" ]]; then
                echo "$val" > "$knob" || sdie "Failed to write $val to $knob"
        else
                # shellcheck disable=SC2024
                echo "$val" | sudo tee "$knob" >/dev/null || sdie "Failed to sudo-write $val to $knob"
        fi
}

# get_tpc: Determine threads-per-core (SMT level), prefer lscpu, fallback to sysfs.
get_tpc() {
        local tpc
        tpc=$(lscpu | awk '/^Thread\(s\) per core:/ {print $4}')
        if [[ -z "${tpc:-}" || "$tpc" -lt 1 ]]; then
                # Fallback: count siblings in cpu0 sibling list (comma-separated)
                local sib="/sys/devices/system/cpu/cpu0/topology/thread_siblings_list"
                if [[ -r "$sib" ]]; then
                        tpc=$(awk -F, '{print NF}' "$sib")
                fi
        fi
        [[ -n "${tpc:-}" && "$tpc" -ge 1 ]] || sdie "Could not determine threads-per-core."
        echo "$tpc"
}

# get_nnodes: Determine number of NUMA nodes, prefer lscpu summary, fallback to -p scan.
get_nnodes() {
        local nn
        nn=$(lscpu | awk '/^NUMA node\(s\):/ {print $3}')
        if [[ -z "${nn:-}" || "$nn" -lt 1 ]]; then
                nn=$(lscpu -p=NODE | awk -F, '$0!~/^#/ && $1~/^[0-9]+$/ {print $1}' | sort -nu | wc -l)
        fi
        [[ -n "${nn:-}" && "$nn" -ge 1 ]] || sdie "Could not determine number of NUMA nodes."
        echo "$nn"
}

# list_cpu_core_node: Print topology as "<cpu> <core> <node>" per line.
list_cpu_core_node() {
        lscpu -p=CPU,CORE,NODE \
                | awk -F, '$0!~/^#/ && $1~/^[0-9]+$/ && $2~/^[0-9]+$/ && $3~/^[0-9]+$/ {print $1" "$2" "$3}'
}

# list_all_cores_by_node: Unique "<node> <core>" pairs (sorted), useful to group cores per NUMA node.
list_all_cores_by_node() {
        lscpu -p=CORE,NODE \
                | awk -F, '$0!~/^#/ && $1~/^[0-9]+$/ && $2~/^[0-9]+$/ {print $2" "$1}' \
                | sort -n -k1,1 -k2,2 -u
}

# --------------------
# Core actions
# --------------------

# smt_set: Keep exactly N hyperthreads online, evenly across NUMA nodes, whole-core only.
smt_set() {
        local target="${1:-}"

        # Basic input validation & usage
        [[ -n "$target" && "$target" =~ ^[0-9]+$ ]] || vdie "Provide numeric hyperthreads to keep. Usage: set <N>"
        (( target >= 2 )) || vdie "Hyperthreads must be >= 2."

        # Discover topology (system errors if discovery fails)
        local tpc nnodes total_cpus total_cores keep_threads keep_cores keep_per_node
        tpc=$(get_tpc)
        nnodes=$(get_nnodes)

        # User-facing constraints: N must be multiple of SMT and NUMA nodes
        (( target % tpc == 0 ))    || vdie "Desired hyperthreads ($target) not a multiple of threads-per-core ($tpc)."
        (( target % nnodes == 0 )) || vdie "Desired hyperthreads ($target) must be a multiple of NUMA nodes ($nnodes)."

        # Count logical CPUs using lscpu -p (robust to formatting)
        total_cpus=$(lscpu -p=CPU | awk -F, '$0!~/^#/ {c++} END{print c+0}')
        (( total_cpus > 0 )) || sdie "No CPUs found."
        total_cores=$(( total_cpus / tpc ))

        keep_threads=$target
        keep_cores=$(( keep_threads / tpc ))
        keep_per_node=$(( keep_cores / nnodes ))

        # More user-facing constraints
        (( keep_cores >= 1 )) || vdie "Would result in zero cores kept."
        (( keep_cores <= total_cores )) || vdie "Request implies $keep_cores cores but only $total_cores exist."
        # Internal calculation sanity (should always hold given divisibility checks)
        (( keep_per_node * nnodes == keep_cores )) || sdie "Internal computation error (per-node core count)."

        # Human-friendly summary
        echo "Threads-per-core:     $tpc"
        echo "NUMA nodes:           $nnodes"
        echo "Total logical CPUs:   $total_cpus"
        echo "Total physical cores: $total_cores"
        echo "Keeping:              $keep_cores cores ($keep_threads hyperthreads)"
        echo "Per-NUMA-node cores:  $keep_per_node"
        echo

        # Build per-node core lists (unique, sorted)
        declare -A node_core_list=()  # node -> " c0 c1 c2 ..."
        declare -A seen=()
        while read -r node core; do
                local key="$node:$core"
                [[ -n "${seen[$key]:-}" ]] && continue
                seen[$key]=1
                node_core_list[$node]="${node_core_list[$node]:-} $core"
        done < <(list_all_cores_by_node)

        # Select first N cores per NUMA node (policy: lowest core IDs per node).
        # You can change selection to "last N" or a custom order if desired.
        declare -A keep_core_map=()  # "node:core" -> 1
        for node in "${!node_core_list[@]}"; do
                mapfile -t cores < <(tr ' ' '\n' <<<"${node_core_list[$node]}" | awk 'NF' | sort -n | uniq)
                (( ${#cores[@]} > 0 )) || sdie "No cores found on NUMA node $node."
                (( ${#cores[@]} >= keep_per_node )) || vdie "NUMA node $node has only ${#cores[@]} cores; need $keep_per_node."
                for core_id in "${cores[@]:0:keep_per_node}"; do
                        keep_core_map["$node:$core_id"]=1
                done
        done

        # Convert kept cores → kept logical CPUs (include all hyperthreads of those cores)
        declare -A keep_cpu_map=()   # cpu_id -> 1
        while read -r cpu core node; do
                if [[ -n "${keep_core_map[$node:$core]:-}" ]]; then
                        keep_cpu_map["$cpu"]=1
                fi
        done < <(list_cpu_core_node)

        # Apply online states, warning (not failing) when a CPU won't offline.
        # Reasons include: cpu0 no knob, IRQ affinity, pinned RT tasks, hypervisor policy.
        echo "Applying CPU online states (balanced across NUMA nodes)..."
        local p cpu_id knob
        for p in /sys/devices/system/cpu/cpu[0-9]*; do
                [[ -d "$p" ]] || continue
                cpu_id=${p##*/cpu}
                knob="$p/online"
                if [[ -e "$knob" ]]; then
                        if [[ -n "${keep_cpu_map[$cpu_id]:-}" ]]; then
                                write_knob 1 "$knob"
                        else
                                # Try to offline; if write fails, warn but don't kill the run.
                                if ! ( [[ -w "$knob" ]] && echo 0 > "$knob" ) && ! ( echo 0 | sudo tee "$knob" >/dev/null ); then
                                        note "Could not offline cpu$cpu_id (non-hotpluggable, IRQ pinned, or policy-restricted)."
                                fi
                        fi
                else
                        # Many systems lack cpu0/online; if it's not in keep set, we simply note it.
                        [[ -n "${keep_cpu_map[$cpu_id]:-}" ]] || note "$p has no 'online' control; leaving as-is."
                fi
        done

        echo
        echo "Result (lscpu):"
        lscpu -e=CPU,CORE,NODE,ONLINE
}

# smt_restore: Bring all CPUs online where possible.
smt_restore() {
        echo "Bringing all CPUs online..."
        local any=0 p knob
        for p in /sys/devices/system/cpu/cpu[0-9]*; do
                [[ -d "$p" ]] || continue
                knob="$p/online"
                if [[ -e "$knob" ]]; then
                        if write_knob 1 "$knob"; then any=1; fi
                fi
        done
        if (( any == 0 )); then
                note "No writable 'online' knobs found; CPUs may already be fully online or platform restricts control."
        fi
        echo
        echo "Result (lscpu):"
        lscpu -e=CPU,CORE,NODE,ONLINE
}

# smt_status: Show detected SMT/NUMA and detailed online map, plus thread siblings.
smt_status() {
        echo "Threads-per-core: $(get_tpc)"
        echo "NUMA nodes:       $(get_nnodes)"
        echo
        lscpu -e=CPU,CORE,NODE,ONLINE
        echo
        echo "Per-core hyperthread groups (from sysfs):"
        for f in /sys/devices/system/cpu/cpu*/topology/thread_siblings_list; do
                [[ -r "$f" ]] || continue
                cpu=${f%/topology/thread_siblings_list}; cpu=${cpu##*/cpu}
                echo "cpu${cpu}: $(cat "$f")"
        done | sort -n -k1.4
}

# --------------------
# Main entry
# --------------------
main() {
        preflight  # Ensure environment is sane before doing anything

        local cmd="${1:-}"
        case "$cmd" in
                set)
                        shift
                        [[ $# -eq 1 ]] || vdie "Usage: $0 set <HYPERTHREADS_TO_KEEP>"
                        smt_set "$1"
                        ;;
                restore)
                        smt_restore
                        ;;
                status|"")
                        smt_status
                        [[ -n "$cmd" ]] || { echo; echo "Usage: $0 {set <N>|restore|status}"; }
                        ;;
                *)
                        vdie "Unknown command: $cmd. Usage: $0 {set <N>|restore|status}"
                        ;;
        esac
}

main "$@"

