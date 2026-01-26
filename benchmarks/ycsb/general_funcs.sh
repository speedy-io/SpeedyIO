#!/bin/bash

source ./scripts/cassandra/update_cassandra_config_files.sh

if [ -z "$BASE" ]; then
        echo "SpeedyIO environment variables are undefined."
        echo "Did you setvars? goto Base directory (/path/to/speedyio_release) and $ source ./scripts/setvars.sh"
        exit 1
fi

# shared lib code folder -- this is where the .so files are stored
SHARED_LIB_FOLDER="$BASE/lib"

REMOTE_MOUNT_DIR="/mnt/speedyio_benchmark_logs"
REMOTE_MOUNT_DIR_FOR_EXTRA_REQUESTER="/mnt/speedyio_logs_requester__"  # This will be suffixed with a timestamp because we need unique directories in case multiple benchmark scripts using the extra requester nodes (to avoid all scripts trying to modify and write into the same mountpoint)
REMOTE_SCRIPTS_DIR="$REMOTE_MOUNT_DIR/utils"

# These are paths in fast_mongodb repo. Will not work with standalone ucsb
MEMINFO_LOGGER_PATH="$BASE/scripts/monitoring/meminfo/meminfo_logger.sh"
VMSTAT_LOGGER_PATH="$BASE/scripts/monitoring/vmstat/vmstat_logger.sh"
VMSTAT_COMMAND_LOGGER_PATH="$BASE/scripts/monitoring/vmstat/vmstat_command_logger.sh"
BUDGET_MEMORY_PATH="$BASE/scripts/numa-memory-limiting/budget_memory.sh"
BUDGET_CPU_PATH="$BASE/scripts/cpu-budget/budget_cpu.sh"
DROP_CASSANDRA_KEYSPACE_SCRIPT_PATH="./scripts/cassandra/drop_keyspace_cassandra.sh"
THROTTLE_SUBSCRIPT_PATH="$BASE/scripts/cgroup/throttle_subscript.sh"
CASSANDRA_COMPACTIONSTATS_LOGGER_PATH="$BASE/scripts/monitoring/cassandra/compactionstats_logger.sh"
CASSANDRA_TABLESTATS_LOGGER_PATH="$BASE/scripts/monitoring/cassandra/tablestats_logger.sh"
CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH="$BASE/scripts/monitoring/cassandra/tablehistograms_logger.sh"
FILE_VALUES_LOGGER_PATH="$BASE/scripts/monitoring/file_values/values_logger.sh"

MEMINFO_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/meminfo_logger.sh"
VMSTAT_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/vmstat_logger.sh"
VMSTAT_COMMAND_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/vmstat_command_logger.sh"
BUDGET_MEMORY_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/budget_memory.sh"
BUDGET_CPU_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/budget_cpu.sh"
DROP_CASSANDRA_KEYSPACE_SCRIPT_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/drop_keyspace_cassandra.sh"
THROTTLE_SUBSCRIPT_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/throttle_subscript.sh"
CASSANDRA_COMPACTIONSTATS_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/compactionstats_logger.sh"
CASSANDRA_TABLESTATS_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/tablestats_logger.sh"
CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/tablehistograms_logger.sh"
FILE_VALUES_LOGGER_PATH_REMOTE="$REMOTE_SCRIPTS_DIR/values_logger.sh"

ANSIBLE_SUPPRESS_OUTPUT=false  # Suppress output from Ansible commands, set to false for debugging
ANSIBLE_CONDENSE_OUTPUT=false  # Condense output from Ansible commands, set to false for debugging

check_ulimit() {
        local is_remote="$1"  # Optional argument to indicate if this is a remote operation

        local required_ulimit=10000
        if [ "$is_remote" = "true" ]; then
            run_on_nodes "
            required_ulimit=$required_ulimit
            current_limit=\$(ulimit -n)
            if [ \"\$current_limit\" -lt \"\$required_ulimit\" ]; then
                echo \"The current ulimit is less than \$required_ulimit: \$current_limit\"
                echo \"Please edit ulimit -n before rerunning this script\"
                exit 1
            else
                echo \"The current ulimit is \$current_limit, which is sufficient.\"
            fi
            " || {
                echo "❌ ulimit check failed on one or more nodes." >&2
                exit 1
            }
        else
            current_limit=$(ulimit -n)
            if [ "$current_limit" -lt "$required_ulimit" ]; then
                echo "The current ulimit is less than $required_ulimit: $current_limit"
                echo "Please edit ulimit -n before rerunning this script"
                exit 1
            else
                echo "The current ulimit is $current_limit, which is sufficient."
            fi
        fi
}

TurnSwapOff() {
    local is_remote="$1"
    echo "Turning swap off..."
    if [ "$is_remote" = "true" ]; then
        run_on_nodes 'sudo swapoff -av || echo "Swap already off or failed"'
    else
        sudo swapoff -av || echo "Swap already off or failed"
    fi
}

RestoreSwap() {
    local is_remote="$1"
    echo "Restoring swap..."
    if [ "$is_remote" = "true" ]; then
        run_on_nodes 'sudo swapon -av || echo "Failed to restore swap"'
    else
        sudo swapon -av || echo "Failed to restore swap"
    fi
}


#FlushDisk() {
#        local is_remote="$1"  # Optional argument to indicate if this is a remote operation
#        echo ""
#        echo "sync and drop cache for unpolluted memory. Might require sudo password"
#
#        if [ "$is_remote" = "true" ]; then
#            run_on_nodes 'sudo sh -c "echo 3 > /proc/sys/vm/drop_caches && sync && echo 3 > /proc/sys/vm/drop_caches"'
#        else
#            sudo sh -c "echo 3 > /proc/sys/vm/drop_caches && sync && echo 3 > /proc/sys/vm/drop_caches"
#        fi
#        sleep 0.5
#}


FlushDisk() {
    local is_remote="$1"
    echo ""
    echo "sync and drop cache for unpolluted memory. Might require sudo password"

    if [ "$is_remote" = "true" ]; then
        run_on_nodes 'bash -c '"'"'
            if awk "NR>1 {count++} END {exit !(count > 0)}" /proc/swaps; then
                SWAP_WAS_ENABLED=true
                echo "Swap was enabled. Turning off..."
                sudo swapoff -av
            else
                SWAP_WAS_ENABLED=false
                echo "Swap already disabled."
            fi

            echo "Dropping caches..."
            sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches; sync; echo 3 > /proc/sys/vm/drop_caches"
            sleep 0.5

            if [ "$SWAP_WAS_ENABLED" = true ]; then
                echo "Restoring swap..."
                sudo swapon -av
            fi
        '"'"
    else
        local SWAP_WAS_ENABLED
        if awk 'NR>1 {count++} END {exit !(count > 0)}' /proc/swaps; then
            SWAP_WAS_ENABLED=true
            echo "Swap was enabled. Turning off..."
            sudo swapoff -av
        else
            SWAP_WAS_ENABLED=false
            echo "Swap already disabled."
        fi

        echo "Dropping caches..."
        sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches; sync; echo 3 > /proc/sys/vm/drop_caches"
        sleep 0.5

        if [ "$SWAP_WAS_ENABLED" = true ]; then
            echo "Restoring swap..."
            sudo swapon -av
        fi
    fi
}


CHECK_LDD() {
        speedyio_file=$1

        echo "Starting dependency check for $speedyio_file"

        if [ ! -f "$speedyio_file" ]; then
                echo "${FUNCNAME[0]} could not find $speedyio_file"
        fi

        # Capture missing libraries
        missing_libs=`ldd "$speedyio_file" 2>&1 | grep "not found" || true`

        # Print missing libraries if any, or a success message if none are missing
        if [ -n "$missing_libs" ]; then
                echo "The following libraries are missing for $speedyio_file:"
                echo "$missing_libs" | awk '{print $1}'
                echo "If you don't know how to fix this, contact us."
                exit 1
        else
                echo "All required libraries are found for $speedyio_file."
        fi
}

budget_memory() {
    local mem_budget_gb="$1"
    local numanode="$2"
    local is_remote="$3"

    check_command_installed "$is_remote" "numactl"

    echo "Running budget memory via budget script..."
    # ---------------- Memory Budgeting ----------------
    if [ "$mem_budget_gb" -gt "0" ]; then
        if [ "$is_remote" = "true" ]; then
            run_on_nodes "$BUDGET_MEMORY_PATH_REMOTE budget $mem_budget_gb $numanode && free -h && numactl --hardware" || {
                echo "Error: Failed to run memory budget on remote nodes." >&2
                return 1
            }
        else
            "$BUDGET_MEMORY_PATH" budget "$mem_budget_gb" "$numanode"
            free -h
            numactl --hardware
        fi
    fi
    echo "DONE reducing mem"
    # ---------------------------------------------------
}

memory_cleanup() {
    local is_remote="$1"

    check_command_installed "$is_remote" "numactl"

    echo "Running memory cleanup remote via budget script..."
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "$BUDGET_MEMORY_PATH_REMOTE cleanup" || {
            echo "Error: Failed to run memory cleanup on remote nodes." >&2
            return 1
        }
    else
        "$BUDGET_MEMORY_PATH" cleanup
    fi
}


budget_cpu() {
    local nr_cpu="$1"
    local is_remote="$2"

    echo "CHECK IF THIS IS COMPLETE.. exiting"
    exit 1

    check_command_installed "$is_remote" "lscpu"

    echo "Running budget CPUs..."

    # ---------------- CPU Budgeting ----------------
    if [ "$nr_cpu" -gt "0" ]; then 
        if [ "$is_remote" = "true" ]; then 
            run_on_nodes "$BUDGET_CPU_PATH_REMOTE set $nr_cpu" || { 
                echo "Error: Failed to run CPU budget on remote nodes." >&2
                return 1
            }    
        else 
            "$BUDGET_CPU_PATH" set "$nr_cpu"
        fi   
    fi   
}


restore_cpu() {
    local is_remote="$1"

    echo "CHECK IF THIS IS COMPLETE.. exiting.. Also make sure to call this cleanup function in the 'clenaup' sections of the load and run scripts"
    exit 1

    check_command_installed "$is_remote" "lscpu"

    echo "Restoring CPUs..."
    if [ "$is_remote" = "true" ]; then 
        run_on_nodes "$BUDGET_CPU_PATH_REMOTE restore" || { 
            echo "Error: Failed to restore CPUs on remote nodes." >&2
            return 1
        }    
    else 
        "$BUDGET_CPU_PATH" restore
    fi   
}


stop_logging() {
    local is_remote="$1"  # Optional argument is_remote

    if [ "$is_remote" = "true" ]; then
        echo "Stopping remote logging..."
        # ----------------------------------------------------------------------
        # Why these weird patterns? (the “bracket trick”)
        #
        # Problem: `pkill -f PATTERN` matches the FULL command line of EVERY
        # process. When invoked via a shell (Ansible runs `/bin/sh -c '…'`),
        # the shell’s own argv contains our pattern string. `pkill -f` can end
        # up killing the very shell that’s executing this line → task dies with
        # rc=-15 (SIGTERM).
        #
        # Fix: Make the pattern match the real target processes but NOT the
        # wrapper shell’s argv by using a character class for the first char:
        #   - Use "[p]ython3 …" instead of "python3 …"
        #   - Use "[/]{tmp path}…" instead of "/tmp/…"
        #
        # On target processes, "[p]ython3" == "python3" (regex class [p] is p).
        # In the wrapper shell’s argv, the literal text is "[p]ython3", which
        # does NOT match the regex (first char is '['), so we don’t self-kill.
        #
        # Rules when using this:
        #   • Keep the pattern QUOTED and essentially LITERAL except for the
        #     first bracketed character (the trick). Example:
        #         pkill -f -- "[p]ython3 /usr/share/bcc/tools/cachestat"
        #         pkill -f -- "[/]tmp/vmstat_logger.sh"
        #   • Do NOT “get fancy” with regex here (no `.*`, alternation `|`,
        #     groups, etc.). If you rewrite it into something that can also
        #     match the shell’s argv (e.g., `p.*on3`), you can reintroduce
        #     self-matches and kill the shell again.
        #   • Always put `--` before the pattern so a leading `-` in a path
        #     isn’t parsed as an option by pkill.
        #   • Append `|| true` because pkill returns 1 when nothing matched.
        #     That’s not an error for “best-effort stop” logic.
        #
        # Alternatives (also safe):
        #   • Exact full-cmdline match: `pkill -f -x -- "<exact cmdline>"`
        #   • PID-based: `pgrep -af -- '…' | awk '{print $1}' | xargs -r kill`
        #
        # If you truly need complex regex matching, do it with `pgrep` first
        # and then kill by PID. Don’t feed complex regex directly to `pkill`
        # in this block, or you risk self-matching again.
        # ----------------------------------------------------------------------
        # Using the /tmp paths here because the scripts are copied to /tmp on remote nodes before execution (see start_logging function)
        local pkill_cmds=(  # Use || true as pgrep -f might fail if no process is found
            "sudo pkill -f -- '[p]ython3 /usr/share/bcc/tools/cachestat' || true"
            "sudo pkill -f -- '[p]ython3 /usr/share/bcc/tools/trace' || true"
            "sudo pkill -f -- '[/]tmp/meminfo_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/vmstat_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/vmstat_command_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/compactionstats_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/tablestats_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/tablehistograms_logger.sh' || true"
            "sudo pkill -f -- '[/]tmp/values_logger.sh' || true"
        )
        local pkill_cmd_remote
        pkill_cmd_remote=$(printf "%s; " "${pkill_cmds[@]}")
        run_on_nodes "$pkill_cmd_remote"
    fi

    # Stop local logging processes irrespective of is_remote
    echo "Stopping local logging..."
    local pkill_cmds_local=(  # Use || true as pgrep -f might fail if no process is found
        "sudo pkill -f 'python3 /usr/share/bcc/tools/cachestat' || true"
        "sudo pkill -f 'python3 /usr/share/bcc/tools/trace' || true"
        "sudo pkill -f '$MEMINFO_LOGGER_PATH' || true"
        "sudo pkill -f '$VMSTAT_LOGGER_PATH' || true"
        "sudo pkill -f '$VMSTAT_COMMAND_LOGGER_PATH' || true"
        "sudo pkill -f '$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH' || true"
        "sudo pkill -f '$CASSANDRA_TABLESTATS_LOGGER_PATH' || true"
        "sudo pkill -f '$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH' || true"
        "sudo pkill -f '$FILE_VALUES_LOGGER_PATH' || true"
    )
    # Execute the local pkill commands in a loop
    for cmd in "${pkill_cmds_local[@]}"; do
        eval "$cmd"
    done
}

start_logging() {
    local logging_output_dir="$1"
    local suffix="$2"
    local is_remote="$3"

    # Validate inputs
    if [ -z "$logging_output_dir" ] || [ -z "$suffix" ]; then
        echo "Error: Missing arguments to start_logging(logging_output_dir, suffix)" >&2
        return 1
    fi

    # Ensure the directory exists
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -d \"$logging_output_dir\" ]" || {
            echo "Error: start_logging: config directory '$logging_output_dir' does not exist on one or more nodes." >&2
            return 1
        }
    else
        if [ ! -d "$logging_output_dir" ]; then
            echo "Error: start_logging: config directory '$logging_output_dir' does not exist." >&2
            return 1
        fi
    fi

    # Check if required tool paths are available
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -x /usr/share/bcc/tools/cachestat ]" || {
            echo "Error: start_logging: cachestat tool not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x /usr/share/bcc/tools/trace ]" || {
            echo "Error: start_logging: trace tool not found or not executable on one or more nodes." >&2
            return 1
        }
    else
        if [ ! -x "/usr/share/bcc/tools/cachestat" ]; then
            echo "Error: start_logging: cachestat tool not found or not executable." >&2
            return 1
        fi
        if [ ! -x "/usr/share/bcc/tools/trace" ]; then
            echo "Error: start_logging: trace tool not found or not executable." >&2
            return 1
        fi
    fi
    
    # Check if meminfo and vmstat logger scripts are executable
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -x \"$MEMINFO_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: MEMINFO_LOGGER_PATH '$MEMINFO_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x \"$VMSTAT_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: VMSTAT_LOGGER_PATH '$VMSTAT_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x \"$VMSTAT_COMMAND_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: VMSTAT_COMMAND_LOGGER_PATH '$VMSTAT_COMMAND_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x \"$FILE_VALUES_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: '$FILE_VALUES_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
    else
        if [ ! -x "$MEMINFO_LOGGER_PATH" ]; then
            echo "Error: start_logging: MEMINFO_LOGGER_PATH '$MEMINFO_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
        if [ ! -x "$VMSTAT_LOGGER_PATH" ]; then
            echo "Error: start_logging: VMSTAT_LOGGER_PATH '$VMSTAT_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
        if [ ! -x "$VMSTAT_COMMAND_LOGGER_PATH" ]; then
            echo "Error: start_logging: VMSTAT_COMMAND_LOGGER_PATH '$VMSTAT_COMMAND_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
        if [ ! -x "$FILE_VALUES_LOGGER_PATH" ]; then
            echo "Error: start_logging: '$FILE_VALUES_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
    fi

    # Check nodetool logger scripts are executable
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -x \"$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: '$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x \"$CASSANDRA_TABLESTATS_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: '$CASSANDRA_TABLESTATS_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
        run_on_nodes "[ -x \"$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH_REMOTE\" ]" || {
            echo "Error: start_logging: '$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH_REMOTE' not found or not executable on one or more nodes." >&2
            return 1
        }
    else
        if [ ! -x "$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH" ]; then
            echo "Error: start_logging: '$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
        if [ ! -x "$CASSANDRA_TABLESTATS_LOGGER_PATH" ]; then
            echo "Error: start_logging: '$CASSANDRA_TABLESTATS_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
        if [ ! -x "$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH" ]; then
            echo "Error: start_logging: '$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH' not found or not executable." >&2
            return 1
        fi
    fi

    # Check if unbuffer command is available
    # unbuffer is part of the expect package, which is often used for logging
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "command -v unbuffer >/dev/null 2>&1" || {
            echo "Error: 'unbuffer' command not found on one or more nodes. Please install 'expect' package." >&2
            return 1
        }
    else
        if ! command -v unbuffer >/dev/null 2>&1; then
            echo "Error: 'unbuffer' command not found. Please install 'expect' package." >&2
            return 1
        fi
    fi

    # Start logging
    echo "Starting logging..."
    if [ "$is_remote" = "true" ]; then
        # Copy scripts to /tmp on each node before running. This is because you cannot execute scripts directly from the
        # remote mount folder when it’s mounted over SSHFS.
        run_on_nodes "cp \"$MEMINFO_LOGGER_PATH_REMOTE\" /tmp/meminfo_logger.sh && chmod +x /tmp/meminfo_logger.sh"
        run_on_nodes "cp \"$VMSTAT_LOGGER_PATH_REMOTE\" /tmp/vmstat_logger.sh && chmod +x /tmp/vmstat_logger.sh"
        run_on_nodes "cp \"$VMSTAT_COMMAND_LOGGER_PATH_REMOTE\" /tmp/vmstat_command_logger.sh && chmod +x /tmp/vmstat_command_logger.sh"
        run_on_nodes "cp \"$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH_REMOTE\" /tmp/compactionstats_logger.sh && chmod +x /tmp/compactionstats_logger.sh"
        run_on_nodes "cp \"$CASSANDRA_TABLESTATS_LOGGER_PATH_REMOTE\" /tmp/tablestats_logger.sh && chmod +x /tmp/tablestats_logger.sh"
        run_on_nodes "cp \"$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH_REMOTE\" /tmp/tablehistograms_logger.sh && chmod +x /tmp/tablehistograms_logger.sh"
        run_on_nodes "cp \"$FILE_VALUES_LOGGER_PATH_REMOTE\" /tmp/values_logger.sh && chmod +x /tmp/values_logger.sh"
        # Start logging using the /tmp copies
        run_on_nodes "nohup sudo unbuffer /usr/share/bcc/tools/cachestat --timestamp 1 > \"$logging_output_dir/cachestat${suffix}.txt\" 2>&1 &"
        run_on_nodes "nohup sudo unbuffer /usr/share/bcc/tools/trace -t 't:vmscan:mm_vmscan_kswapd_wake \"nid=%d,order=%d\", args->nid, args->order' > \"$logging_output_dir/kswapd_wake${suffix}.txt\" 2>&1 &"
        run_on_nodes "nohup sudo unbuffer bash /tmp/meminfo_logger.sh > \"$logging_output_dir/meminfo${suffix}.csv\" 2>&1 &"
        run_on_nodes "nohup sudo unbuffer bash /tmp/vmstat_logger.sh > \"$logging_output_dir/vmstat_proc${suffix}.csv\" 2>&1 &"
        run_on_nodes "nohup sudo unbuffer bash /tmp/vmstat_command_logger.sh > \"$logging_output_dir/vmstat_command${suffix}.csv\" 2>&1 &"
        run_on_nodes "nohup sudo env CASSANDRA_HOME_DIR=\"$CASSANDRA_HOME_DIR\" unbuffer bash /tmp/compactionstats_logger.sh > \"$logging_output_dir/cassandra_compactionstats${suffix}.log\" 2>&1 &"
        run_on_nodes "nohup sudo env CASSANDRA_HOME_DIR=\"$CASSANDRA_HOME_DIR\" unbuffer bash /tmp/tablestats_logger.sh > \"$logging_output_dir/cassandra_tablestats${suffix}.log\" 2>&1 &"
        run_on_nodes "nohup sudo env CASSANDRA_HOME_DIR=\"$CASSANDRA_HOME_DIR\" unbuffer bash /tmp/tablehistograms_logger.sh > \"$logging_output_dir/cassandra_tablehistograms${suffix}.log\" 2>&1 &"
        run_on_nodes "nohup sudo unbuffer bash /tmp/values_logger.sh > \"$logging_output_dir/file_values${suffix}.csv\" 2>&1 &"
    else
        sudo unbuffer /usr/share/bcc/tools/cachestat --timestamp 1 > "${logging_output_dir}/cachestat${suffix}.txt" 2>&1 &
        sudo unbuffer /usr/share/bcc/tools/trace --timestamp 't:vmscan:mm_vmscan_kswapd_wake "nid=%d,order=%d", args->nid, args->order' > "${logging_output_dir}/kswapd_wake${suffix}.txt" 2>&1 &
        sudo unbuffer bash "$MEMINFO_LOGGER_PATH" > "${logging_output_dir}/meminfo${suffix}.csv" 2>&1 &
        sudo unbuffer bash "$VMSTAT_LOGGER_PATH" > "${logging_output_dir}/vmstat_proc${suffix}.csv" 2>&1 &
        sudo unbuffer bash "$VMSTAT_COMMAND_LOGGER_PATH" > "${logging_output_dir}/vmstat_command${suffix}.csv" 2>&1 &
        sudo env CASSANDRA_HOME_DIR="$CASSANDRA_HOME_DIR" unbuffer bash "$COMPACTIONSTATS_LOGGER_PATH" > "${logging_output_dir}/cassandra_compactionstats${suffix}.log" 2>&1 &
        sudo env CASSANDRA_HOME_DIR="$CASSANDRA_HOME_DIR" unbuffer bash "$TABLESTATS_LOGGER_PATH" > "${logging_output_dir}/cassandra_tablestats${suffix}.log" 2>&1 &
        sudo env CASSANDRA_HOME_DIR="$CASSANDRA_HOME_DIR" unbuffer bash "$TABLEHISTOGRAMS_LOGGER_PATH" > "${logging_output_dir}/cassandra_tablehistograms${suffix}.log" 2>&1 &
        sudo unbuffer bash "$FILE_VALUES_LOGGER_PATH" > "${logging_output_dir}/file_values${suffix}.csv" 2>&1 &
    fi
    sleep 2
}

# -----------------------------------------------------------------------------
# Function: is_only_read_workloads_ucsb
#
# Description:
#   Echoes "true" if all elements in workload_arr are "Read", else "false".
#   This allows capturing the result in a variable: result=$(is_only_read_workloads_ucsb)
# -----------------------------------------------------------------------------
is_only_read_workloads_ucsb() {
    for wl in "${workload_arr[@]}"; do
        if [[ "$wl" != "Read" ]]; then
            echo "false"
            return 0
        fi
    done
    echo "true"
    return 0
}


# Checks if all provided workloads are read-only for cassandra-stress.
# Accepts a list of workload strings as arguments.
# Each workload string may contain comma-separated key-value pairs (e.g., "read=1,write=1").
# Returns "true" if all workloads only contain "read" operations, otherwise "false".
# This won't work for specifications where other operations are specified but their ratio
# is set to zero, e.g., "read=1,write=0". In such cases, it will return "false".
is_only_read_workloads_cassandra_stress() {
    local arr=("$@") # Store all arguments in an array
    for workload in "${arr[@]}"; do
        # Remove spaces for easier matching
        local w="${workload// /}" # Remove all spaces from the workload string
        IFS=',' read -ra parts <<< "$w" # Split workload string by commas into parts
        for part in "${parts[@]}"; do
            IFS='=' read -ra kv <<< "$part" # Split each part by '=' into key-value
            if [[ "${kv[0]}" != "read" ]]; then # If the key is not "read"
                echo "false" # Not a read-only workload
                return 0
            fi
        done
    done
    echo "true" # All workloads are read-only
    return 0
}


###############################################
# Function: create_modified_cassandra_launcher
#
# Description:
#   Duplicates the original `bin/cassandra` script and inserts
#   LD_PRELOAD logic (for a shared library) before Cassandra launches.
#
# Arguments:
#   $1 - config_name (e.g., "et_pvt_lru")
#   $2 - absolute path to .so file (e.g., "/home/user/lib_speedyio_et_pvt_lru.so")
#
# Output:
#   Creates: bin/cassandra_<config_name>
###############################################
create_modified_cassandra_launcher() {
    local config_name="$1"
    local so_path="$2"
    local is_remote="$3"  # Optional argument to indicate if this is a remote operation
    local original_script="$CASSANDRA_HOME_DIR/bin/cassandra"
    local new_script="$CASSANDRA_HOME_DIR/bin/cassandra_${config_name}"
    local insertion_point='# Start up the service'

    # Check if original_script and so_path are provided
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -f \"$original_script\" ]" || {
            echo "❌ ERROR: Original Cassandra script not found at: $original_script" >&2
            return 1
        }
    else
        if [[ ! -f "$original_script" ]]; then
            echo "❌ ERROR: Original Cassandra script not found at: $original_script"
            return 1
        fi
    fi
    
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -f \"$so_path\" ]" || {
            echo "❌ ERROR: Shared library not found at: $so_path" >&2
            return 1
        }
    else
        if [[ ! -f "$so_path" ]]; then
            echo "❌ ERROR: Shared library not found at: $so_path"
            return 1
        fi
    fi

    # Check if the insertion point exists in the original script
    if [ "$is_remote" = "true" ]; then
        if ! run_on_nodes "grep -Fq \"$insertion_point\" \"$original_script\"" ; then
            echo "❌ ERROR: Insertion point not found in $original_script"
            echo "Expected line containing: $insertion_point"
            return 1
        fi
    else
        if ! grep -Fq "$insertion_point" "$original_script"; then
            echo "❌ ERROR: Insertion point not found in $original_script"
            echo "Expected line containing: $insertion_point"
            return 1
        fi
    fi
    # Create a new script by copying the original
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "cp \"$original_script\" \"$new_script\""
    else
        cp "$original_script" "$new_script"
    fi
    
    local preload_snippet=$(cat <<EOF
# Add speedyio library to LD_PRELOAD, preserving any existing settings.
SPEEDYIO_LDPRELOAD_PATH="$so_path"
if [ -z "\$LD_PRELOAD" ]; then
    export LD_PRELOAD="\$SPEEDYIO_LDPRELOAD_PATH"
else
    export LD_PRELOAD="\$LD_PRELOAD:\$SPEEDYIO_LDPRELOAD_PATH"
fi
echo "Using LD_PRELOAD in cassandra: \$LD_PRELOAD"

EOF
)

    # Insert snippet before the insertion point
    if [ "$is_remote" = "true" ]; then
        # 1. Base64-encode the preload snippet locally
        snippet_b64=$(printf '%s\n' "$preload_snippet" | base64 -w0)

        run_on_nodes "
            tmpfile=\$(mktemp) &&
            echo '$snippet_b64' | base64 -d > \"\$tmpfile\" &&

            awk -v f=\"\$tmpfile\" -v marker='$insertion_point' '
                BEGIN { while ((getline < f) > 0) snippet = snippet \$0 ORS; close(f) }
                \$0 ~ marker { printf \"%s\", snippet }
                { print }
            ' '$new_script' > '${new_script}.tmp' &&

            mv '${new_script}.tmp' '$new_script' &&
            chmod +x '$new_script' &&
            rm -f \"\$tmpfile\"
        "
    else
        awk -v snippet="$preload_snippet" -v marker="$insertion_point" '
        {
            if ($0 ~ marker) {
                print snippet
            }
            print
        }
        ' "$new_script" > "${new_script}.tmp" && mv "${new_script}.tmp" "$new_script" && chmod +x "$new_script"
    fi

    echo "✅ Created modified Cassandra launcher: $new_script"
}

# Returns the correct cassandra binary path for a given config
get_cassandra_bin() {
    local cfg="$1"

    # Validate input
    if [ -z "$cfg" ]; then
        echo "Error: Missing argument to get_cassandra_bin(cfg)" >&2
        return 1
    fi

    # Check if CASSANDRA_HOME_DIR is set
    if [ -z "$CASSANDRA_HOME_DIR" ]; then
        echo "Error: CASSANDRA_HOME_DIR environment variable is not set." >&2
        return 1
    fi

    local cassandra_bin=""
    if [[ "$cfg" == "vanilla" ]]; then
        cassandra_bin="$CASSANDRA_HOME_DIR/bin/cassandra"
    else
        cassandra_bin="$CASSANDRA_HOME_DIR/bin/cassandra_${cfg}"
    fi

    # Check if the binary exists
    if [ ! -x "$cassandra_bin" ]; then
        echo "Error: Cassandra binary '$cassandra_bin' not found or not executable." >&2
        return 1
    fi

    # Check cassandra version
    local version
    version=$("$cassandra_bin" -v 2>/dev/null)
    if [[ ! $version =~ ^3\. ]]; then
        echo "ERROR: Cassandra 3.x required. Found version: $version. Support for other versions is not implemented in this script. They have different formats for config file." >&2
        return 1
    fi

    echo "$cassandra_bin"
}

# Function to execute the drop keyspace script which takes the cassandra data folder as an argument
drop_keyspace_cassandra() {
    local cassandra_data_folder="$1"

    # Validate input
    if [ -z "$cassandra_data_folder" ]; then
        echo "Error: Missing argument to drop_keyspace(cassandra_data_folder)" >&2
        return 1
    fi

    # Check if the script exists
    if [ ! -f "$DROP_CASSANDRA_KEYSPACE_SCRIPT_PATH" ]; then
        echo "Error: Drop keyspace script '$DROP_CASSANDRA_KEYSPACE_SCRIPT_PATH' not found." >&2
        return 1
    fi

    # Execute the script
    bash "$DROP_CASSANDRA_KEYSPACE_SCRIPT_PATH" "$cassandra_data_folder"
}

wait_for_cassandra() {
    local timeout=90  # seconds
    local interval=1
    local elapsed=0

    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    echo "⏳ Waiting for Cassandra to start..."

    while (( elapsed < timeout )); do
        if $nodetool_bin status 2>/dev/null | grep -qE '^\s*UN\s+'; then
            echo "✅ Cassandra is up and running."
            return 0
        else
            echo "⏳ Waiting for Cassandra to start... ($elapsed seconds elapsed)"
        fi
        sleep "$interval"
        ((elapsed+=interval))
    done

    echo "❌ Cassandra did not start within $timeout seconds."
    return 1
}

invalidate_caches_cassandra() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation

    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    echo "Invalidating cassandra key caches and row caches ..."
    # Have " || true " condition here as this command sometimes fails and stops the benchmark script, and
    # this command is anyways not super important
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "time $nodetool_bin invalidatekeycache || true; time $nodetool_bin invalidaterowcache || true"
    else
        time $nodetool_bin invalidatekeycache && time $nodetool_bin invalidaterowcache || true
    fi
    sleep 1
}

start_cassandra() {

    echo "Pruning and restoring tokens part not handled for local cassandra yet. Exiting..."
    exit 1

    local expt_config="$1"
    local config_file_abs_path="$2"
    local output_file="$3"

    # Validate inputs
    if [ -z "$expt_config" ] || [ -z "$config_file_abs_path" ] || [ -z "$output_file" ]; then
        echo "Error: Missing arguments to start_cassandra(expt_config, config_file_abs_path, output_file)" >&2
        return 1
    fi

    thp_check_all "false" || {
        echo "ERROR: THP check failed." >&2
        return 1
    }

    # Remove the logs folder before starting Cassandra
    rm -rf "$CASSANDRA_HOME_DIR/logs" || {
        echo "ERROR: Failed to remove logs directory at $CASSANDRA_HOME_DIR/logs" >&2
        return 1
    }

    local cgroup_str=""
    if [[ "$CGROUPV2_ENABLED" == true ]]; then 
        cgroup_str="$THROTTLE_SUBSCRIPT_PATH $(get_cgroupv2_str_prefix $CGROUPV2_DATA_FOLDER)"
    fi

    cassandra_bin=$(get_cassandra_bin "$expt_config")
    # Prints from shared library will appear in the output_file only if the -f flag is used
    # Hence we use -f here to ensure everything appear in the logs
    $cgroup_str $cassandra_bin -Dcassandra.config=file://$config_file_abs_path -f > "$output_file" 2>&1 &

    wait_for_cassandra || {
        echo "ERROR: Cassandra startup failed."
        stop_cassandra
        exit 1
    }

    local sleep_time=5
    echo "Cassandra started successfully. Waiting for $sleep_time seconds to stabilize..."
    sleep "$sleep_time"
}

stop_cassandra() {
    echo "Stopping Cassandra..."
    killall -w java || true
    sleep 1
    # Sometimes Cassandra does not stop after one killall, so we try again
    killall -w java || true
}

start_db() {
    local db_type="$1"
    local config="$2"
    local config_file_abs_path="$3"
    local output_file="$4"

    # Validate inputs
    if [ -z "$db_type" ] || [ -z "$config" ] || [ -z "$config_file_abs_path" ] || [ -z "$output_file" ]; then
        echo "Error: Missing arguments to start_db(db_type, config, config_file_abs_path, output_file)" >&2
        return 1
    fi

    case "$db_type" in
        cassandra)
            start_cassandra "$config" "$config_file_abs_path" "$output_file"
            ;;
        rocksdb)
            echo "start_db: No operation needed for rocksdb."
            ;;
        *)
            echo "start_db: error: Unsupported database type '$db_type'." >&2
            return 1
            ;;
    esac
}

stop_db() {
    local db_type="$1"

     # Validate input
    if [ -z "$db_type" ]; then
        echo "Error: Missing argument to stop_db(db_type)" >&2
        return 1
    fi

    case "$db_type" in
        cassandra)
            stop_cassandra
            ;;
        rocksdb)
            echo "stop_db: No operation needed for rocksdb."
            ;;
        *)
            echo "stop_db: error: Unsupported database type '$db_type'." >&2
            return 1
            ;;
    esac
}

# Function to create tarball from DATA_PATH
create_data_tarball() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local data_path="$2"  # Path to the data directory to be archived
    local tarball_path="$3"  # Path where the tarball will be created

    if [ -z "$data_path" ] || [ -z "$tarball_path" ]; then
        echo "Error: data_path or tarball_path is not set. data_path: $data_path  ; tarball_path:$tarball_path" >&2
        return 1
    fi

    # Ensure the parent directory for tarball_path exists
    local tarball_dir
    tarball_dir="$(dirname "$tarball_path")"
    echo "Ensuring tarball directory exists: $tarball_dir"
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "mkdir -p \"$tarball_dir\""
    else
        mkdir -p "$tarball_dir"
    fi

    echo "Deleting existing tarball: $tarball_path"
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "rm -rf \"$tarball_path\""
    else
        rm -rf "$tarball_path"
    fi

    echo "Creating tarball from $data_path to $tarball_path"
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "time tar -cf \"$tarball_path\" -C \"$(dirname "$data_path")\" \"$(basename "$data_path")\""
        if [[ $? -ne 0 ]]; then
            echo "Error: Failed to create tarball on remote nodes." >&2
            return 1
        fi
    else
        time tar -cf "$tarball_path" -C "$(dirname "$data_path")" "$(basename "$data_path")"
    fi
    return 0
}

restore_data_from_another_cluster() {
    local is_remote="$1"      # Optional argument to indicate if this is a remote operation
    local dest_data_path="$2"  # Destination DATA_PATH to restore to
    local file_to_extract="$3" # Optional: relative path of file inside the source path to extract

    local -a source_cluster_ips=("${DATA_SOURCE_CLUSTER_IPS[@]}")   # Array of source cluster IPs
    local -a source_cluster_paths=("${DATA_SOURCE_CLUSTER_PATHS[@]}") # Array of source cluster DATA_PATHs

    # If file_to_extract is defined, append it to each source paths
    if [ -n "$file_to_extract" ]; then
        for i in "${!source_cluster_paths[@]}"; do
            source_cluster_paths[i]="${source_cluster_paths[i]%/}/$file_to_extract"
        done
    fi

    echo "Source cluster IPs: ${source_cluster_ips[*]}"
    echo "Source cluster DATA_PATHs: ${source_cluster_paths[*]}"
    echo "Destination DATA_PATH: $dest_data_path"
    echo "File to extract (if any): $file_to_extract"

    # Validate inputs
    # only is_remote = true is allowed here
    if [ "$is_remote" != "true" ]; then
        echo "Error: restore_data_from_another_cluster currently supports only remote operation (is_remote=true)." >&2
        return 1
    fi

    # Lengths of NODE_LIST, source_cluster_ips, and source_cluster_paths must match
    local num_nodes=${#NODE_LIST[@]}
    local num_source_ips=${#source_cluster_ips[@]}
    local num_source_paths=${#source_cluster_paths[@]}
    if [ "$num_nodes" -ne "$num_source_ips" ] || [ "$num_nodes" -ne "$num_source_paths" ]; then
        echo "Error: Lengths of NODE_LIST, source_cluster_ips, and source_cluster_paths must match." >&2
        echo "NODE_LIST length: $num_nodes, source_cluster_ips length: $num_source_ips, source_cluster_paths length: $num_source_paths" >&2
        return 1
    fi

    # No source cluster ip or path can be empty
    for ip in "${source_cluster_ips[@]}"; do
        if [ -z "$ip" ]; then
            echo "Error: One of the source cluster IPs is empty." >&2
            return 1
        fi
    done
    for path in "${source_cluster_paths[@]}"; do
        if [ -z "$path" ]; then
            echo "Error: One of the source cluster DATA_PATHs is empty." >&2
            return 1
        fi
    done

    # No elements in NODE_LIST and source_cluster_ips arrays can be the common
    for i in "${!NODE_LIST[@]}"; do
        for j in "${!source_cluster_ips[@]}"; do
            if [ "${NODE_LIST[i]}" == "${source_cluster_ips[j]}" ]; then
                echo "Error: NODE_LIST and source_cluster_ips cannot have common elements. Found common element: ${NODE_LIST[i]}" >&2
                return 1
            fi
        done
    done

    # Check if all nodes in source_cluster_ips are reachable via ssh
    echo "Checking SSH connectivity to source data cluster nodes..."
    for ip in "${source_cluster_ips[@]}"; do
        ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no "$ip" "echo 2>&1" && {
            echo "SSH connectivity to $ip successful."
        } || {
            echo "Error: Unable to connect to $ip via SSH. Please ensure SSH keys are set up correctly." >&2
            return 1
        }
    done
    echo "SSH connectivity to all source data cluster nodes verified."

    echo "🔄 Restoring data from another cluster to current cluster path: $dest_data_path"

    # Delete existing DATA_PATH on all nodes
    if [ -n "$file_to_extract" ]; then
        echo "Doing mkdir parent of DATA_PATH"
        run_on_nodes " mkdir -p \"$(dirname "$dest_data_path")\" " || {
            echo "Error: Failed to create parent of DATA_PATH on one or more nodes." >&2
            return 1
        }
    else
        echo "Deleting existing DATA_PATH: $dest_data_path" and doing mkdir parent of DATA_PATH
        run_on_nodes " rm -rf \"$dest_data_path\" && mkdir -p \"$(dirname "$dest_data_path")\" " || {
            echo "Error: Failed to delete or create DATA_PATH on one or more nodes." >&2
            return 1
        }
    fi

    # Do parallel scp from source cluster nodes to current cluster nodes
    # echo "Starting parallel SCP from source cluster nodes to current cluster nodes..."
    # local start_time=$(date +%s)
    # local scp_pids=()
    # for i in "${!NODE_LIST[@]}"; do
    #     local dest_node="${NODE_LIST[i]}"
    #     local source_ip="${source_cluster_ips[i]}"
    #     local source_path="${source_cluster_paths[i]}"
    #     echo "SCP from $source_ip:$source_path to $dest_node:$dest_data_path"
    #     scp -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no -r "$source_ip:$source_path" "$dest_node:$dest_data_path" &
    #     scp_pids+=($!)
    # done
    # # Wait for all specific scp pids to finish
    # for pid in "${scp_pids[@]}"; do
    #     wait "$pid"
    # done
    # local end_time=$(date +%s)
    # local duration=$((end_time - start_time))
    # echo "All SCP operations completed in $duration seconds."

    local start_time=$(date +%s)
    echo "Starting parallel SCP from source data cluster nodes to current cluster nodes ..."
    run_on_nodes "DEST_IPS_STR='${NODE_LIST[*]}' SRC_IPS_STR='${source_cluster_ips[*]}' SRC_DIRS_STR='${source_cluster_paths[*]}' DEST_DATA_PATH='${dest_data_path}' bash -lc '
set -euo pipefail

# Fail fast if caller forgot to populate these.
: \"\${DEST_IPS_STR:?missing DEST_IPS_STR}\"
: \"\${SRC_IPS_STR:?missing SRC_IPS_STR}\"
: \"\${SRC_DIRS_STR:?missing SRC_DIRS_STR}\"
: \"\${DEST_DATA_PATH:?missing DEST_DATA_PATH}\"

# Turn the space-separated strings into arrays
read -a DEST_IPS <<< \"\$DEST_IPS_STR\"
read -a SRC_IPS  <<< \"\$SRC_IPS_STR\"
read -a SRC_DIRS <<< \"\$SRC_DIRS_STR\"

# Map this host to its index (first IP from hostname -I, no single quotes anywhere)
my_ip=\$(hostname -I | sed \"s/ .*//\")
idx=-1
for i in \"\${!DEST_IPS[@]}\"; do
if [[ \"\${DEST_IPS[i]}\" == \"\$my_ip\" ]]; then idx=\$i; break; fi
done
if (( idx < 0 )); then
echo \"MAP_FAIL: my_ip=\$my_ip not in DEST_IPS [\${DEST_IPS[*]}]\" >&2
exit 2
fi

src_ip=\${SRC_IPS[\$idx]}
src_dir=\${SRC_DIRS[\$idx]}

echo \"COPY \$src_ip:\$src_dir/  ->  \$DEST_DATA_PATH   \"
scp -rq \"\$src_ip:\$src_dir/\" \"\$DEST_DATA_PATH\"
'"
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    echo "All SCP operations completed in $duration seconds."

    # Check if the file/folder sizes match to verify successful copy
    # echo "Verifying data integrity by comparing sizes..."
    # for i in "${!NODE_LIST[@]}"; do
    #     local dest_node="${NODE_LIST[i]}"
    #     local source_ip="${source_cluster_ips[i]}"
    #     local source_path="${source_cluster_paths[i]}"
    #     local source_size
    #     local dest_size
    #     source_size=$(ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$source_ip" "du -sb \"$source_path\" | cut -f1")
    #     dest_size=$(ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$dest_node" "du -sb \"$dest_data_path\" | cut -f1")
    #     echo "Source ($source_ip:$source_path) size: $source_size bytes"
    #     echo "Destination ($dest_node:$dest_data_path) size: $dest_size bytes"
    #     if [ "$source_size" != "$dest_size" ]; then
    #         echo "Error: Size mismatch for node $dest_node. Source size: $source_size, Destination size: $dest_size" >&2
    #         return 1
    #     fi
    # done
    # echo "Data integrity verified. Sizes match on all nodes."
    return 0
}

# -----------------------------------------------------------------------------
# Function: restore_data_from_tarball
#
# Description:
#   Restores the DATA_PATH directory or a specific file from the tarball specified by TARBALL_PATH.
#
# Arguments:
#   $1 - is_remote (optional): If "true", performs the operation on all nodes in NODE_LIST via run_on_nodes.
#   $2 - data_path: Path to the data directory to be restored (or parent directory for file restore).
#   $3 - tarball_path: Path to the tarball to be extracted.
#   $4 - file_to_extract (optional): Relative path of the file inside the tarball to extract.
#
# Returns:
#   0 if the restore operation succeeds.
#   1 if any error occurs during the restore process.
# -----------------------------------------------------------------------------
restore_data_from_tarball() {
    local is_remote="$1"      # Optional argument to indicate if this is a remote operation
    local data_path="$2"      # Path to the data directory to be restored (or parent dir for file restore)
    local tarball_path="$3"   # Path to the tarball to be extracted
    local file_to_extract="$4" # Optional: relative path of file inside tarball to extract

    # Validate inputs
    if [ -z "$data_path" ] || [ -z "$tarball_path" ]; then
        echo "Error: data_path or tarball_path is not set. data_path: $data_path  ; tarball_path:$tarball_path" >&2
        return 1
    fi

    echo "🔄 Restoring data from tarball: $tarball_path to $data_path"
    if [ -n "$file_to_extract" ]; then
        echo "Extracting specific file: $file_to_extract"
    fi

    if [ "$is_remote" = "true" ]; then
        run_on_nodes "[ -f \"$tarball_path\" ]" || {
            echo "Error: Tarball $tarball_path does not exist on one or more nodes." >&2
            return 1
        }
    else
        if [[ ! -f "$tarball_path" ]]; then
            echo "Error: Tarball $tarball_path does not exist." >&2
            return 1
        fi
    fi

    # echo "Deleting existing DATA_PATH: $data_path"
    # if [ "$is_remote" = "true" ]; then
    #     run_on_nodes "rm -rf \"$data_path\" && mkdir -p \"$data_path\" "
    # else
    #     rm -rf "$data_path" && mkdir -p "$data_path"
    # fi

    if [ -n "$file_to_extract" ]; then
        # Only mkdir, do not delete the whole data path when extracting a specific file
        echo "mkdir DATA_PATH: $data_path"
        if [ "$is_remote" = "true" ]; then
            run_on_nodes "mkdir -p \"$data_path\" "
        else
            mkdir -p "$data_path"
        fi
        # Extract only the specific file, preserving folder tree
        echo "Extracting $file_to_extract from $tarball_path to $data_path"
        if [ "$is_remote" = "true" ]; then
            time run_on_nodes "tar -xf \"$tarball_path\" -C \"$data_path\" --strip-components=1 \"$file_to_extract\""
            if [[ $? -ne 0 ]]; then
                echo "Error: Failed to extract file $file_to_extract from tarball on remote nodes." >&2
                return 1
            fi
        else
            time tar -xf "$tarball_path" -C "$data_path" --strip-components=1 "$file_to_extract"
            if [[ $? -ne 0 ]]; then
                echo "Error: Failed to extract file $file_to_extract from tarball locally." >&2
                return 1
            fi
        fi
    else
        # Extract everything
        echo "Deleting existing DATA_PATH: $data_path"
        if [ "$is_remote" = "true" ]; then
            run_on_nodes "rm -rf \"$data_path\" && mkdir -p \"$data_path\" "
        else
            rm -rf "$data_path" && mkdir -p "$data_path"
        fi
        echo "Extracting all contents from $tarball_path to $(dirname "$data_path")"
        if [ "$is_remote" = "true" ]; then
            time run_on_nodes "tar -xf \"$tarball_path\" -C \"$(dirname "$data_path")\""
            if [[ $? -ne 0 ]]; then
                echo "Error: Failed to extract tarball on remote nodes." >&2
                return 1
            fi
        else
            time tar -xf "$tarball_path" -C "$(dirname "$data_path")"
            if [[ $? -ne 0 ]]; then
                echo "Error: Failed to extract tarball locally." >&2
                return 1
            fi
        fi
    fi
    return 0
}

run_on_nodes_simple() {
    local cmd="$1"
    echo "🔧  Running command on all nodes: $cmd"
    for host in "${NODE_LIST[@]}"; do
        echo "        Executing on $host: $cmd"
        ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$host" "$cmd"
        if [[ $? -ne 0 ]]; then
            echo "❌  Command failed on node: $host  --> Command: $cmd"
            exit 1
        fi
    done
    return 0
}

_run_on_nodes() {
    local cmd="$1"
    shift
    local nodes=("$@") # nodes passed as additional arguments after cmd

    if [ "$ANSIBLE_SUPPRESS_OUTPUT" = false ]; then
        echo "🔧  Running command on nodes (via Ansible): $cmd"
    fi

    # Create a comma-separated list of nodes for Ansible
    local hosts_csv
    hosts_csv="$(IFS=,; echo "${nodes[*]}")"

    local ansible_condense_output=""
    if [ "$ANSIBLE_CONDENSE_OUTPUT" = true ]; then
        ansible_condense_output="-o"
    fi

    local ansible_ssh_args=' -o StrictHostKeyChecking=no ' # Disable strict host key checking for Ansible SSH connections

    local ansible_forks=1000
    local rc=0
    if [ "$ANSIBLE_SUPPRESS_OUTPUT" = true ]; then
        ANSIBLE_SSH_ARGS="$ansible_ssh_args" ansible all -i "$hosts_csv," -m shell -a "$cmd" -u "$USER" --timeout=0 --forks $ansible_forks $ansible_condense_output >/dev/null 2>&1
        rc=$?
    else
        ANSIBLE_SSH_ARGS="$ansible_ssh_args" ansible all -i "$hosts_csv," -m shell -a "$cmd" -u "$USER" --timeout=0 --forks $ansible_forks $ansible_condense_output
        rc=$?
    fi
    
    if [[ $rc -ne 0 ]]; then
        echo "❌  Command failed on one or more nodes: $cmd (exit code: $rc)"
        return $rc
    fi
    return 0
}

run_on_nodes() {
    local cmd="$1"
    _run_on_nodes "$cmd" "${NODE_LIST[@]}"
    return $?
}

run_on_extra_requester_nodes() {
    local cmd="$1"
    _run_on_nodes "$cmd" "${EXTRA_REQUESTER_NODES[@]}"
    return $?
}

run_on_extra_requester_nodes_and_current_node() {
    local cmd="$1"
    # Create a new array including EXTRA_REQUESTER_NODES and localhost
    local nodes_with_requester=("${EXTRA_REQUESTER_NODES[@]}" "localhost")
    _run_on_nodes "$cmd" "${nodes_with_requester[@]}"
    return $?
}

check_and_install_sshfs() {
    # Check if sshfs is installed on all nodes
    local sshfs_check_and_install_cmd='command -v sshfs >/dev/null 2>&1 || (sudo yum config-manager --set-enabled powertools && sudo yum install -y sshfs)'
    run_on_nodes "$sshfs_check_and_install_cmd" || {
        echo "❌  Failed to verify or install sshfs on one or more nodes." >&2
        exit 1
    }
    run_on_extra_requester_nodes "$sshfs_check_and_install_cmd" || {
        echo "❌  Failed to verify or install sshfs on extra requester nodes." >&2
        exit 1
    }
}

# Delete & recopy the entire Cassandra tree to each node
copy_cassandra_to_nodes() {
    echo "📦  Copying Cassandra tree to all nodes (full wipe) ..."
    run_on_nodes "rm -rf \"$CASSANDRA_HOME_DIR\""
    # Raise error if CASSANDRA_HOME_DIR varialble is not set or CASSANDRA_HOME_DIR does not exist
    if [[ -z "$CASSANDRA_HOME_DIR" || ! -d "$CASSANDRA_HOME_DIR" ]]; then
        echo "❌  CASSANDRA_HOME_DIR is not set or does not exist: $CASSANDRA_HOME_DIR" >&2
        return 1
    fi
    # Delete the log folder on local cassandra folder before copying it
    rm -rf "$CASSANDRA_HOME_DIR/logs"  # Ensure local logs are removed before copying
    for host in "${NODE_LIST[@]}"; do
        scp -rq "$CASSANDRA_HOME_DIR" "$host:$CASSANDRA_HOME_DIR" || return 1
    done
}

# Copy the LD_PRELOAD .so files to each node
copy_so_files_to_nodes() {
    local -n configs_list=$1  # Use nameref for array argument
    echo "📦  Copying .so files to all nodes for configs: ${configs_list[*]} ..."
    for config in "${configs_list[@]}"; do
        if [[ "$config" != "vanilla" ]]; then
            local so_file="$SHARED_LIB_FOLDER/lib_speedyio_${config}.so"
            if [[ ! -f "$so_file" ]]; then
                echo "❌  .so file not found: $so_file"
                return 1
            fi
            for host in "${NODE_LIST[@]}"; do
                scp -q "$so_file" "$host:$CASSANDRA_HOME_DIR/bin/" || {
                    echo "❌  Failed to copy $so_file to $host:$CASSANDRA_HOME_DIR/bin/"
                    return 1
                }
            done
        fi
    done
    echo "✅  .so files copied to all nodes."
}

# Delete & recopy the entire ycsb tree to each node
copy_ycsb_to_extra_requester_nodes() {
    echo "📦  Copying YCSB tree to all nodes ..."
    
    # Raise error if YCSB_HOME_DIR varialble is not set or YCSB_HOME_DIR does not exist
    if [[ -z "$YCSB_HOME_DIR" || ! -d "$YCSB_HOME_DIR" ]]; then
        echo "❌  YCSB_HOME_DIR is not set or does not exist: $YCSB_HOME_DIR" >&2
        return 1
    fi

    # Raise error if YCSB_WORKLOADS_DIR variable is not set or YCSB_WORKLOADS_DIR does not exist
    if [[ -z "$YCSB_WORKLOADS_DIR" || ! -d "$YCSB_WORKLOADS_DIR" ]]; then
        echo "❌  YCSB_WORKLOADS_DIR is not set or does not exist: $YCSB_WORKLOADS_DIR" >&2
        return 1
    fi

    # run_on_extra_requester_nodes "rm -rf \"$YCSB_HOME_DIR\"" || {
    #     echo "❌  Failed to delete YCSB directory on nodes: $YCSB_HOME_DIR" >&2
    #     return 1
    # }

    # # Copy the YCSB directory to each node
    # for host in "${EXTRA_REQUESTER_NODES[@]}"; do
    #     scp -rq "$YCSB_HOME_DIR" "$host:$YCSB_HOME_DIR" || {
    #         echo "❌  Failed to copy YCSB tree to $host:$YCSB_HOME_DIR" >&2
    #         return 1
    #     }
    # done

    ssh_opts=' -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=0 '

    for host in "${EXTRA_REQUESTER_NODES[@]}"; do
    # Ensure destination exists
    ssh $ssh_opts "$host" "mkdir -p '$YCSB_HOME_DIR'" || {
        echo "❌ mkdir on $host failed" >&2; exit 1; }

    # Mirror local → remote (compress + checksum + safe updates)
    # Why `rsync` here instead of `rm -rf && scp -r`
    # ------------------------------------------------
    # Problem with scp:
    #   • `scp -r` always recopies the entire tree → slow and wasteful on large dirs.
    #   • Common pattern `rm -rf DEST && scp -r SRC DEST` creates a race window:
    #       - Other scripts/processes can see a half-populated or missing tree.
    #       - Two concurrent scripts can stomp each other’s deletes/copies.
    #   • `scp` offers no “mirror” semantics (can’t remove files that no longer exist).
    #   • No safe “write to temp then rename” → readers can observe truncated files.
    #   • If multiple scripts run concurrently, they can clobber each other. 
    #     One script may want to start ycsb while another deletes files and recopies.
    #
    # Why rsync fixes it:
    #   • Delta transfer: only changed files/blocks move (faster, less network).
    #   • Mirror semantics: `--delete` removes stale files on the remote.
    #   • Safer updates: `--partial --delay-updates` writes temp files then renames
    #     at the end, so consumers don’t see partial files.
    #   • Can add compression (`-z`) and integrity (`--checksum`) when needed.
    #   • Works over SSH with the same auth (`-e "ssh ..."`).
    #
    # Flags we use (and why):
    #   -a            : archive (perms, times, recurse)
    #   -z            : compress over the wire (LAN optional; WAN helpful)
    #   --checksum    : detect changes by content, not just size/mtime (CPU trade-off)
    #   --delete      : mirror exactly; remove files that disappeared locally
    #   --partial     : keep partials if interrupted
    #   --delay-updates: stage to temp and atomically flip at end to avoid torn reads
    #   -e "ssh ...": pass SSH options (BatchMode, StrictHostKeyChecking, etc.)
    #
    # Trailing slash gotcha:
    #   SRC/ → copy *contents* of SRC into DEST (what we want for mirroring).
    #   SRC  → create DEST/SRC then copy into that subdir.
    #   We deliberately use trailing slashes on both sides:  SRC/  DEST/
    #
    # Concurrency note:
    #   • rsync removes the “empty dir” window and avoids partial-file reads, but
    #     two writers still racing to the same DEST can clobber each other.
    #   • If concurrent pushes are possible, use a lock (flock) or the “versioned
    #     release + symlink flip” pattern and GC old releases.
    rsync -az --checksum --delete --partial --delay-updates \
            -e "ssh $ssh_opts" \
            "$YCSB_HOME_DIR"/  "$host:$YCSB_HOME_DIR"/  || {
        echo "❌ rsync to $host failed" >&2; exit 1; }
    done

    echo "✅  YCSB tree copied to all extra requester nodes."
}
 
# -----------------------------------------------------------------------------
# copy_monitoring_scripts_to_nodes
#
# Copies monitoring scripts to all nodes specified in NODE_LIST.
#
# Globals:
#   MEMINFO_LOGGER_PATH      - Path to the meminfo logger script.
#   VMSTAT_LOGGER_PATH       - Path to the vmstat logger script.
#   VMSTAT_COMMAND_LOGGER_PATH - Path to the vmstat command logger script.
#   BUDGET_MEMORY_PATH       - Path to the budget memory script.
#   BUDGET_CPU_PATH          - Path to the budget cpu script.
#   THROTTLE_SUBSCRIPT_PATH  - Path to the cgroup throttling subscript
#   CASSANDRA_COMPACTIONSTATS_LOGGER_PATH - Path to cassandra compactionstats logger script
#   CASSANDRA_TABLESTATS_LOGGER_PATH - Path to cassandra tablestats logger script
#   CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH - Path to cassandra table histograms logger script
#   REMOTE_SCRIPTS_DIR       - Remote directory where scripts will be copied.
#   NODE_LIST                - Array of hostnames or IPs of the nodes.
#
# Arguments:
#   None
#
# Returns:
#   0 if scripts are successfully copied to all nodes.
#   1 if any error occurs during directory creation or file transfer.
#
# Example:
#   copy_monitoring_scripts_to_nodes
#
# Notes:
#   - Requires passwordless SSH access to all nodes in NODE_LIST.
#   - The function creates the remote directory if it does not exist.
#   - The scripts are copied using scp to each node.
# -----------------------------------------------------------------------------
copy_monitoring_scripts_to_nodes() {
    local scripts=("$MEMINFO_LOGGER_PATH" "$VMSTAT_LOGGER_PATH" "$VMSTAT_COMMAND_LOGGER_PATH" "$BUDGET_MEMORY_PATH" "$BUDGET_CPU_PATH" "$THROTTLE_SUBSCRIPT_PATH" "$CASSANDRA_COMPACTIONSTATS_LOGGER_PATH" "$CASSANDRA_TABLESTATS_LOGGER_PATH" "$CASSANDRA_TABLEHISTOGRAMS_LOGGER_PATH" "$FILE_VALUES_LOGGER_PATH")

    run_on_nodes "mkdir -p \"$REMOTE_SCRIPTS_DIR\"" || {
        echo "❌  Failed to create remote scripts directory: $REMOTE_SCRIPTS_DIR" >&2
        return 1
    }

    for host in "${NODE_LIST[@]}"; do
        scp -q "${scripts[@]}" "$host:$REMOTE_SCRIPTS_DIR/" || {
            echo "❌  Failed to copy monitoring scripts to $host:$REMOTE_SCRIPTS_DIR" >&2
            return 1
        }
    done

    # # Ensure scripts are executable on all nodes
    # local script_names=()
    # # Get  the base names of the scripts for remote storage
    # for script in "${scripts[@]}"; do
    #     script_names+=("$(basename "$script")")
    # done
    # run_on_nodes "cd $REMOTE_SCRIPTS_DIR && chmod +x ${script_names[*]}"
    echo "✅  Monitoring scripts copied to all nodes at $REMOTE_SCRIPTS_DIR"
}


_create_remote_mounts() {
    local logs_root_dir="$1"
    local create_separate_dir_per_node="${2:-true}"  # Default to true if not provided
    local remote_mount_dir="$3"
    local requester_host="$4"
    local -n node_list_ref="$5"  # Pass array name, use nameref

    check_and_install_sshfs

    # local sshfs_opts="reconnect,ServerAliveInterval=15,ServerAliveCountMax=3,BatchMode=yes,StrictHostKeyChecking=accept-new"
    # local   sshfs_opts="reconnect,ServerAliveInterval=15,ServerAliveCountMax=3,BatchMode=yes,StrictHostKeyChecking=accept-new,cache=no,direct_io,no_readahead"
    # local   sshfs_opts="reconnect,ServerAliveInterval=15,ServerAliveCountMax=3,BatchMode=yes,StrictHostKeyChecking=accept-new,sshfs_sync,no_readahead,sync_readdir,direct_io"
    local  sshfs_opts="reconnect,ServerAliveInterval=15,ServerAliveCountMax=3,BatchMode=yes,StrictHostKeyChecking=accept-new,sshfs_sync,direct_io,cache=no,attr_timeout=0,entry_timeout=0,negative_timeout=0,no_readahead,sync_readdir"



    mkdir -p "$logs_root_dir"   # ensure root exists locally

    for host in "${node_list_ref[@]}"; do
        # Use $logs_root_dir directly if create_separate_dir_per_node is false
        if [ "$create_separate_dir_per_node" = true ]; then
            # Create a separate directory for each node
            local host_dir="$logs_root_dir/logs__$host"
            mkdir -p "$host_dir"  # per-node dir on requester
        else
            # Use the root logs directory directly
            local host_dir="$logs_root_dir"
        fi

        echo "📦  mounting "$host_dir" on $host at $remote_mount_dir  ..."

        ssh -o StrictHostKeyChecking=no "$host" bash -s -- "$remote_mount_dir" "$host_dir" "$requester_host" "$sshfs_opts" <<'REMOTE'
set -e
remote_mount="$1"
host_dir="$2"
requester="$3"
opts="$4"

if [ ! -d "$remote_mount" ]; then
    sudo mkdir -p "$remote_mount"
fi

sudo chown "$USER" "$remote_mount"

if mountpoint -q "$remote_mount"; then
    echo "❌  $remote_mount already mounted" >&2
    exit 1
fi


if [ "$(ls -A "$remote_mount")" ]; then
    echo "❌  Remote mount directory $remote_mount is not empty. Please unmount it first or make sure it is empty if something got created in it by mistake." >&2
    exit 1
fi

sshfs -o "$opts" "$USER@$requester:$host_dir" "$remote_mount"

echo "   ↪ mount ok"
REMOTE

        [[ $? -ne 0 ]] && { echo "❌  Mounting failed on $host"; return 1; }
    done

    echo "✅  All nodes mounted successfully at $remote_mount_dir, logs will be stored in: $logs_root_dir"
}

create_log_mounts_ucsb() {
    local logs_root_dir="$1"
    _create_remote_mounts "$logs_root_dir" "true" "$REMOTE_MOUNT_DIR" "$REQUESTER_NODE_IP" NODE_LIST
    copy_monitoring_scripts_to_nodes || {
        echo "❌  Failed to copy monitoring scripts to nodes." >&2
        return 1
    }
}

create_log_mounts_extra_requesters() {
    local logs_root_dir="$1"
    local timestamp="$2"  # This will be used to suffix the folder name for the mountpoint
    local __resultvar="$3"  # output variable name (by reference)
    local remote_mount_dir_for_requester="${REMOTE_MOUNT_DIR_FOR_EXTRA_REQUESTER}${timestamp}"
    _create_remote_mounts "$logs_root_dir" "false" "$remote_mount_dir_for_requester" "$REQUESTER_NODE_IP" EXTRA_REQUESTER_NODES
    
    # Use indirect reference to assign the value to the passed-in variable name
    if [[ "$__resultvar" ]]; then
        eval "$__resultvar=\"$remote_mount_dir_for_requester\""
    fi
}


###############################################################################
# remove_log_mounts_ucsb
# ---------------------------------------------------------------------------
# Unmounts /mnt/ucsb_benchlogs on every node in parallel via run_on_nodes.
###############################################################################

_remove_remote_mounts() {
    local remote_mount_dir="$1"
    local -n node_list_ref="$2"  # Pass array name, use nameref

    check_and_install_sshfs

    # findmnt is more reliable for FUSE situations, mountpoint command sometimes can't 
    # resolve the directory correctly if it is stuck in a zombie state
    local cmd='findmnt --target "'"$remote_mount_dir"'" && \
               (fusermount -u "'"$remote_mount_dir"'" 2>/dev/null || \
                sudo umount -l "'"$remote_mount_dir"'" 2>/dev/null) || true'

    echo "🗑️  unmounting $remote_mount_dir on all nodes …"
    for host in "${node_list_ref[@]}"; do
        ssh -o StrictHostKeyChecking=no "$host" "$cmd" || {
            echo "⚠️  unmount reported errors on node $host"; return 1;
        }
    done
    echo "✅  unmount completed."
}

remove_log_mounts_ucsb() {
    _remove_remote_mounts "$REMOTE_MOUNT_DIR" NODE_LIST
}

remove_log_mounts_extra_requesters() {
    local timestamp="$1"  # This will be used to suffix the folder name for the mountpoint
    local remote_mount_dir_for_requester="${REMOTE_MOUNT_DIR_FOR_EXTRA_REQUESTER}${timestamp}"
    # Return if EXTRA_REQUESTER_NODES is empty or not set
    if [ -z "${EXTRA_REQUESTER_NODES+x}" ] || [ ${#EXTRA_REQUESTER_NODES[@]} -eq 0 ]; then
        echo "No extra requester nodes to unmount."
        return 0
    fi
    _remove_remote_mounts "$remote_mount_dir_for_requester" EXTRA_REQUESTER_NODES
    run_on_extra_requester_nodes " sudo rm -rf $remote_mount_dir_for_requester "
}


# Prepare data directories (wipe + mkdir)
prepare_data_dirs_remote() {
    local data_path="$1"

    # Validate input
    if [ -z "$data_path" ]; then
        echo "Error: Missing argument to prepare_data_dirs_remote(data_path)" >&2
        return 1
    fi

    check_and_install_sshfs
    run_on_nodes "rm -rf \"$data_path\""
    run_on_nodes "mkdir -p \"$data_path\""
}


###############################################################################
# patch_cassandra_configs_remote
#   • For every host in NODE_LIST
#       1.  create tmp copy of cassandra.yaml        → patch → scp → rm
#       2.  create tmp copy of cassandra-env.sh      → patch → scp → rm
###############################################################################
patch_cassandra_configs_remote() {
    local cassandra_data_folder="$1"
    local cassandra_start_mode="$2"  # "normal" or "preloaded"

    # Validate inputs
    if [ -z "$cassandra_data_folder" ] || [ -z "$cassandra_start_mode" ]; then
        echo "Error: Missing arguments to patch_cassandra_configs_remote(cassandra_data_folder, cassandra_start_mode)" >&2
        return 1
    fi

    # cassandra_start_mode must be either "normal" or "preloaded"
    if [[ "$cassandra_start_mode" != "normal" && "$cassandra_start_mode" != "preloaded" ]]; then
        echo "Error: Invalid cassandra_start_mode '$cassandra_start_mode'. Must be 'normal' or 'preloaded'." >&2
        return 1
    fi

    local seeds_csv
    seeds_csv="$(IFS=,; echo "${NODE_LIST[*]}")"

    local unique_suffix_preloaded_cluster_name="Preloaded Start Mode $(date +%s)"

    for host in "${NODE_LIST[@]}"; do
        echo "🔧  Patching configs for $host ..."
        local node_ip="$host"   # host list already contains IPs

        # Declare empty associative array for preloaded tokens to start with
        declare -A cassandra_preloaded_yaml_overrides
        if [[ "$cassandra_start_mode" == "preloaded" ]]; then
            if [[ "$DB_TYPE" == "cassandra3" || "$DB_TYPE" == "cassandra4" || "$DB_TYPE" == "cassandra5" ]]; then
                # Set auto bootstrap to false for preloaded nodes
                cassandra_preloaded_yaml_overrides["auto_bootstrap"]=false
                cassandra_preloaded_yaml_overrides["cluster_name"]="Test Cluster $unique_suffix_preloaded_cluster_name"
                # Print the overrides for debugging
                # echo "    Preloaded YAML overrides for $host:"
                # for key in "${!cassandra_preloaded_yaml_overrides[@]}"; do
                #     echo "        $key: ${cassandra_preloaded_yaml_overrides[$key]}"
                # done
                local token_info_file="$cassandra_data_folder/cassandra_tokens.txt"
                get_tokens_assoc_from_file cassandra_preloaded_yaml_overrides "$token_info_file" "$host" || {
                    echo "❌  Failed to get tokens for $host from $token_info_file"
                    return 1
                }
            else
                echo "Error: Preloaded start mode is only supported for Cassandra 3.x in this script." >&2
                return 1
            fi
        fi
        

        # ------------------------------------------------------------------ #
        # 1. YAML  (tmp copy → patch → copy back)
        # ------------------------------------------------------------------ #
        local tmp_yaml
        local cassandra_yaml="$CASSANDRA_HOME_DIR/conf/cassandra.yaml"
        tmp_yaml=$(mktemp /tmp/cassandra_yaml.XXXX.yaml) \
            || { echo "❌  mktemp failed"; return 1; }
        cp "$cassandra_yaml" "$tmp_yaml"

        if [[ "$DB_TYPE" == "cassandra3" ]]; then
            update_cassandra3_yaml_with_yq_uncomment  "$tmp_yaml" \
            "$cassandra_data_folder/data" \
            "$cassandra_data_folder/commitlog" \
            "$cassandra_data_folder/saved_caches" \
            "$SEED_NODE_IP" "$node_ip" CASSANDRA3_YAML_OVERRIDES cassandra_preloaded_yaml_overrides \
            || { echo "❌  YAML patch failed for $host"; rm -f "$tmp_yaml"; return 1; }
        elif [[ "$DB_TYPE" == "cassandra4" ]]; then
            update_cassandra4_yaml_with_yq_uncomment  "$tmp_yaml" \
            "$cassandra_data_folder/data" \
            "$cassandra_data_folder/commitlog" \
            "$cassandra_data_folder/saved_caches" \
            "$SEED_NODE_IP" "$node_ip" CASSANDRA4_YAML_OVERRIDES cassandra_preloaded_yaml_overrides \
            || { echo "❌  YAML patch failed for $host"; rm -f "$tmp_yaml"; return 1; }
        elif [[ "$DB_TYPE" == "cassandra5" ]]; then
            update_cassandra5_yaml_with_yq_uncomment  "$tmp_yaml" \
            "$cassandra_data_folder/data" \
            "$cassandra_data_folder/commitlog" \
            "$cassandra_data_folder/saved_caches" \
            "$SEED_NODE_IP" "$node_ip" CASSANDRA5_YAML_OVERRIDES cassandra_preloaded_yaml_overrides \
            || { echo "❌  YAML patch failed for $host"; rm -f "$tmp_yaml"; return 1; }
        else
            echo "❌  patch_cassandra_configs_remote: Unsupported db_type: $db_type"; rm -f "$tmp_yaml"; return 1;
        fi
        
        
        # Debugging: show the diff between original and patched YAML
        # echo " ======== Debugging $cassandra_yaml diff for host: $host ========================= "
        # diff --ignore-all-space --ignore-blank-lines "$cassandra_yaml" "$tmp_yaml"
        # echo " ================================================================================= "

        scp -q "$tmp_yaml" "$host:$cassandra_yaml" \
            || { echo "❌  YAML scp failed for $host"; rm -f "$tmp_yaml"; return 1; }
        rm -f "$tmp_yaml"

        # ------------------------------------------------------------------ #
        # 2. cassandra-env.sh  (tmp copy → patch → copy back)
        # ------------------------------------------------------------------ #
        local env_sh="$CASSANDRA_HOME_DIR/conf/cassandra-env.sh"
        local tmp_env
        tmp_env=$(mktemp /tmp/cassandra_env.XXXX.sh) \
            || { echo "❌  mktemp failed"; return 1; }
        cp "$env_sh" "$tmp_env"

        update_cassandra_env_jmx "$tmp_env" "$node_ip" \
            || { echo "❌  env patch failed for $host"; rm -f "$tmp_env"; return 1; }
        
        # Debugging: show the diff between original and patched env
        # echo " ======== Debugging $env_sh diff for host: $host ========================= "
        # diff --ignore-all-space --ignore-blank-lines "$env_sh" "$tmp_env"
        # echo " ========================================================================= "

        scp -q "$tmp_env" "$host:$env_sh" \
            || { echo "❌  env scp failed for $host"; rm -f "$tmp_env"; return 1; }
        rm -f "$tmp_env"

        # Choose correct JVM overrides array and jvm.options file based on DB_TYPE
        if [[ "$DB_TYPE" == "cassandra3" ]]; then
            jvm_var_name="CASSANDRA3_JVM_OVERRIDES"
            jvm_options_file="$CASSANDRA_HOME_DIR/conf/jvm.options"

        elif [[ "$DB_TYPE" == "cassandra4" ]]; then
            jvm_var_name="CASSANDRA4_JVM_OVERRIDES"
            jvm_options_file="$CASSANDRA_HOME_DIR/conf/jvm11-server.options"

        elif [[ "$DB_TYPE" == "cassandra5" ]]; then
            jvm_var_name="CASSANDRA5_JVM_OVERRIDES"
            jvm_options_file="$CASSANDRA_HOME_DIR/conf/jvm17-server.options"

        else
            echo "❌  patch_cassandra_configs_remote: Unsupported DB_TYPE for JVM options: $DB_TYPE" >&2
            return 1
        fi

        # Check if the selected overrides array exists and is non-empty
        if ! declare -p "$jvm_var_name" &>/dev/null || eval "[ \${#${jvm_var_name}[@]} -eq 0 ]"; then
            echo "$jvm_var_name is not set or is empty. Skipping JVM options patching."
        else
            tmp_jvm_options=$(mktemp /tmp/jvm_options.XXXX.options) \
            || { echo "❌  mktemp failed"; return 1; }
            cp "$jvm_options_file" "$tmp_jvm_options"

            apply_jvm_overrides "$tmp_jvm_options" "$jvm_var_name" \
            || { echo "❌  JVM options patch failed for $host"; rm -f "$tmp_jvm_options"; return 1; }

            scp -q "$tmp_jvm_options" "$host:$jvm_options_file" \
            || { echo "❌  JVM options scp failed for $host"; rm -f "$tmp_jvm_options"; return 1; }
            rm -f "$tmp_jvm_options"
        fi

    done

    run_on_nodes "cp $cassandra_yaml $env_sh $jvm_options_file $REMOTE_MOUNT_DIR" || {
        echo "❌  Failed to copy patched configs to remote mount directory: $REMOTE_MOUNT_DIR"
        return 1
    }
    echo "📂  Patched configs copied to remote mount directory: $REMOTE_MOUNT_DIR"

    echo "✅  cassandra.yaml & cassandra-env.sh patched and copied to all nodes."
}

get_node_cfg() {
    local node="$1"
    echo "${NODE_CFG_MAP[$node]:-vanilla}"
}

get_launcher_for_node() {
    local node="$1"
    local cfg
    cfg="$(get_node_cfg "$node")"
    if [[ "$cfg" == "vanilla" ]]; then
        echo "bin/cassandra"
    else
        echo "bin/cassandra_${cfg}"
    fi
}

start_cassandra_cluster_mixed() {
    # No args required. Uses NODE_LIST + SEED_NODE_IP + NODE_CFG_MAP.
    if [[ -z "${NODE_LIST+x}" || ${#NODE_LIST[@]} -eq 0 ]]; then
        echo "❌ start_cassandra_cluster_mixed: NODE_LIST is empty/unset" >&2
        return 1
    fi

    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    # Basic checks
    run_on_nodes "[ -x \"$nodetool_bin\" ]" || {
        echo "❌ nodetool not found or not executable on one or more nodes: $nodetool_bin" >&2
        return 1
    }

    thp_check_all "true" || {
        echo "❌ THP check failed on one or more nodes." >&2
        return 1
    }

    # Verify per-node launcher exists
    for h in "${NODE_LIST[@]}"; do
        local launcher cfg
        launcher="$(get_launcher_for_node "$h")"
        cfg="$(get_node_cfg "$h")"
        ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$h" \
            "[ -x \"$CASSANDRA_HOME_DIR/$launcher\" ]" || {
                echo "❌ Missing Cassandra launcher on $h (cfg=$cfg): $CASSANDRA_HOME_DIR/$launcher" >&2
                return 1
            }
    done

    # Start seed
    local cgroup_str=""
    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        cgroup_str="$THROTTLE_SUBSCRIPT_PATH_REMOTE $(get_cgroupv2_str_prefix $CGROUPV2_DATA_FOLDER $SEED_NODE_IP)"
    fi

    local seed_launcher seed_cfg
    seed_launcher="$(get_launcher_for_node "$SEED_NODE_IP")"
    seed_cfg="$(get_node_cfg "$SEED_NODE_IP")"

    local seed_start_cmd="nohup $cgroup_str $CASSANDRA_HOME_DIR/$seed_launcher -f > $REMOTE_MOUNT_DIR/seed__${SEED_NODE_IP}__${seed_cfg}.log 2>&1 &"
    ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$SEED_NODE_IP" "$seed_start_cmd" || {
        echo "❌ Failed to start seed node: $SEED_NODE_IP (cfg=$seed_cfg)" >&2
        return 1
    }

    # Wait for seed UN
    local t=0
    while (( t < 90 )); do
        ssh -o StrictHostKeyChecking=no "$SEED_NODE_IP" "$nodetool_bin" status 2>/dev/null | grep -qE '^\s*UN\s+' && break
        sleep 2; ((t+=2))
    done
    (( t >= 90 )) && { echo "❌ Seed failed to start (no UN within 90s): $SEED_NODE_IP" >&2; return 1; }
    echo "✅ Seed node started: $SEED_NODE_IP (cfg=$seed_cfg)"

    # Followers, sequential
    local -a node_list_without_seed=()
    for h in "${NODE_LIST[@]}"; do
        [[ "$h" == "$SEED_NODE_IP" ]] || node_list_without_seed+=("$h")
    done

    local started=1
    local timeout=120

    for h in "${node_list_without_seed[@]}"; do
        local host_cfg host_launcher
        host_cfg="$(get_node_cfg "$h")"
        host_launcher="$(get_launcher_for_node "$h")"

        echo "🚀 starting follower node $h (cfg=$host_cfg) …"

        if [[ "$CGROUPV2_ENABLED" == true ]]; then
            cgroup_str="$THROTTLE_SUBSCRIPT_PATH_REMOTE $(get_cgroupv2_str_prefix $CGROUPV2_DATA_FOLDER $h)"
        else
            cgroup_str=""
        fi

        local host_start_cmd="nohup $cgroup_str $CASSANDRA_HOME_DIR/$host_launcher -f > $REMOTE_MOUNT_DIR/host__${h}__${host_cfg}.log 2>&1 &"
        ssh -o StrictHostKeyChecking=no "$h" "$host_start_cmd" || {
            echo "❌ launch failed for $h (cfg=$host_cfg)" >&2
            return 1
        }

        local expected=$(( ++started ))
        echo "⏳ waiting until $expected node(s) are UN …"

        local elapsed=0
        while (( elapsed < timeout )); do
            local un_count
            un_count=$(ssh "$SEED_NODE_IP" "$nodetool_bin" status 2>/dev/null \
                | awk '/^[[:space:]]*UN[[:space:]]+/ {c++} END{print c+0}')
            if (( un_count == expected )); then
                echo "✅ $h is UN (cfg=$host_cfg) (cluster size: $un_count/$expected)"
                break
            fi
            sleep 2; ((elapsed+=2))
        done

        if (( elapsed >= timeout )); then
            echo "❌ $h did not reach UN within ${timeout}s (cfg=$host_cfg)" >&2
            ssh "$SEED_NODE_IP" "$nodetool_bin" status
            return 1
        fi
    done

    echo "🎉 All ${#NODE_LIST[@]} nodes are UP and NORMAL."
    sleep 20
}


# Start the cluster – seed first, then others
start_cassandra_cluster() {
    local cfg="$1"     # vanilla | et_pvt_lru ...

    if [ -z "$cfg" ] ; then
        echo "Error: Missing arguments to start_cassandra_cluster(cfg)" >&2
        return 1
    fi

    local launcher="bin/cassandra"
    local v_launcher="bin/cassandra"
    [[ "$cfg" != "vanilla" ]] && launcher="bin/cassandra_${cfg}"

    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"
    # Check Cassandra launcher and nodetool on all nodes
    run_on_nodes "[ -x \"$CASSANDRA_HOME_DIR/$launcher\" ]" || {
        echo "❌  Cassandra launcher not found or not executable on one or more nodes: $CASSANDRA_HOME_DIR/$launcher"
        return 1
    }
    run_on_nodes "[ -x \"$nodetool_bin\" ]" || {
        echo "❌  nodetool not found or not executable on one or more nodes: $nodetool_bin"
        return 1
    }

    thp_check_all "true" || {
        echo "❌  THP check failed on one or more nodes."
        return 1
    }

    local cgroup_str=""
    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        cgroup_str="$THROTTLE_SUBSCRIPT_PATH_REMOTE $(get_cgroupv2_str_prefix $CGROUPV2_DATA_FOLDER $SEED_NODE_IP)"
    fi

    # Prints from shared library will appear in the output_file only if the -f flag is used
    # Hence we use -f here to ensure everything appear in the logs
    local seed_start_cmd="nohup $cgroup_str $CASSANDRA_HOME_DIR/$launcher -f > $REMOTE_MOUNT_DIR/seed__${SEED_NODE_IP}__${cfg}.log 2>&1 &"
    ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$SEED_NODE_IP" "$seed_start_cmd" || {
        echo "❌  Failed to start seed node: $SEED_NODE_IP"
        return 1
    }

    # Wait for seed to report UN
    local t=0; while (( t < 90 )); do
        ssh -o StrictHostKeyChecking=no "$SEED_NODE_IP" $nodetool_bin status 2>/dev/null | grep -qE '^\s*UN\s+' && break
        sleep 2; ((t+=2))
    done
    (( t >= 90 )) && { echo "Seed failed to start"; return 1; }

    echo "✅  Seed node started: $SEED_NODE_IP"

    ###############################################################################
    #  Sequential follower-node startup
    #  --------------------------------
    #  • Seed node is already up and UN.
    #  • Start one follower at a time.
    #  • After launching a node, poll the *seed* until *all nodes launched so far*
    #    show UN.  Only then continue to the next host.
    ###############################################################################

    # Build list that excludes the seed
    node_list_without_seed=()
    for h in "${NODE_LIST[@]}"; do
        [[ "$h" == "$SEED_NODE_IP" ]] || node_list_without_seed+=("$h")
    done

    started=1                       # seed already running
    timeout=120                      # seconds

    for h in "${node_list_without_seed[@]}"; do
        echo "🚀  starting follower node $h …"
        if [[ "$CGROUPV2_ENABLED" == true ]]; then
            cgroup_str="$THROTTLE_SUBSCRIPT_PATH_REMOTE $(get_cgroupv2_str_prefix $CGROUPV2_DATA_FOLDER $h)"
        fi
        # Prints from shared library will appear in the output_file only if the -f flag is used
        # Hence we use -f here to ensure everything appear in the logs
        local host_start_cmd="nohup $cgroup_str $CASSANDRA_HOME_DIR/$launcher -f > $REMOTE_MOUNT_DIR/host__${h}__${cfg}.log 2>&1 &"
        ssh -o StrictHostKeyChecking=no "$h" "$host_start_cmd" || { echo "❌  launch failed for $h"; return 1; }

        expected=$(( ++started ))   # seed + followers launched so far
        echo "⏳  waiting until $expected node(s) are UN …"

        elapsed=0
        while (( elapsed < timeout )); do
            # ssh "$SEED_NODE_IP" "$nodetool_bin" status  # For debugging
            un_count=$(ssh "$SEED_NODE_IP" "$nodetool_bin" status 2>/dev/null \
                    | awk '/^[[:space:]]*UN[[:space:]]+/ {c++} END{print c+0}')
            if (( un_count == expected )); then
                echo "✅  $h is UN  (cluster size: $un_count/$expected)"
                break
            fi
            sleep 2; ((elapsed+=2))
        done

        if (( elapsed >= timeout )); then
            echo "❌  $h did not reach UN within ${timeout}s"
            ssh "$SEED_NODE_IP" "$nodetool_bin" status   # debug snapshot
            return 1
        fi
    done

    echo "🎉  All ${#NODE_LIST[@]} nodes are UP and NORMAL."
    sleep 20  # Give some time for the cluster to stabilize
}

# Stop Cassandra on all nodes
stop_cassandra_cluster() {
    echo "Stopping Cassandra on all nodes..."
    # Sometimes Cassandra does not stop after one killall, so we try again
    run_on_nodes 'killall -w java || true; sleep 1; killall -w java || true'
}

cassandra_disable_autocompaction() {
    # Disable auto compaction before loading. 
    # NOTE that disableautocompaction is a global setting, so it will affect all keyspaces which 
    # are present on the node at the time of running this command. Hence this needs to be run 
    # after creating our required keyspaces.

    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    echo "Disabling cassandra auto compaction ..."
    if [[ "$is_remote" == true ]]; then
        run_on_nodes "$nodetool_bin disableautocompaction" || { echo "Failed to disable auto compaction on nodes."; exit 1; }
    else
        $nodetool_bin disableautocompaction || { echo "Failed to disable auto compaction on local node."; exit 1; }
    fi
}

cassandra_enable_autocompaction() {
    # Enable auto compaction after loading. 
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    echo "Enabling cassandra auto compaction ..."
    if [[ "$is_remote" == true ]]; then
        run_on_nodes "$nodetool_bin enableautocompaction" || { echo "Failed to enable auto compaction on nodes."; exit 1; }
    else
        $nodetool_bin enableautocompaction || { echo "Failed to enable auto compaction on local node."; exit 1; }
    fi
}

cassandra_wait_for_background_compactions() {
    # Wait for all background compactions to finish
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"
    local sleep_interval=30
    local start_time=$(date +%s)
    
    echo "Waiting for background compactions to finish ..."
    if [[ "$is_remote" == true ]]; then
        # For multinode, check compaction status on all nodes individually
        while true; do
            all_done=true
            pending_str=""
            for node in "${NODE_LIST[@]}"; do
                pending=$(ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$node" "$nodetool_bin compactionstats -H" | awk '/pending tasks/ {print $3}')
                if [[ -z "$pending" ]]; then
                    pending="?"
                fi
                pending_str+="[$node: $pending] "
                if [[ "$pending" != "0" && "$pending" != "?" ]]; then
                    all_done=false
                fi
            done

            if [[ "$all_done" == true ]]; then
                echo "All compactions finished on all nodes."
                break
            fi

            printf "Waiting: pending=%s ...\r" "$pending_str"
            sleep $sleep_interval
        done
    else
        while true; do
            # grab the pending count
            pending=$("$nodetool_bin" compactionstats -H | awk '/pending tasks:/ {print $3}')

            # sanity check our parsing
            if [[ -z "$pending" ]]; then
                echo "Failed to parse pending tasks. Retrying…" >&2
                sleep 5
                continue
            fi

            # done when no more pending tasks
            if (( pending == 0 )); then
                echo "All compactions finished."
                break
            fi

            # otherwise show the status and wait
            printf "Waiting: pending=%s ...\r" "$pending"
            sleep $sleep_interval
        done
    fi
    local end_time=$(date +%s)
    local elapsed=$(( end_time - start_time ))
    echo "Total wait time for compactions: ${elapsed}s"
}

cassandra_force_major_compaction() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"
    echo "Forcing major compaction ..."
    if [[ "$is_remote" == true ]]; then
        run_on_nodes "$nodetool_bin compact && sleep 60"
    else
        $nodetool_bin compact && sleep 60
    fi
}

cassandra_flush() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"
    echo "Flushing Cassandra data ..."
    if [[ "$is_remote" == true ]]; then
        run_on_nodes "$nodetool_bin flush && sleep 5"
    else
        $nodetool_bin flush && sleep 5
    fi
}

# Function to enable or disable WAL (durable_writes) on a Cassandra keyspace
set_cassandra_wal() {
    local is_remote="$1"
    local keyspace="$2"
    local action="$3"  # "enable" or "disable"
    local cqlsh_bin="$CASSANDRA_HOME_DIR/bin/cqlsh"

    if [ -z "$keyspace" ] || { [ "$action" != "enable" ] && [ "$action" != "disable" ]; }; then
        echo "Error: Usage: set_cassandra_wal <is_remote> <keyspace> <enable|disable>" >&2
        return 1
    fi

    local durable_writes="true"
    if [ "$action" = "disable" ]; then
        durable_writes="false"
    fi

    local cql_cmd="SELECT * FROM system_schema.keyspaces WHERE keyspace_name='$keyspace'; ALTER KEYSPACE $keyspace WITH durable_writes = $durable_writes; SELECT * FROM system_schema.keyspaces WHERE keyspace_name='$keyspace';"

    if [[ "$is_remote" == true ]]; then
        run_on_nodes " $cqlsh_bin -e \"$cql_cmd\" ; sleep 2 "
    else
        $cqlsh_bin -e "$cql_cmd"
        sleep 2
    fi
}

###############################################
# Function: cassandra_save_tokens_to_file
#
# Description:
#   Dumps this node’s token list (from system.local) to a simple 2-line file:
#     Line 1: <num_tokens>
#     Line 2: <token1,token2,...>   (comma-separated, no spaces)
#
#   Supports local mode and remote mode (executes the whole routine on each
#   remote via your run_on_nodes helper). Uses python3 to safely parse CQL set.
#
# Args:
#   $1  is_remote   - "true" to run through run_on_nodes, anything else = local
#   $2  output_file - Absolute or relative path to write (created atomically)
#
# Requirements:
#   - $CASSANDRA_HOME_DIR set so that $CASSANDRA_HOME_DIR/bin/cqlsh exists
#   - python3 on the target host(s)
#   - run_on_nodes defined if is_remote=true
#
# Exit codes:
#   0 on success; non-zero on error (prints message to stderr)
#
# Example (local):
#   cassandra_save_tokens_to_file false /var/lib/cassandra/tokens-10.10.1.15.txt
#
# Example (remote to all nodes in your helper’s inventory):
#   cassandra_save_tokens_to_file true /var/lib/cassandra/tokens-$(hostname -I | awk '{print $1}' | tr . _).txt
###############################################
cassandra_save_tokens_to_file() {
    local is_remote="$1"        # true|false
    local output_file="$2"      # where to write the 2-line file

    if [[ -z "$output_file" ]]; then
        echo "cassandra_save_tokens_to_file: Error: output_file is not set." >&2
        return 1
    fi
    if [[ -z "$CASSANDRA_HOME_DIR" ]]; then
        echo "cassandra_save_tokens_to_file: Error: CASSANDRA_HOME_DIR is not set." >&2
        return 1
    fi

    local cqlsh_bin="$CASSANDRA_HOME_DIR/bin/cqlsh"
    if [[ ! -x "$cqlsh_bin" ]]; then
        echo "Error: cqlsh not found or not executable at: $cqlsh_bin" >&2
        return 1
    fi

    # awk filter: drop header/separators; emit the first data row only
    local awk_filter='/^-+/{next} /tokens/{next} NF{print; exit}'

    echo "Saving cassandra tokens to $output_file (is_remote=$is_remote) ..."

    if [[ "$is_remote" == true ]]; then
        # Entire capture+write runs on each remote host
        run_on_nodes "
            set -euo pipefail
            cqlsh_bin=\"$cqlsh_bin\"
            out=\"$output_file\"
            mkdir -p \"\$(dirname \"\$out\")\"

            RAW=\$("\$cqlsh_bin" --no-color -e \"SELECT tokens FROM system.local;\" 2>/dev/null | awk '$awk_filter') || true
            if [ -z \"\$RAW\" ]; then
                echo 'ERROR: could not read tokens from system.local' >&2
                exit 2
            fi

            # Use env to pass RAW safely into python
            TOK_TMP=\"\$out.tmp.\$\$\"
            env TOKENS_RAW=\"\$RAW\" python3 - \"\$TOK_TMP\" <<'PY'
import os, sys, ast
raw = os.environ.get('TOKENS_RAW','').strip()
if not raw:
    print('ERROR: empty tokens', file=sys.stderr)
    sys.exit(3)
# raw looks like: {'-1137', '456', ...}
try:
    toks = sorted(ast.literal_eval(raw), key=int)
except Exception as e:
    print('ERROR: failed to parse tokens:', e, file=sys.stderr)
    sys.exit(4)
csv = ','.join(toks)
with open(sys.argv[1], 'w') as f:
    f.write(str(len(toks)) + '\\n')
    f.write(csv + '\\n')
PY

            chmod 0640 \"\$TOK_TMP\"
            mv -f \"\$TOK_TMP\" \"\$out\"
            echo \"Tokens saved to \$out\"
        " || return $?
    else
        # Local mode
        local RAW
        RAW=$("$cqlsh_bin" --no-color -e "SELECT tokens FROM system.local;" 2>/dev/null | awk "$awk_filter") || true
        if [[ -z "$RAW" ]]; then
            echo "ERROR: could not read tokens from system.local" >&2
            return 2
        fi

        mkdir -p "$(dirname "$output_file")"
        local tmp="$output_file.tmp.$$"
        env TOKENS_RAW="$RAW" python3 - "$tmp" <<'PY'
import os, sys, ast
raw = os.environ.get('TOKENS_RAW','').strip()
if not raw:
    print('ERROR: empty tokens', file=sys.stderr)
    sys.exit(3)
try:
    toks = sorted(ast.literal_eval(raw), key=int)
except Exception as e:
    print('ERROR: failed to parse tokens:', e, file=sys.stderr)
    sys.exit(4)
csv = ','.join(toks)
with open(sys.argv[1], 'w') as f:
    f.write(str(len(toks)) + '\n')
    f.write(csv + '\n')
PY
        local rc=$?
        if [[ $rc -ne 0 ]]; then
            rm -f "$tmp" 2>/dev/null || true
            return $rc
        fi
        chmod 0640 "$tmp"
        mv -f "$tmp" "$output_file"
        echo "Tokens saved to $output_file"
    fi
}

###############################################
# get_tokens_assoc_from_file <out_assoc_name> <tokens_file> [node_ip]
#
# Reads a saved 2-line tokens file and populates an associative array:
#   [num_tokens]    -> integer count
#   [initial_token] -> comma-separated tokens (no spaces)
#
# If node_ip is provided, the file is read on that node via SSH.
#
# File format:
#   Line 1: <N>
#   Line 2: <t1,t2,...,tN>
#
# Returns: 0 on success; non-zero on error (stderr explains).
###############################################
get_tokens_assoc_from_file() {
  local out_arr_name="$1"
  local tokens_file="$2"
  local node_ip="${3:-}"

  [[ -z "$out_arr_name" || -z "$tokens_file" ]] && {
    echo "get_tokens_assoc_from_file: usage: <out_assoc_name> <tokens_file> [node_ip]" >&2
    return 1
  }

  local n csv
  if [[ -n "$node_ip" ]]; then
    # Remote read
    local ssh_opts='-o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no'
    ssh $ssh_opts "$node_ip" "[ -f '$tokens_file' ]" || {
      echo "get_tokens_assoc_from_file: remote file not found on $node_ip: $tokens_file" >&2
      return 1
    }
    n=$(ssh $ssh_opts "$node_ip" "head -n 1 '$tokens_file' | tr -d '\r\n'") || {
      echo "get_tokens_assoc_from_file: failed to read line 1 from $tokens_file on $node_ip" >&2
      return 1
    }
    csv=$(ssh $ssh_opts "$node_ip" "sed -n '2p' '$tokens_file' | tr -d '[:space:]\r'") || {
      echo "get_tokens_assoc_from_file: failed to read line 2 from $tokens_file on $node_ip" >&2
      return 1
    }
  else
    # Local read
    [[ -f "$tokens_file" ]] || {
      echo "get_tokens_assoc_from_file: file not found: $tokens_file" >&2
      return 1
    }
    read -r n < "$tokens_file" || {
      echo "get_tokens_assoc_from_file: cannot read num_tokens (line 1) from $tokens_file" >&2
      return 1
    }
    csv=$(sed -n '2p' "$tokens_file" | tr -d '[:space:]') || {
      echo "get_tokens_assoc_from_file: cannot read tokens CSV (line 2) from $tokens_file" >&2
      return 1
    }
  fi

  [[ -n "$csv" ]] || { echo "get_tokens_assoc_from_file: missing tokens CSV on line 2" >&2; return 1; }
  [[ "$n" =~ ^[0-9]+$ ]] || { echo "get_tokens_assoc_from_file: bad num_tokens on line 1: '$n'" >&2; return 1; }

  # Count CSV entries; reconcile with declared count
  local actual_count
  actual_count=$(awk -F',' '{print NF}' <<<"$csv")
  if [[ "$actual_count" -ne "$n" ]]; then
    echo "get_tokens_assoc_from_file: warn: num_tokens ($n) != CSV count ($actual_count); using CSV count" >&2
    n="$actual_count"
  fi

  # Populate caller's assoc array
  declare -gA "$out_arr_name" 2>/dev/null || true
  declare -n __out="$out_arr_name"
  __out[num_tokens]="$n"
  __out[initial_token]="$csv"
  return 0
}

cassandra_prune_system_for_clone() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local data_dir="$2"    # Path to Cassandra data directory (e.g., /var/lib/cassandra/data)

    if [ -z "$data_dir" ]; then
        echo "Error: data_dir is not set." >&2
        return 1
    fi

    echo "Pruning Cassandra system keyspaces in $data_dir (is_remote=$is_remote) ..."
    
    # TODO: This block is commented as of now because other short-running monitoring processes like "watch nodetool status" or "watch nodetool compactionstats" also use java.
    #       If you want to enforce this check, consider checking for the Cassandra process specifically instead of any java process.
    # Fail if cassandra is running
    # if [[ "$is_remote" == true ]]; then
    #     if ! run_on_nodes 'if pgrep -x java >/dev/null; then echo "ERROR: java running on this node" >&2; exit 1; fi'; then
    #         echo "Cassandra appears to be running on one or more remote nodes (java process detected). Please stop it first." >&2
    #         return 1
    #     fi
    # else
    #     if pgrep -x java >/dev/null; then
    #         echo "ERROR: Cassandra appears to be running on the local node (java process detected). Please stop it first." >&2
    #         return 1
    #     fi
    # fi

    local delete_str="cd $data_dir && rm -rf data/system/local-* data/system/peers-* data/system/peers_v2-* commitlog/* hints/* saved_caches/* data/system_traces data/system_distributed/* "
    if [[ "$is_remote" == true ]]; then
        run_on_nodes "$delete_str" || {
            echo "ERROR: Failed to prune system keyspaces on remote nodes." >&2
            return 1
        }
    else
        eval "$delete_str" || {
            echo "ERROR: Failed to prune system keyspaces on local node." >&2
            return 1
        }
    fi
    echo "Cassandra system keyspaces pruned successfully."
}


cassandra_get_total_unique_rows_approx() {
    local is_remote="$1"     # "true" for remote, "false" for local
    local keyspace="$2"      # keyspace name (not table)
    local replication="$3"   # replication factor (integer)
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"

    if [ -z "$is_remote" ] || [ -z "$keyspace" ] || [ -z "$replication" ]; then
        echo "Usage: cassandra_get_total_unique_rows_approx <is_remote> <keyspace> <replication_factor>" >&2
        return 1
    fi

    local -a output_arr=()
    if [[ "$is_remote" == true ]]; then
        for node in "${NODE_LIST[@]}"; do
            local out
            out=$(ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$node" "$nodetool_bin tablestats $keyspace | grep 'Number of partitions (estimate):'" || true)
            output_arr+=("$out")
        done
    else
        local out
        out=$("$nodetool_bin" tablestats "$keyspace" | grep 'Number of partitions (estimate):' || true)
        output_arr+=("$out")
    fi

    # Print the raw output for debugging
    # echo "DEBUG: output from nodetool tablestats: ${output_arr[*]}" >&2

    local total=0
    for output in "${output_arr[@]}"; do
        local count_lines
        count_lines=$(echo "$output" | grep -c 'Number of partitions (estimate):')
        if [ "$count_lines" -ne 1 ]; then
            echo "Error: The supplied keyspace has more than one table, or output is ambiguous. Please specify a keyspace with only one table. (Found $count_lines occurrences of 'Number of partitions (estimate):')" >&2
            return 1
        fi
        local num
        num=$(echo "$output" | grep -oE '[0-9]+$')
        if [[ ! "$num" =~ ^[0-9]+$ ]]; then
            echo "Error: Could not parse partition count." >&2
            return 1
        fi
        total=$((total + num))
    done

    local approx_unique
    if [ "$replication" -gt 0 ]; then
        approx_unique=$((total / replication))
    else
        approx_unique="$total"
    fi

    echo "$approx_unique"
}

cassandra_check_unique_rows() {
    local is_multinode="$1"
    local keyspace="$2"
    local repl_factor="$3"
    local expected_row_count="$4"

    echo "Checking if the number of unique rows inserted is as expected ..."
    unique_rows=$(cassandra_get_total_unique_rows_approx "$is_multinode" "$keyspace" "$repl_factor")
    percent_threshold=2  # Allowable percent difference
    percent_diff=$((100 * (unique_rows - expected_row_count) / expected_row_count))
    if (( percent_diff < 0 )); then
        percent_diff=$(( -percent_diff ))
    fi
    echo "Approx total unique rows in $keyspace.keyspace: $unique_rows , expected: $expected_row_count, percent_diff=$percent_diff% (threshold=$percent_threshold%)"
    if (( percent_diff > percent_threshold )); then
        echo "Error: Number of unique rows ($unique_rows) differs from expected ($expected_row_count) by more than $percent_threshold%. percent_diff=$percent_diff% . Exiting."
        return 1
    fi
    echo "Number of unique rows is within the acceptable range."
}

###############################################################################

#Function to disable speculative_retry
set_speculative_retry(){
    local is_remote="$1"
    local keyspace="$2"
    local tablename="$3"
    local action="$4"  # "enable" or "disable"
    local cqlsh_bin="$CASSANDRA_HOME_DIR/bin/cqlsh"

    if [ -z "$keyspace" ] || { [ "$action" != "enable" ] && [ "$action" != "disable" ]; }; then
        echo "Error: Usage: set_cassandra_speculative_retry <is_remote> <keyspace> <tablename> <enable|disable>" >&2
        return 1
    fi

    local spec_retry="99PERCENTILE"
    if [ "$action" = "disable" ]; then
        spec_retry="NONE"
    fi

    local cql_cmd="SELECT table_name, speculative_retry FROM system_schema.tables WHERE keyspace_name='$keyspace' AND table_name='$tablename'; ALTER TABLE $keyspace.$tablename WITH speculative_retry='$spec_retry'; SELECT table_name, speculative_retry FROM system_schema.tables WHERE keyspace_name='$keyspace' AND table_name='$tablename';"

    if [[ "$is_remote" == true ]]; then
        run_on_nodes "CQLSH_PYTHON=/usr/bin/python3.9 $cqlsh_bin -e \"$cql_cmd\" ; sleep 2 "
    else
        $cqlsh_bin -e "$cql_cmd"
        sleep 2
    fi
}




#############################
# Check for pure cgroup v2
#############################
check_pure_cgroupv2() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation

    # If this is a remote operation, we need to run the checks on each node
    if [ "$is_remote" = "true" ]; then
        run_on_nodes "
            if mount | grep -q '^cgroup2 on /sys/fs/cgroup ' && ! mount | grep -q '^cgroup on /sys/fs/cgroup/'; then
                echo 'OK: Pure cgroup v2 environment detected. No cgroup v1 mounts.'
                exit 0
            else
                echo 'ERROR: Not a pure cgroup v2 environment.'
                exit 1
            fi
        " || {
            echo "ERROR: Failed to check cgroup v2 on remote nodes." >&2
            exit 1
        }
    else
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
    fi
    echo "check_pure_cgroupv2: OK: Pure cgroup v2 environment detected. No cgroup v1 mounts."
}

remove_cgroup() {  # TODO: Check where to call this if you want to remove cgroup 
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local cgroup_name="$2"  # Path to the cgroup to be removed

    if [ -z "$cgroup_name" ]; then
        echo "Error: cgroup_name is not set." >&2
        return 1
    fi

    local cgroup_path="/sys/fs/cgroup/$cgroup_name"

    # If this is a remote operation, we need to run the command on each node
    if [ "$is_remote" = "true" ]; then
        run_on_nodes " sudo rmdir $cgroup_path " || {
            echo "ERROR: Failed to remove cgroup $cgroup_name on remote nodes." >&2
            return 1
        }
    else
        sudo rmdir "$cgroup_path" || {
            echo "ERROR: Failed to remove cgroup $cgroup_name on local node." >&2
            return 1
        }
    fi

    echo "croup with name: $cgroup_name removed successfully."
}

get_device_number_from_path() {
    local path="$1"
    local node_ip="$2"  # Optional, if you want to run this on a specific remote node

    if [ -z "$path" ]; then
        echo "Error: Missing arguments path to get_device_number_from_path" >&2
        exit 1
    fi
    # cg_dev <directory>  ➜  MAJ:MIN of the queue-owning device
    local cmd='
        dev=$(findmnt -T "'"$path"'" -no SOURCE) || exit 1
        while [ "$(lsblk -ndo TYPE "$dev")" = "part" ]; do
            dev="/dev/$(lsblk -ndo PKNAME "$dev")" || exit 1
        done
        lsblk -ndo MAJ:MIN "$dev"
    '

    local device_number

    # If node_ip is provided, run the command on that node
    if [ -n "$node_ip" ]; then
        device_number=$(ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$node_ip" "$cmd")
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to get device number from path '$path' on node '$node_ip'." >&2
            exit 1
        fi
    else
        # Run the command locally
        local device_number
        device_number=$(eval "$cmd")
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to get device number from path '$path'." >&2
            exit 1
        fi
    fi

    if [ -z "$device_number" ]; then
        echo "ERROR: No device number found for path '$path' and node_ip: $node_ip." >&2
        exit 1
    fi

    echo "$device_number"
}

get_cgroupv2_str_prefix() {
    local data_dir="$1"  # Path to the data directory
    local node_ip="$2"  # Optional, if you want to run this on a specific remote node

    local cgroup_name=$CGROUPV2_CGROUP_NAME
    local rbps=$CGROUPV2_RBPS
    local wbps=$CGROUPV2_WBPS
    local riops=$CGROUPV2_RIOPS
    local wiops=$CGROUPV2_WIOPS

    local data_dir_device_number
    if [ -n "$node_ip" ]; then
        # If node_ip is provided, run the command on that node
        data_dir_device_number=$(get_device_number_from_path "$data_dir" "$node_ip")
    else
        # Run the command locally
        data_dir_device_number=$(get_device_number_from_path "$data_dir")
    fi

    if [ -z "$data_dir_device_number" ]; then
        echo "ERROR: Failed to get device number for data directory '$data_dir'." >&2
        exit 1
    fi

    # Construct the cgroup v2 string prefix
    local cgroupv2_str_prefix="$cgroup_name $data_dir_device_number $rbps $wbps $riops $wiops -- "
    echo "$cgroupv2_str_prefix"
}

###############################################################################
# validate_readahead_or_die
#
# PURPOSE
#   Guard against the “large-readahead composite device” pitfall:  if the
#   block-device stack underneath a given file / directory contains **any**
#   composite layer (md-raid, LVM striped LV, dm-crypt, etc.) whose
#   `read_ahead_kb` value exceeds that of one of its immediate child
#   (-slave) devices, the function prints a descriptive error and exits 1.
#
# USAGE
#   validate_readahead_or_die <path> [<remote_ip>]
#
#     <path>       A file or directory whose underlying device tree you
#                  want to validate (e.g. /var/lib/cassandra).
#     <remote_ip>  Optional.  If supplied, the checks run on that host via
#                  password-less SSH; useful inside orchestration scripts.
#
# RETURN VALUE
#   0  → All composite devices have readahead ≤ their leaves (safe).
#   1  → Mismatch detected **or** an error occurred (abort workload).
#
# DEPENDENCIES
#   * GNU coreutils (findmnt, lsblk, cat, ls)
#   * Bash 4 arrays
#   * Optional SSH access when <remote_ip> is used.
#
# IMPLEMENTATION NOTES
#   • Completely **iterative** (no recursion) for easy maintenance:
#       – Uses a FIFO array `devices_to_check` to walk `/sys/block/<dev>/slaves`.
#   • Treats partitions specially: walks up to the queue-owning parent
#     (nvme0n1, sda, etc.) so we validate the true I/O queue.
#   • No magic thresholds – we only enforce *consistency* between layers.
#   • If desired, you can tighten the policy by adding an absolute cap:
#       e.g. `(( ra_current > 512 )) && bad=1`.
###############################################################################
validate_readahead_or_die() {
    local path="$1"        # Target file/directory.
    local node_ip="$2"     # Optional: remote host for SSH.

    #------------------------------ sanity ---------------------------------
    [[ -z $path ]] && {
        echo "validate_readahead_or_die: missing argument <path>" >&2
        return 1
    }

    # If path does not exist (locally or remotely), echo a message and create it.
    if [[ -n $node_ip ]]; then
        ssh -o ConnectTimeout=3 -o BatchMode=yes \
            -o StrictHostKeyChecking=no "$node_ip" "[ -e '$path' ] || (echo \"validate_readahead_or_die: '$path' does not exist on node '$node_ip', creating it.\" >&2; mkdir -p '$path')" || {
            echo "validate_readahead_or_die: ERROR: Failed to create '$path' on node '$node_ip'" >&2
            return 1
        }
    else
        if [[ ! -e $path ]]; then
            echo "validate_readahead_or_die: '$path' does not exist, creating it." >&2
            mkdir -p "$path" || {
                echo "validate_readahead_or_die: ERROR: Failed to create '$path'" >&2
                return 1
            }
        fi
    fi

    # _run CMD …  → execute locally or over ssh (batch/no-prompt).
    _run() {
        if [[ -n $node_ip ]]; then
            ssh -o ConnectTimeout=3 -o BatchMode=yes \
                -o StrictHostKeyChecking=no "$node_ip" "$@"
        else
            eval "$@"
        fi
    }

    # Helper: map a device basename to the queue owner (handles partitions)
    _queue_owner() {
        local d="$1"
        local t
        t=$(_run lsblk -ndo TYPE "/dev/$d" 2>/dev/null || echo "")
        if [[ "$t" == "part" ]]; then
            _run lsblk -ndo PKNAME "/dev/$d"
        else
            echo "$d"
        fi
    }

    _read_ra_kb() {
        local q="$1"   # queue owner basename
        _run cat "/sys/block/$q/queue/read_ahead_kb" 2>/dev/null || echo 0
    }

    #--------------------------- resolve device ----------------------------
    # 1. Find the source device for the mount containing <path>.
    # 2. Strip off any partition layer so we land on the queue owner.
    local dev
    dev=$(_run 'findmnt -T "'"$path"'" -no SOURCE') || return 1
    while [[ $(_run lsblk -ndo TYPE "$dev") == "part" ]]; do
        dev="/dev/$(_run lsblk -ndo PKNAME "$dev")"
    done
    dev="$(basename "$dev")"        # e.g. dm-0, md0, nvme0n1

    #-------------------- iterative breadth-first walk ---------------------
    local -a devices_to_check=("$dev")   # FIFO queue.
    local bad=0                          # Flag any mismatch.

    while ((${#devices_to_check[@]})); do
        # Pop first element.
        local current="${devices_to_check[0]}"
        devices_to_check=("${devices_to_check[@]:1}")

        # Always read RA from the queue owner (partitions map to parent)
        local qcur ra_current
        qcur=$(_queue_owner "$current")
        ra_current=$(_read_ra_kb "$qcur")

        # Immediate children (empty if leaf device).
        local slaves
        slaves=$(_run 'ls -A /sys/block/'"$current"'/slaves 2>/dev/null' || true)

        for s in $slaves; do
            local qslave ra_slave
            qslave=$(_queue_owner "$s")
            ra_slave=$(_read_ra_kb "$qslave")

            # Flag inconsistency: parent RA larger than child’s RA.
            if (( ra_current > ra_slave )); then
                echo "ERROR: $current read_ahead_kb=$ra_current > "\
"$s read_ahead_kb=$ra_slave" >&2
                bad=1
            fi
            # Enqueue slave for further walk-down.
            devices_to_check+=("$s")
        done
    done

    if (( bad )); then
        echo "validate_readahead_or_die: Aborting: composite readahead mismatch detected under '$path'" >&2
        return 1
    fi
    echo "validate_readahead_or_die: OK: readahead values consistent under '$path' , node_ip: $node_ip"
}


cassandra_force_repair() {
    local is_remote="$1"  # Optional argument to indicate if this is a remote operation
    local nodetool_bin="$CASSANDRA_HOME_DIR/bin/nodetool"
    echo "Starting cassandra repair ..."

     # I have observed that this errors when --ignore-unreplicated-keyspaces is not supplied.
     # It works with --full not specified, but it is better to use --full to ensure all data is repaired.
     # -j 4 is the max threads that can be used for repair, as per the Cassandra documentation.
    local repair_str=" -j 4 --ignore-unreplicated-keyspaces --full"
    # local repair_str=" -j 4 --ignore-unreplicated-keyspaces "

    local repair_start_time=$(date +%s)

    if [[ "$is_remote" == true ]]; then
        for node in "${NODE_LIST[@]}"; do
            echo "Repairing node: $node"
            ssh -o ConnectTimeout=0 -o BatchMode=yes -o StrictHostKeyChecking=no "$node" "$nodetool_bin repair $repair_str && sleep 5" || {
                echo "❌  Repair failed on node: $node"
                return 1
            }
        done
    else
        $nodetool_bin repair $repair_str && sleep 5
    fi

    local repair_end_time=$(date +%s)
    repair_elapsed_time=$((repair_end_time - repair_start_time))
    echo "Cassandra Repair Time taken: ${repair_elapsed_time} seconds"
}

# Disable ALL THP paths (anon, defrag, khugepaged, shmem/tmpfs, zero page),
# then run the checker. Works on modern Linux (CentOS/RHEL/Alma/Rocky/Ubuntu/Debian).
# Usage: thp_disable_all true|false
thp_disable_all() {
  local is_remote="${1:-false}"

  local CMD='bash -lc "
    set -e
    if [[ \$(uname -s) != Linux ]]; then echo \"ERROR: Not Linux\" >&2; exit 2; fi
    source /etc/os-release 2>/dev/null || true
    echo \"OS: \${NAME:-unknown} \${VERSION_ID:-}\" >&2

    base=/sys/kernel/mm/transparent_hugepage
    [[ -d \$base ]] || base=/sys/kernel/mm/redhat_transparent_hugepage
    if [[ ! -d \$base ]]; then echo \"ERROR: THP sysfs not found\" >&2; exit 2; fi

    echo \"Disabling THP under \$base ...\"

    # 1) Anonymous THP off + no direct defrag stalls
    echo never | sudo tee \$base/enabled >/dev/null
    echo never | sudo tee \$base/defrag  >/dev/null || true

    # 2) Shmem/tmpfs THP off (memfd, SysV SHM, /dev/zero MAP_SHARED, etc.)
    [[ -e \$base/shmem_enabled ]] && echo never | sudo tee \$base/shmem_enabled >/dev/null || true

    # 3) Stop background collapsing/compaction daemon
    [[ -e \$base/khugepaged/run    ]] && echo 0 | sudo tee \$base/khugepaged/run    >/dev/null || true
    [[ -e \$base/khugepaged/defrag ]] && echo 0 | sudo tee \$base/khugepaged/defrag >/dev/null || true
    # (Do NOT write pages_to_scan=0; many kernels reject it.)

    # 4) Don’t map the shared huge zero page (if exposed)
    [[ -e \$base/use_zero_page ]] && echo 0 | sudo tee \$base/use_zero_page >/dev/null || true

    # 5) Per-mount tmpfs policy: /dev/shm -> huge=never
    if mountpoint -q /dev/shm; then
      sudo mount -o remount,huge=never /dev/shm || true
    fi
  "'
  
  echo "Disabling THP ..."
  if [[ "$is_remote" == true ]]; then
    run_on_nodes "$CMD" || { echo "Failed to disable THP on one or more nodes."; return 1; }
  else
    eval "$CMD" || { echo "Failed to disable THP on local node."; return 1; }
  fi
  echo "THP disabled. Verifying ..."
  thp_check_all "$is_remote" || { echo "THP check failed after disable."; return 1; }
  echo "THP disable and verification complete."
}

# Verify EVERYTHING THP-related is off. Exit 0 = off; non-zero = not fully off.
# Usage: thp_check_all true|false
thp_check_all() {
  local is_remote="${1:-false}"

  local CHECK='bash -lc "
    fail=0
    if [[ \$(uname -s) != Linux ]]; then echo \"ERROR: Not Linux\" >&2; exit 2; fi
    base=/sys/kernel/mm/transparent_hugepage
    [[ -d \$base ]] || base=/sys/kernel/mm/redhat_transparent_hugepage
    if [[ ! -d \$base ]]; then echo \"ERROR: THP sysfs not found\" >&2; exit 2; fi

    en=\$(cat \$base/enabled 2>/dev/null || echo \"\")
    df=\$(cat \$base/defrag  2>/dev/null || echo \"\")
    sh=\$(cat \$base/shmem_enabled 2>/dev/null || echo \"N/A\")
    uz=\$(cat \$base/use_zero_page 2>/dev/null || echo \"N/A\")
    khr=\$(cat \$base/khugepaged/run 2>/dev/null || echo \"N/A\")
    khd=\$(cat \$base/khugepaged/defrag 2>/dev/null || echo \"N/A\")
    shm_mount=\$(grep -w \" /dev/shm \" /proc/mounts || true)

    echo \"enabled:           \$en\"
    echo \"defrag:            \$df\"
    echo \"shmem_enabled:     \$sh\"
    echo \"use_zero_page:     \$uz\"
    echo \"khugepaged/run:    \$khr\"
    echo \"khugepaged/defrag: \$khd\"
    [[ -n \$shm_mount ]] && echo \"mount /dev/shm:   \$shm_mount\" || echo \"/dev/shm not mounted\"
    echo

    # 1) Anonymous THP policy
    if grep -q \"\\[never\\]\" <<<\"\$en\"; then
      echo \"[OK] enabled = never\"
    else
      echo \"[FAIL] enabled not never -> \$en\"; fail=1
    fi

    # 2) Fault-path defrag
    if grep -q \"\\[never\\]\" <<<\"\$df\"; then
      echo \"[OK] defrag = never\"
    else
      # Some kernels ignore defrag when enabled=never; accept that as OK.
      if grep -q \"\\[never\\]\" <<<\"\$en\"; then
        echo \"[OK] defrag inert (enabled=never) -> \$df\"
      else
        echo \"[FAIL] defrag not never -> \$df\"; fail=1
      fi
    fi

    # 3) Shmem/tmpfs policy
    if [[ \"\$sh\" != \"N/A\" ]]; then
      if grep -q \"\\[never\\]\" <<<\"\$sh\" || [[ \"\$sh\" == never ]]; then
        echo \"[OK] shmem_enabled = never\"
      else
        echo \"[FAIL] shmem_enabled not never -> \$sh\"; fail=1
      fi
    else
      echo \"[SKIP] shmem_enabled not present\"
    fi

    # 4) Zero page
    if [[ \"\$uz\" == 0 || \"\$uz\" == \"N/A\" ]]; then
      echo \"[OK] use_zero_page = 0 (or unsupported)\"
    else
      echo \"[FAIL] use_zero_page != 0 -> \$uz\"; fail=1
    fi

    # 5) khugepaged fully stopped
    if [[ \"\$khr\" == 0 || \"\$khr\" == \"N/A\" ]]; then
      echo \"[OK] khugepaged/run = 0 (or unsupported)\"
    else
      echo \"[FAIL] khugepaged/run != 0 -> \$khr\"; fail=1
    fi
    if [[ \"\$khd\" == 0 || \"\$khd\" == \"N/A\" ]]; then
      echo \"[OK] khugepaged/defrag = 0 (or unsupported)\"
    else
      echo \"[FAIL] khugepaged/defrag != 0 -> \$khd\"; fail=1
    fi

    # 6) /dev/shm mount policy
    if [[ -n \$shm_mount ]]; then
      if grep -qw \"huge=never\" <<<\"\$shm_mount\"; then
        echo \"[OK] /dev/shm mounted with huge=never\"
      else
        echo \"[FAIL] /dev/shm missing huge=never\"; fail=1
      fi
    else
      echo \"[OK] /dev/shm not mounted (nothing to fix)\"
    fi

    echo
    echo \"Counters (should not grow during your run):\"
    grep -E \"thp_(fault|collapse)_(alloc|failed)|thp_file_(alloc|mmap|mapped)\" /proc/vmstat || true
    grep -E \"AnonHugePages|ShmemHugePages|ShmemPmdMapped\" /proc/meminfo || true
    echo

    exit \$fail
  "'

  if [[ "$is_remote" == true ]]; then
    run_on_nodes "$CHECK" || { echo "THP check failed on one or more nodes."; return 1; }
  else
    eval "$CHECK" || { echo "THP check failed on local node."; return 1; }
  fi
  echo "THP check passed: all relevant THP paths appear disabled."
  return 0
}


#### AWS related functions ####
# Transfer file/folder to S3 bucket
upload_to_s3() {
    local source_path="$1"
    local s3_bucket="$2"
    local s3_path="$3"  # Optional, path within the bucket

    if [ -z "$source_path" ] || [ -z "$s3_bucket" ]; then
        echo "Usage: upload_to_s3 <source_path> <s3_bucket> [s3_path]" >&2
        return 1
    fi

    if [ ! -e "$source_path" ]; then
        echo "Error: Source path '$source_path' does not exist." >&2
        return 1
    fi

    local s3_uri="s3://$s3_bucket"
    if [ -n "$s3_path" ]; then
        s3_uri="$s3_uri/$s3_path"
    fi

    echo "Uploading '$source_path' to '$s3_uri' ..."
    aws s3 cp --recursive "$source_path" "$s3_uri"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to upload '$source_path' to '$s3_uri'." >&2
        return 1
    fi

    echo "Upload completed successfully."
    return 0
}

