#!/bin/bash

# set -e

# Global default parameters for Cassandra YAML updates
# Keys correspond to YAML fields (excluding data_file_directories, commitlog_directory, saved_caches_directory)
# Values are defaults; can be overridden by passing an associative array to the function
declare -A CASSANDRA3_YAML_DEFAULT_PARAMS=(
    [disk_access_mode]="standard"
    [compaction_throughput_mb_per_sec]=0
    [concurrent_compactors]="$(nproc)"
    [memtable_flush_writers]="$(( $(nproc) / 2 ))"
    [batch_size_warn_threshold_in_kb]=50000
    [batch_size_fail_threshold_in_kb]=50000
    [concurrent_reads]=128
    [concurrent_writes]=128
    [read_request_timeout_in_ms]=60000
    [range_request_timeout_in_ms]=60000
    [write_request_timeout_in_ms]=60000
    [counter_write_request_timeout_in_ms]=60000
    [cas_contention_timeout_in_ms]=60000
    [truncate_request_timeout_in_ms]=60000
    [request_timeout_in_ms]=60000
    [slow_query_log_timeout_in_ms]=60000
    [stream_throughput_outbound_megabits_per_sec]=8000
)

declare -A CASSANDRA4_YAML_DEFAULT_PARAMS=(
    # -----------------------------------------------------------------------------
    # auto_bootstrap: false
    #
    # Cassandra 4 enforces stricter bootstrap and range-consistency rules than
    # Cassandra 3. Even if the data directories on all nodes were pre-loaded using
    # RF=1 (each node already holds its own token ranges), Cassandra 4 will still
    # treat a node as a new joiner and force a full bootstrap stream unless
    # auto_bootstrap is explicitly disabled.
    #
    # In Cassandra 3 this “preload RF=1 and start nodes one-by-one” pattern appeared
    # to work without auto_bootstrap=false because 3.x was more permissive: it trusted
    # existing system.local tokens, tolerated mismatches in SSTable metadata, and did
    # not aggressively validate range coverage. As a result, nodes often skipped
    # streaming and came up immediately.
    #
    # Cassandra 4 does not allow that shortcut. On first startup of a non-empty ring,
    # it performs strict validation of token ownership, system tables, and range
    # consistency. If auto_bootstrap is left at the default (true), a newly started
    # node—despite having correct pre-loaded SSTables—will enter JOINING (UJ) and
    # stream large amounts of data from peers as if it were empty.
    #
    # Setting auto_bootstrap=false tells Cassandra 4:
    #   "Do not perform bootstrap streaming; trust the data on disk."
    #
    # This must only be used on the very first startup of a cluster that has been
    # correctly pre-seeded with data and explicitly assigned tokens.
    # -----------------------------------------------------------------------------
    [auto_bootstrap]=false

    [disk_access_mode]="standard"  # This disk_access_mode option is not specified in cassandra 4 yaml, but if you supply it then it works
    [disk_optimization_strategy]="ssd"

    # 4.x: compaction_throughput: “0MiB/s”
    [compaction_throughput]="0MiB/s"

    [concurrent_compactors]="$(nproc)"
    [memtable_flush_writers]="$(( $(nproc) / 2 ))"
    [concurrent_reads]=128
    [concurrent_writes]=128

    [batch_size_warn_threshold]="50000KiB"
    [batch_size_fail_threshold]="50000KiB"

    [read_request_timeout]="60000ms"
    [range_request_timeout]="60000ms"
    [write_request_timeout]="60000ms"
    [counter_write_request_timeout]="60000ms"
    [cas_contention_timeout]="60000ms"
    [truncate_request_timeout]="60000ms"
    [request_timeout]="60000ms"
    [slow_query_log_timeout]="60000ms"

    # 8000 Mbit/s ~= 1000 MiB/s; we’ll just set a high cap
    [stream_throughput_outbound]="1000MiB/s"
)

declare -A CASSANDRA5_YAML_DEFAULT_PARAMS=(
    # -----------------------------------------------------------------------------
    # auto_bootstrap
    #
    # Cassandra 5 keeps Cassandra 4’s strict bootstrap behavior.
    # Any node joining a non-empty ring is treated as a new joiner
    # unless auto_bootstrap=false is explicitly set.
    #
    # Required when:
    #   - Data is pre-seeded on disk
    #   - Tokens are explicitly assigned
    #   - You want to avoid unnecessary streaming
    #
    # MUST ONLY be used on the very first startup of a correctly
    # prepared cluster.
    # -----------------------------------------------------------------------------
    [auto_bootstrap]=false

    # Disk behavior
    # Still accepted in C5 even if not emphasized in docs
    [disk_access_mode]="standard"
    [disk_optimization_strategy]="ssd"

    # Compaction
    [compaction_throughput]="0MiB/s"
    [concurrent_compactors]="$(nproc)"

    # Memtables
    [memtable_flush_writers]="$(( $(nproc) / 2 ))"

    # Concurrency
    # Same tuning model as 4.x
    [concurrent_reads]=128
    [concurrent_writes]=128

    # Batch size thresholds
    [batch_size_warn_threshold]="50000KiB"
    [batch_size_fail_threshold]="50000KiB"

    # Timeouts (explicit units required)
    [read_request_timeout]="60000ms"
    [range_request_timeout]="60000ms"
    [write_request_timeout]="60000ms"
    [counter_write_request_timeout]="60000ms"
    [cas_contention_timeout]="60000ms"
    [truncate_request_timeout]="60000ms"
    [request_timeout]="60000ms"
    [slow_query_log_timeout]="60000ms"

    # Streaming
    # High cap to avoid throttling during bootstrap / repair
    # 1000 MiB/s ~= 8 Gbit/s
    [stream_throughput_outbound]="1000MiB/s"
)

# update_cassandra_yaml_with_yq_uncomment \
#   <yaml_path> <data_dirs_csv> <commitlog_dir> <saved_caches_dir> \
#   <seed_ips> <node_ip> \
#   <base_map_name> <extra_map_name> <preloaded_yaml_map_name>
#
# - base_map_name: name of defaults map (CASSANDRA3_YAML_DEFAULT_PARAMS or CASSANDRA4_YAML_DEFAULT_PARAMS)
# - extra_map_name: overrides on top of base defaults (optional)
# - preloaded_yaml_map_name: last-layer overrides (optional)
update_cassandra_yaml_with_yq_uncomment() {
    local yaml_file="$1" raw_data_dirs="$2" raw_commitlog_dir="$3" raw_saved_caches_dir="$4"
    local seed_ips="${5:-}" node_ip="${6:-}"
    local base_map_name="${7}"          # REQUIRED: e.g. CASSANDRA3_YAML_DEFAULT_PARAMS or CASSANDRA4_YAML_DEFAULT_PARAMS
    local extra_map_name="${8:-}"       # OPTIONAL
    local preloaded_yaml_map_name="${9:-}" # OPTIONAL

    # Validate input file
    [[ ! -f "$yaml_file" ]] && { echo "❌ ERROR: cassandra.yaml not found: $yaml_file"; return 1; }

    # Validate base_map_name exists
    if ! declare -p "$base_map_name" &>/dev/null; then
        echo "❌ ERROR: base defaults map '$base_map_name' is not defined."
        return 1
    fi

    check_or_install_yq

    # Expand and normalize paths
    IFS=',' read -ra data_dirs <<< "$raw_data_dirs"
    local resolved_dirs=()
    for p in "${data_dirs[@]}"; do
        [[ "$p" == ~* ]] && p="${p/#\~/$HOME}"
        resolved_dirs+=( "$(realpath -m "$p")" )
    done
    [[ "$raw_commitlog_dir" == ~* ]] && raw_commitlog_dir="${raw_commitlog_dir/#\~/$HOME}"
    [[ "$raw_saved_caches_dir" == ~* ]] && raw_saved_caches_dir="${raw_saved_caches_dir/#\~/$HOME}"
    local res_commit="$(realpath -m "$raw_commitlog_dir")"
    local res_cache="$(realpath -m "$raw_saved_caches_dir")"

    # Nameref to base defaults
    declare -n base_defaults="$base_map_name"

    # Conflict check: preloaded vs base/extras
    if [[ -n "$preloaded_yaml_map_name" ]]; then
        declare -n preloaded="$preloaded_yaml_map_name"
        for key in "${!preloaded[@]}"; do
            if [[ -n "${base_defaults[$key]+x}" ]]; then
                echo "❌ ERROR: Key '$key' in preloaded map conflicts with base defaults ($base_map_name). Cannot proceed."
                return 1
            fi
            if [[ -n "$extra_map_name" ]]; then
                declare -n extras="$extra_map_name"
                if [[ -n "${extras[$key]+x}" ]]; then
                    echo "❌ ERROR: Key '$key' in preloaded map conflicts with extra overrides ($extra_map_name). Cannot proceed."
                    return 1
                fi
            fi
        done
    fi

    # Merge base defaults + extras + preloaded (base < extra < preloaded)
    declare -A merged
    for key in "${!base_defaults[@]}"; do
        merged[$key]="${base_defaults[$key]}"
    done

    if [[ -n "$extra_map_name" ]]; then
        declare -n extras="$extra_map_name"
        for key in "${!extras[@]}"; do
            merged[$key]="${extras[$key]}"
        done
    fi

    if [[ -n "$preloaded_yaml_map_name" ]]; then
        declare -n preloaded="$preloaded_yaml_map_name"
        for key in "${!preloaded[@]}"; do
            merged[$key]="${preloaded[$key]}"
        done
    fi

    # Build sed command to uncomment fixed paths + all merged keys
    local sed_cmd=(sed -i -E)
    local keys=(data_file_directories commitlog_directory saved_caches_directory)
    for key in "${!merged[@]}"; do
        keys+=("$key")
    done
    for key in "${keys[@]}"; do
        sed_cmd+=(-e "s/^#[[:space:]]*(${key}:)/\\1/")
    done
    "${sed_cmd[@]}" "$yaml_file"

    # Construct the base yq filter for paths and seeds/node overrides
    local df_list='['
    for d in "${resolved_dirs[@]}"; do df_list+="\"$d\","; done
    df_list="${df_list%,}]"

    local filter=".data_file_directories = $df_list |
                  .commitlog_directory = \"$res_commit\" |
                  .saved_caches_directory = \"$res_cache\""

    if [[ -n $seed_ips ]]; then
        filter+=" | .seed_provider[0].parameters[0].seeds = \"$seed_ips\" | .endpoint_snitch = \"GossipingPropertyFileSnitch\" "
    fi

    if [[ -n $node_ip ]]; then
        filter+=" | .listen_address = \"$node_ip\" | .rpc_address = \"0.0.0.0\" | .broadcast_rpc_address = \"$node_ip\""
    fi

    # Append merged params, treating pure numbers/booleans/null as bare; others quoted
    for key in "${!merged[@]}"; do
        local val="${merged[$key]}"
        if [[ "$val" =~ ^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]] || \
           [[ "$val" =~ ^(true|false|yes|no|on|off|null|~)$ ]]; then
            filter+=" | .${key} = ${val}"
        else
            filter+=" | .${key} = \"${val}\""
        fi
    done

    # Apply updates
    yq eval -i "$filter" "$yaml_file"
    echo "✅ cassandra.yaml updated successfully: $yaml_file (base defaults: $base_map_name)"
}

###############################################
# Wrapper: Cassandra 3.x
###############################################
update_cassandra3_yaml_with_yq_uncomment() {
    local yaml_file="$1" raw_data_dirs="$2" raw_commitlog_dir="$3" raw_saved_caches_dir="$4"
    local seed_ips="${5:-}" node_ip="${6:-}" extra_map_name="${7:-}" preloaded_yaml_map_name="${8:-}"

    update_cassandra_yaml_with_yq_uncomment \
        "$yaml_file" \
        "$raw_data_dirs" \
        "$raw_commitlog_dir" \
        "$raw_saved_caches_dir" \
        "$seed_ips" \
        "$node_ip" \
        "CASSANDRA3_YAML_DEFAULT_PARAMS" \
        "$extra_map_name" \
        "$preloaded_yaml_map_name"
}

###############################################
# Wrapper: Cassandra 4.x
###############################################
update_cassandra4_yaml_with_yq_uncomment() {
    local yaml_file="$1" raw_data_dirs="$2" raw_commitlog_dir="$3" raw_saved_caches_dir="$4"
    local seed_ips="${5:-}" node_ip="${6:-}" extra_map_name="${7:-}" preloaded_yaml_map_name="${8:-}"

    update_cassandra_yaml_with_yq_uncomment \
        "$yaml_file" \
        "$raw_data_dirs" \
        "$raw_commitlog_dir" \
        "$raw_saved_caches_dir" \
        "$seed_ips" \
        "$node_ip" \
        "CASSANDRA4_YAML_DEFAULT_PARAMS" \
        "$extra_map_name" \
        "$preloaded_yaml_map_name"
}


###############################################
# Wrapper: Cassandra 5.x
###############################################
update_cassandra5_yaml_with_yq_uncomment() {
    local yaml_file="$1" raw_data_dirs="$2" raw_commitlog_dir="$3" raw_saved_caches_dir="$4"
    local seed_ips="${5:-}" node_ip="${6:-}" extra_map_name="${7:-}" preloaded_yaml_map_name="${8:-}"

    update_cassandra_yaml_with_yq_uncomment \
        "$yaml_file" \
        "$raw_data_dirs" \
        "$raw_commitlog_dir" \
        "$raw_saved_caches_dir" \
        "$seed_ips" \
        "$node_ip" \
        "CASSANDRA5_YAML_DEFAULT_PARAMS" \
        "$extra_map_name" \
        "$preloaded_yaml_map_name"
}


###############################################################################
# update_cassandra_env_jmx  – enable remote JMX and set RMI hostname
# Args: 1 node_ip  (may be empty => just turn LOCAL_JMX off)
###############################################################################
update_cassandra_env_jmx() {
    local jmx_file="$1"
    local node_ip="$2"

    [[ -f "$jmx_file" ]] || { echo "❌  env file not found: $jmx_file"; return 1; }

    # Ensure LOCAL_JMX=no
    if grep -q '^LOCAL_JMX=' "$jmx_file"; then
        sed -i 's/^LOCAL_JMX=.*/LOCAL_JMX=no/' "$jmx_file"
    else
        echo 'LOCAL_JMX=no' >> "$jmx_file"
    fi

    # Add java.rmi.server.hostname only if node_ip supplied and not already present
    if [[ -n $node_ip ]]; then
        echo "JVM_OPTS=\"\$JVM_OPTS -Djava.rmi.server.hostname=$node_ip\"" >> "$jmx_file"
    fi

    echo "✅  cassandra-env.sh updated: $jmx_file"
}

# apply_jvm_overrides <file> <assoc_array_name>
#
# Updates or inserts JVM option overrides in a given jvm.options file.
#
# Arguments:
#   file             - Path to the jvm.options file to be modified.
#   assoc_array_name - Name of an associative array (passed by name) containing JVM option overrides.
#                      Each key is a JVM option prefix (e.g., -Xmx, -XX:+UseG1GC), and each value is the
#                      desired suffix or assignment (e.g., 48G, =500, or empty string for flags).
#                      To comment out an entry, set the value to "__COMMENT__".
#
# Behavior:
#   - If a line is commented (e.g., #-XX:+UseG1GC) and matches an override, it is uncommented.
#   - If a line exists for a key but has a different value, it is updated.
#   - If no line exists for a key, the override is appended at the end of the file.
#   - If the value is "__COMMENT__", the line is commented out instead.

apply_jvm_overrides() {
    local file="$1"
    local map_name="$2"
    declare -n overrides="$map_name"

    for key in "${!overrides[@]}"; do
        local value="${overrides[$key]}"
        local escaped_key
        escaped_key=$(printf '%s\n' "$key" | sed 's/[][\.^$*+?|(){}]/\\&/g')

        if [[ "$value" == "__COMMENT__" ]]; then
            # Comment out any uncommented line that starts with the key
            if grep -qE -- "^[[:space:]]*${escaped_key}" "$file"; then
                sed -i -E "s|^([[:space:]]*)(${escaped_key}[^\n]*)|# \2|" "$file"
            fi
        else
            local new_line="${key}${value}"
            local escaped_new_line
            escaped_new_line=$(printf '%s\n' "$new_line" | sed 's/[][\.^$*+?|(){}]/\\&/g')

            # Uncomment the line if it exists and is commented
            sed -i -E "s|^[[:space:]]*#[[:space:]]*(${escaped_key}[^\n]*)|\1|" "$file"

            # Skip if correct line already exists
            if grep -Fxq -- "$new_line" "$file"; then
                continue
            fi

            # Update if a line with the same key exists but value differs
            if grep -qE -- "^[[:space:]]*${escaped_key}" "$file"; then
                sed -i -E "s|^[[:space:]]*${escaped_key}[^\n]*|${new_line}|" "$file"
            else
                # Append if not found
                echo "$new_line" >> "$file"
            fi
        fi
    done
}



###############################################
# Function: check_or_install_yq
#
# Description:
#   Checks if `yq` (v4+, Go version) is installed.
#   If not found or incompatible, attempts to download and install
#   the latest version to /usr/local/bin/yq.
#
# Notes:
#   - Works for Linux and macOS with x86_64 or ARM64 architectures
#   - Requires `curl`, `chmod`, and `sudo` (if /usr/local/bin not writable)
#   - Verifies install and prints version
###############################################
check_or_install_yq() {
    local version ok=1

    if command -v yq >/dev/null 2>&1; then
        version=$(yq --version 2>/dev/null)
        if [[ "$version" == yq*version\ v4* ]]; then
            echo "✅ yq is already installed: $version"
            return 0
        else
            echo "⚠️ Found incompatible yq version: $version"
            ok=0
        fi
    else
        echo "🔍 yq not found in PATH."
        ok=0
    fi

    if [[ $ok -eq 0 ]]; then
        echo "📦 Installing yq v4..."

        local os=$(uname -s | tr '[:upper:]' '[:lower:]')
        local arch=$(uname -m)

        # Normalize architecture
        case "$arch" in
            x86_64) arch="amd64" ;;
            aarch64|arm64) arch="arm64" ;;
            *) echo "❌ Unsupported architecture: $arch"; return 1 ;;
        esac

        local yq_url="https://github.com/mikefarah/yq/releases/latest/download/yq_${os}_${arch}"
        local install_path="/usr/local/bin/yq"

        # If not writable, fallback to sudo
        if [[ ! -w $(dirname "$install_path") ]]; then
            echo "Using sudo to write to $install_path..."
            curl -L "$yq_url" -o /tmp/yq && sudo mv /tmp/yq "$install_path"
        else
            curl -L "$yq_url" -o "$install_path"
        fi

        chmod +x "$install_path"

        if command -v yq >/dev/null 2>&1; then
            echo "✅ yq installed successfully: $(yq --version)"
            return 0
        else
            echo "❌ Failed to install yq"
            return 1
        fi
    fi
}

