#!/bin/bash

# set -x
set -e

WORKLOAD_CONFIG_FILE="./workload_config.sh"

source ./general_funcs.sh
source "$WORKLOAD_CONFIG_FILE"

declare -a trials=("SATA_RA_32KB")
# declare -a trials=("SATA_RA_4KB")

DISK_DEV="/dev/sda" ## used to set RA

declare -a thread_arr=("32,12")
# declare -a thread_arr=("32,10" "32,12" "32,14")
# declare -a thread_arr=("32,8" "32,12" "32,14" "32,16" "32,18" "32,20" "32,25" "32,30")
# declare -a thread_arr=("16,16")

# Specify the workload types
#declare -a workload_arr=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf" "workloadc_uniform")
# declare -a workload_arr=("workloada")
declare -a workload_arr=("workloada_uniform")
# declare -a workload_arr=("workloada_uni_30r_70w")


# YCSB target throughput in ops/sec
declare -a ops_throttling_arr=("0")
# declare -a ops_throttling_arr=("50000" "100000" "150000" "200000" "250000")
# declare -a ops_throttling_arr=("20000" "40000" "60000" "80000" "100000")
# declare -a ops_throttling_arr=("80000")
# declare -a ops_throttling_arr=("150000" "250000")

#run vplus with 16KB RA
# declare -a config_arr=("vanilla")
# declare -a config_arr=("vanilla_plus")
# declare -a config_arr=("pvt_2MB_nosync_new" "pvt_128KB_nosync_new" "vanilla_plus" "vanilla")
# declare -a config_arr=("vanilla" "vanilla_plus" "pvt_2MB_nosync_new" "pvt_128KB_nosync_new")
declare -a config_arr=("release" "vanilla")



# declare -a mem_budget_arr=("64") # Available Memory left in the system in GB
declare -a mem_budget_arr=("0") # Available Memory left in the system in GB
# declare -a mem_budget_arr=("0" "64" "32") # Available Memory left in the system in GB

# Number and time of operations to run. Whichever is earlier, the run will stop.
# declare -a ops_arr=( "-p operationcount=$((100 * $YCSB_ROW_COUNT)) -p maxexecutiontime=600" )
declare -a ops_arr=( "-p operationcount=$((100 * $YCSB_ROW_COUNT)) -p maxexecutiontime=3600" )
# declare -a ops_arr=( "-p operationcount=$((100 * $YCSB_ROW_COUNT)) -p maxexecutiontime=11000" )


# --- Global variable to enable/disable logging ---
ENABLE_LOGGING=false  # Set to false to disable logging
MIXED_VANILLA_AND_OTHERS=false #Enables some nodes run vanilla and others on pvt_lru

# ------------------------------------------------------------------------------------------------------------------------------------------------- #

CONFIG_PATH="$BASE/lib"

# --- Prepare run output directory ---
RUN_TIMESTAMP=$(date +%Y-%m-%dT%H-%M-%S)
RUN_OUTPUT_DIR="${YCSB_OUTPUT_DIR}/results_ycsb_${YCSB_DB_SIZE_GB}GB_${RUN_TIMESTAMP}"
mkdir -p "$RUN_OUTPUT_DIR"
echo "Run output directory: $RUN_OUTPUT_DIR"
echo "Copying workload config and run files to run output dir for reference."
# Copy the ycsb workloads directory to the run output directory
cp -r "$YCSB_WORKLOADS_DIR" "$RUN_OUTPUT_DIR/speedyio_ycsb_workloads"
# Copy the workload config file and this current run file to the run output directory for reference
cp "$WORKLOAD_CONFIG_FILE" "$0" "$RUN_OUTPUT_DIR/"

# Cleanup on SIGINT or fatal error
if [[ "$IS_MULTINODE" == true ]]; then
    cleanup() {
        echo -e "\n Cleanup triggered -- stopping cluster & flushing caches ..."
        stop_logging "true"
        stop_cassandra_cluster  || true
        FlushDisk "true"        || true
        memory_cleanup "true"
        remove_log_mounts_ucsb

        # Call remove_log_mounts_extra_requesters if EXTRA_REQUESTER_NODES is set and non-empty
        if [[ -n "$EXTRA_REQUESTER_NODES" ]]; then
            remove_log_mounts_extra_requesters "${RUN_TIMESTAMP}"
        fi

        echo "Cleanup complete."
        exit 1
    }
else
    cleanup() {
        echo "Caught SIGINT. Performing full cleanup..."
        stop_db "cassandra"
        stop_logging "false"
        memory_cleanup "false"
        FlushDisk "false"
        echo "Cleanup complete. Exiting."
        exit 1
    }
fi

trap cleanup INT TERM ERR

check_ulimit "$IS_MULTINODE"
ycsb_python_check
if [[ "$CGROUPV2_ENABLED" == true ]]; then
    check_pure_cgroupv2 "$IS_MULTINODE"  # Check if cgroup v2 is enabled
fi

if [[ "$IS_MULTINODE" == true ]]; then
    check_ulimit "false"  # Check ulimit for requester node as well

    stop_cassandra_cluster  || true

    remove_log_mounts_ucsb
    create_log_mounts_ucsb "${RUN_OUTPUT_DIR}"
    
    # Call remove_log_mounts_extra_requesters if EXTRA_REQUESTER_NODES is set and non-empty
    if [[ -n "$EXTRA_REQUESTER_NODES" ]]; then
        remove_log_mounts_extra_requesters "${RUN_TIMESTAMP}"
        REMOTE_MOUNT_DIR_FOR_REQUESTER=""
        create_log_mounts_extra_requesters "${RUN_OUTPUT_DIR}" "${RUN_TIMESTAMP}" REMOTE_MOUNT_DIR_FOR_REQUESTER
        # REMOTE_MOUNT_DIR_FOR_REQUESTER=$(create_log_mounts_extra_requesters "${RUN_OUTPUT_DIR}" "${RUN_TIMESTAMP}")
        # echo "REMOTE_MOUNT_DIR_FOR_REQUESTER=$REMOTE_MOUNT_DIR_FOR_REQUESTER"
        # exit 1
        copy_ycsb_to_extra_requester_nodes || { echo "Copy of YCSB tree to extra requester nodes failed."; exit 1; }
    fi

    copy_cassandra_to_nodes        || { echo "Copy of Cassandra tree failed."; exit 1; }
    copy_so_files_to_nodes config_arr || { echo "Copy of shared libraries failed."; exit 1; }
    patch_cassandra_configs_remote "${YCSB_DATA_FOLDER}" "normal" || { echo "Configs patch failed.";            exit 1; }

    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        # Call validate_readahead_or_die for every DB node
        for h in "${NODE_LIST[@]}"; do
            echo "Validating readahead on node $h"
            validate_readahead_or_die "$CGROUPV2_DATA_FOLDER" "$h"
        done
    fi
else
    stop_db "cassandra" # Stop the database
    ORIGINAL_CONFIG_FILE="$CASSANDRA_HOME_DIR/conf/cassandra.yaml"
    if [ ! -f "$ORIGINAL_CONFIG_FILE" ]; then
        echo "Error: Cassandra configuration file not found at $ORIGINAL_CONFIG_FILE. Exiting."
        exit 1
    fi

    DB_CONFIG_FILE="$RUN_OUTPUT_DIR/cassandra_load.yaml"
    cp "$ORIGINAL_CONFIG_FILE" "$DB_CONFIG_FILE"

    update_cassandra_yaml_with_yq_uncomment "$DB_CONFIG_FILE" \
        "$YCSB_DATA_FOLDER/data" \
        "$YCSB_DATA_FOLDER/commitlog" \
        "$YCSB_DATA_FOLDER/saved_caches"
    
    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        echo "Validating readahead on localhost"
        validate_readahead_or_die "$CGROUPV2_DATA_FOLDER" 
    fi
fi

# Disable THP on all nodes
thp_disable_all "$IS_MULTINODE" || { echo "Failed to disable THP on one or more nodes."; exit 1; }

RUN() {
        TRIAL=$1
        THREAD=$2
        RUN_YCSB_INSTANCES_PER_NODE=$3
        MEM_BUDGET_GB=$4
        CONFIG=$5
        OPS_PARAM=$6
        OPS_THROTTLING=$7
        WORKLOAD=$8

        local ops_param_without_p_flags="${OPS_PARAM//-p /}"  # Remove -p flags for the run identifier
        EXPT_IDENTIFIER_STR="trial_${TRIAL}__workload_${WORKLOAD}__ops_param_${ops_param_without_p_flags}__ops_throttling_${OPS_THROTTLING}__threads_${THREAD}-${RUN_YCSB_INSTANCES_PER_NODE}__mem_budget_gb_${MEM_BUDGET_GB}__config_${CONFIG}"
        EXPT_IDENTIFIER_STR="${EXPT_IDENTIFIER_STR// /__}"  # replace every space with "__"

        echo "Running YCSB with identifier: $EXPT_IDENTIFIER_STR"

        local SO_FILE_PATH=""
        if [ "$CONFIG" != "vanilla" ]; then
            local so_file_name="lib_speedyio_${CONFIG}.so"
            if [[ "$IS_MULTINODE" == true ]]; then
                # .so file will be available in the cassandra bin directory on the remote nodes
                SO_FILE_PATH="$CASSANDRA_HOME_DIR/bin/$so_file_name"
            else
                # Else use local config path directly
                SO_FILE_PATH="$CONFIG_PATH/$so_file_name"
            fi
        fi

        if [[ "$IS_MULTINODE" == true ]]; then
            hosts_csv=$(IFS=,; echo "${NODE_LIST[*]}")
        else
            hosts_csv="127.0.0.1"
        fi

        echo "External YCSB_JAVA_OPTS: $YCSB_JAVA_OPTS"

        get_read_command() {
            local mode="$1"
            local index_to_use_in_suffix="$2"
            local workload_dir=""
            local hdr_output_path=""
            local jvm_args_str=""

            # Check if index_to_use_in_suffix is set and is a number >= 0
            if [[ -z "$index_to_use_in_suffix" || ! "$index_to_use_in_suffix" =~ ^[0-9]+$ ]]; then
                echo "Error: index_to_use_in_suffix must be a non-negative integer, passed: $index_to_use_in_suffix"
                exit 1
            fi

            if [[ "$mode" == "remote" ]]; then
                workload_dir="$REMOTE_MOUNT_DIR_FOR_REQUESTER/speedyio_ycsb_workloads"  # This will basically point to RUN_OUTPUT_DIR/speedyio_ycsb_workloads on the main local requester node
                hdr_output_path="${REMOTE_MOUNT_DIR_FOR_REQUESTER}/ycsb_run__'${EXPT_IDENTIFIER_STR}'__node__extra_requester_{{ inventory_hostname }}__part_${index_to_use_in_suffix}.out."
                jvm_args_str=" -jvm-args \\\"$YCSB_JAVA_OPTS\\\" "
            elif [[ "$mode" == "local" ]]; then
                workload_dir="$RUN_OUTPUT_DIR/speedyio_ycsb_workloads"
                hdr_output_path="${RUN_OUTPUT_DIR}/ycsb_run__${EXPT_IDENTIFIER_STR}__node_local__part_${index_to_use_in_suffix}.out."
                jvm_args_str=" -jvm-args \"$YCSB_JAVA_OPTS\" "
            else
                echo "Error: Invalid argument to get_read_command. Use 'local' or 'remote'." >&2
                exit 1
            fi

            local workload_local=$WORKLOAD

            # Total number of YCSB instances across all requester nodes including local
            local total_ycsb_instances=$(( RUN_YCSB_INSTANCES_PER_NODE * ( ${#EXTRA_REQUESTER_NODES[@]} + 1 ) ))
            # The coreconnections, maxconnections, maxpendingrequests properties are set to try to avoid
            # GC storm in YCSB
            echo "./bin/ycsb run cassandra-cql -s \
            -threads $THREAD \
            $jvm_args_str \
            -p hosts=$hosts_csv \
            -P $workload_dir/$workload_local \
            -p recordcount=$YCSB_ROW_COUNT \
            $OPS_PARAM \
            -target $(( $OPS_THROTTLING / $total_ycsb_instances  )) \
            -p cassandra.readconsistencylevel=$YCSB_CASSANDRA_READ_CONSISTENCY_LEVEL \
            -p cassandra.writeconsistencylevel=$YCSB_CASSANDRA_WRITE_CONSISTENCY_LEVEL \
            -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=true \
            -p hdrhistogram.output.path=$hdr_output_path \
            $YCSB_CUSTOM_FIELD_PARAMS "
            # -p throttle.schedule=1200s:1500s:6250,1800s:2400s:6875 \
            # -p throttle.schedule=0s:600s:15000 \
            # -p cassandra.coreconnections=8 \
            # -p cassandra.maxconnections=8 \
#            -p cassandra.maxpendingrequests=256 "
        }

        # Ensure clean slate, possibly any bg processes from previous runs
        stop_logging "$IS_MULTINODE"

        if [[ "$IS_MULTINODE" == true ]]; then
            stop_cassandra_cluster  || true
        else
            stop_db "cassandra" # Stop the database
        fi
        
        TurnSwapOff "$IS_MULTINODE" # Turn off swap if on
        FlushDisk "$IS_MULTINODE"
        
        budget_memory "$MEM_BUDGET_GB" "all" "$IS_MULTINODE" # Set memory budget

        if [ "$CONFIG" != "vanilla" ]; then
            create_modified_cassandra_launcher "$CONFIG" "$SO_FILE_PATH" "$IS_MULTINODE"
        fi

        #Behaviour used to make things work on linux 6
        run_on_nodes "sudo sysctl -w net.ipv4.tcp_tw_reuse=1"

        if [[ "$IS_MULTINODE" == true ]]; then

            if [[ "$MIXED_VANILLA_AND_OTHERS" == true ]]; then
                # Vanilla
                for n in "${V_NODE_LIST[@]}"; do
                    NODE_CFG_MAP["$n"]="vanilla"
                done

                for n in "${OTHER_NODE_LIST[@]}"; do
                    NODE_CFG_MAP["$n"]="$CONFIG"
                done
                start_cassandra_cluster_mixed || { echo "Mix Cluster failed to start."; exit 1; }
            else #every node runs the same config
                start_cassandra_cluster "$CONFIG" || { echo "Cluster failed to start."; exit 1; }
            fi
            
            echo "Cluster is up -- commencing run ..."
        else
            start_db "cassandra" $CONFIG "$DB_CONFIG_FILE" "${RUN_OUTPUT_DIR}/db_${EXPT_IDENTIFIER_STR}.log"   # Start the database
        fi

        # cassandra_disable_autocompaction "$IS_MULTINODE" || { echo "Failed to disable auto compaction on nodes."; exit 1; }
        # Command to disable WAL (durable_writes) on keyspace ycsb
        # set_cassandra_wal "$IS_MULTINODE" "ycsb" "disable" || { echo "Failed to disable WAL on keyspace ycsb."; exit 1; }

        # Compact before starting the workload
        cassandra_wait_for_background_compactions "$IS_MULTINODE"


        #set_speculative_retry
        set_speculative_retry "$IS_MULTINODE" "ycsb" "usertable" "disable" || { echo "Failed to disable WAL on keyspace ycsb."; exit 1; }


        # Start logging if enabled
        if [ "$ENABLE_LOGGING" == true ]; then
            if [[ "$IS_MULTINODE" == true ]]; then
                start_logging "$REMOTE_MOUNT_DIR" "$EXPT_IDENTIFIER_STR" "$IS_MULTINODE"
            else
                start_logging "$RUN_OUTPUT_DIR" "$EXPT_IDENTIFIER_STR" "$IS_MULTINODE"
            fi
        fi

        #Clear Lock stat and start recording
        # run_on_nodes "sudo sh -c 'echo 0 > /proc/sys/kernel/lock_stat'; sudo sh -c 'echo 0 > /proc/lock_stat'; sudo cat /proc/lock_stat; sudo sh -c 'echo 1 > /proc/sys/kernel/lock_stat'"

        # Run local YCSB in background
        pushd "$YCSB_HOME_DIR" || { echo "Failed to change directory to YCSB home."; exit 1; }
        
        for ((i = 0; i < RUN_YCSB_INSTANCES_PER_NODE; i++)); do
            local read_command_local="$(get_read_command "local" "$i")"
            echo "read_command_local part $i = $read_command_local"
            eval "$read_command_local 2>&1 | tee \"$RUN_OUTPUT_DIR/ycsb_run__${EXPT_IDENTIFIER_STR}__node_local__part_${i}.out\" || echo \"YCSB part ${i} returned non-zero exit but continuing.\" &"
            pids[$i]=$!
        done
        # echo "local ycsb pids: ${pids[@]}"

        popd || { echo "Failed to change directory back from YCSB home."; exit 1; }

        # Run on extra requester nodes (ensure pushd/popd in remote command)
        if [[ "$IS_MULTINODE" == true && -n "$EXTRA_REQUESTER_NODES" ]]; then
            read_cmds=()
            for ((i = 0; i < RUN_YCSB_INSTANCES_PER_NODE; i++)); do
                read_cmds[i]=$(get_read_command "remote" "$i")
            done
            # Build the serialized array as a string
            serialized_array="read_cmds=("
            for cmd in "${read_cmds[@]}"; do
                serialized_array+="\"$cmd\" "
            done
            serialized_array+=")"

            # echo $serialized_array

            multi_instance_remote_command="$serialized_array; \
for remote_i in \$(seq 0 $((RUN_YCSB_INSTANCES_PER_NODE - 1))); do \
  log_file=\"${REMOTE_MOUNT_DIR_FOR_REQUESTER}/ycsb_run__${EXPT_IDENTIFIER_STR}__node__extra_requester_{{ inventory_hostname }}__part_\${remote_i}.out\"; \
  eval \"\${read_cmds[\$remote_i]}\" 2>&1 | tee \"\$log_file\" || echo \"YCSB part \$i failed\" & \
done; wait"

            echo "Multi-instance remote command: $multi_instance_remote_command"
            run_on_extra_requester_nodes "pushd \"$YCSB_HOME_DIR\" && $multi_instance_remote_command && popd"

            echo "Remote extra requester nodes all ycsb processes finished ..."
        fi

        # Wait for extra requester nodes to finish (run_on_extra_requester_nodes is blocking)
        # Then wait for local YCSB to finish
        echo "Waiting local YCSB requesters to finish..."
        
        # Wait for all local ycsb runs to complete. By the time this command is reached, 
        # all requester nodes should have completed their runs.
        # Need to wait using specific pid info because a generic "wait" will always
        # get stuck because it waits for all child processes to end, including other ones 
        # such as monitoring script background processes.
        for ((i = 0; i < RUN_YCSB_INSTANCES_PER_NODE; i++)); do
            wait "${pids[$i]}"
            echo "✅ YCSB local run part $i completed with exit code $?"
        done

        export LD_PRELOAD=""

        if [ "$ENABLE_LOGGING" = true ]; then
            sleep 2
            stop_logging "$IS_MULTINODE" # Ensure clean slate
        fi

        #Save Lock Stat to location
        # run_on_nodes "sudo sh -c 'echo 0 > /proc/sys/kernel/lock_stat'; sudo cat /proc/lock_stat > ~/ssd/lockstat__trial_${TRIAL}__config_${CONFIG}__workload_${WORKLOAD}"

        # invalidate_caches_cassandra "$IS_MULTINODE" || { echo "Cassandra key and row cache invalidation failed."; exit 1; }

        # Compact before stopping the cluster
        cassandra_wait_for_background_compactions "$IS_MULTINODE"


        if [[ "$IS_MULTINODE" == true ]]; then
            stop_cassandra_cluster
        else
            stop_db "cassandra" # Stop the database
        fi
        
        memory_cleanup "$IS_MULTINODE" # Cleanup memory budget
}

ra_sectors_from_trial() {
  local tr="$1"
  if [[ "$tr" =~ _RA_([0-9]+)KB$ ]]; then
    local kb="${BASH_REMATCH[1]}"
    echo $(( kb * 1024 / 512 ))   # blockdev --setra uses 512B sectors
  else
    echo "Error: trial '$tr' does not match pattern *_RA_<N>KB" >&2
    exit 1
  fi
}


for tr in "${trials[@]}"; do
    # Set readahead according to this trial
    RA_SECTORS="$(ra_sectors_from_trial "$tr")"
    run_on_nodes "VAR=$RA_SECTORS; sudo blockdev --setra \$VAR $DISK_DEV; sudo blockdev --getra $DISK_DEV"
    run_on_nodes "sudo blockdev --report; lsblk"

    for wkld in "${workload_arr[@]}"; do
            for ops_param in "${ops_arr[@]}"; do
                for budget_gb in "${mem_budget_arr[@]}"; do
                    for ops_throttle in "${ops_throttling_arr[@]}"; do
                        for th_elem in "${thread_arr[@]}"; do
                            IFS=',' read -r th nr_ycsb <<< "$th_elem"
                            for cf in "${config_arr[@]}"; do

                                # restore_data_from_tarball "$IS_MULTINODE" "$YCSB_DATA_FOLDER" "$YCSB_TARBALL_PATH" || { echo "Data restoration failed."; exit 1; }

                                memory_cleanup "$IS_MULTINODE"
                                FlushDisk "$IS_MULTINODE"
                                echo ""
                                echo "======================================================================================================================="
                                echo "Running Trial:$tr || Workload:$wkld || $run_param_string || Ops Limit: $ops_throttle || Threads:$th || YCSBs:$nr_ycsb || Mem Budget:$budget_gb GB || Config:$cf"
                                echo "======================================================================================================================="
                                RUN "$tr" "$th" "$nr_ycsb" "$budget_gb" "$cf" "$ops_param" "$ops_throttle" "$wkld"

                                FlushDisk "$IS_MULTINODE"

                            done
                        done
                    done
                done
            done
    done
done

memory_cleanup "$IS_MULTINODE" # Cleanup memory budget
FlushDisk "$IS_MULTINODE"
echo "All trials completed. Exiting."
cleanup

run_on_nodes "sudo blockdev --report"

