#!/bin/bash

set -e
# set -x

WORKLOAD_CONFIG_FILE="./workload_config.sh"

source "$WORKLOAD_CONFIG_FILE"

source ./general_funcs.sh

###############################################################################
# 2.  Trap for cleanup on ctrl-C or any ERR
###############################################################################
if [[ "$IS_MULTINODE" == true ]]; then
    cleanup() {
        echo -e "\n Cleanup triggered -- stopping cluster & flushing caches ..."
        stop_cassandra_cluster  || true
        FlushDisk "true"        || true
        remove_log_mounts_ucsb
        echo "Cleanup complete."
    }
else
    cleanup() {
        echo "Caught SIGINT. Performing full cleanup..."
        stop_db "cassandra"
        FlushDisk "false"
        echo "Cleanup complete. Exiting."
    }
fi
trap cleanup INT TERM ERR


cleanup

LOAD_TIMESTAMP=$(date +%Y-%m-%dT%H-%M-%S)
LOAD_OUTPUT_DIR="$YCSB_OUTPUT_DIR/load_ycsb_${YCSB_DB_SIZE_GB}GB_${LOAD_TIMESTAMP}"
mkdir -p $LOAD_OUTPUT_DIR
echo "Copying workload config and load files to run output dir for reference."
# Copy the workload config file and this current run file to the run output directory for reference
cp "$WORKLOAD_CONFIG_FILE" "$0" "$LOAD_OUTPUT_DIR/"

check_ulimit "$IS_MULTINODE"
ycsb_python_check
if [[ "$CGROUPV2_ENABLED" == true ]]; then
    check_pure_cgroupv2 "$IS_MULTINODE"  # Check if cgroup v2 is enabled
fi

# Check if LOAD_YCSB_INSTANCES_PER_NODE is set and is a number greater than 0
if [[ -z "$LOAD_YCSB_INSTANCES_PER_NODE" || ! "$LOAD_YCSB_INSTANCES_PER_NODE" =~ ^[0-9]+$ || "$LOAD_YCSB_INSTANCES_PER_NODE" -le 0 ]]; then
    echo "Error: LOAD_YCSB_INSTANCES_PER_NODE must be set to a number greater than 0."
    exit 1
fi

# Disable THP on all nodes
thp_disable_all "$IS_MULTINODE" || { echo "Failed to disable THP on one or more nodes."; exit 1; }

if [[ "$IS_MULTINODE" == true ]]; then
    stop_cassandra_cluster  || true
    create_log_mounts_ucsb "${LOAD_OUTPUT_DIR}"
    copy_cassandra_to_nodes        || { echo "Copy of Cassandra tree failed."; exit 1; }
    prepare_data_dirs_remote "${YCSB_DATA_FOLDER}" || { echo "Data-directory prep failed.";   exit 1; }
    
    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        # Call validate_readahead_or_die for every DB node
        for h in "${NODE_LIST[@]}"; do
            echo "Validating readahead on node $h"
            validate_readahead_or_die "$CGROUPV2_DATA_FOLDER" "$h"
        done
    fi

    patch_cassandra_configs_remote "${YCSB_DATA_FOLDER}" "normal" || { echo "Configs patch failed.";            exit 1; }
else
    stop_db "cassandra" # Stop Cassandra if it is running
    echo "Cleaning up previous data..."
    echo "Removing data folder: $YCSB_DATA_FOLDER"
    rm -rf $YCSB_DATA_FOLDER
    mkdir -p "$YCSB_DATA_FOLDER"
    
    if [[ "$CGROUPV2_ENABLED" == true ]]; then
        echo "Validating readahead on localhost"
        validate_readahead_or_die "$CGROUPV2_DATA_FOLDER" 
    fi

    ORIGINAL_CONFIG_FILE="$CASSANDRA_HOME_DIR/conf/cassandra.yaml"
    if [ ! -f "$ORIGINAL_CONFIG_FILE" ]; then
        echo "Error: Cassandra configuration file not found at $ORIGINAL_CONFIG_FILE. Exiting."
        exit 1
    fi

    DB_CONFIG_FILE="$LOAD_OUTPUT_DIR/cassandra_load.yaml"
    cp "$ORIGINAL_CONFIG_FILE" "$DB_CONFIG_FILE"

    update_cassandra_yaml_with_yq_uncomment "$DB_CONFIG_FILE" \
        "$YCSB_DATA_FOLDER/data" \
        "$YCSB_DATA_FOLDER/commitlog" \
        "$YCSB_DATA_FOLDER/saved_caches"
fi

if [[ "$IS_MULTINODE" == true ]]; then
    start_cassandra_cluster "vanilla"  || { echo "Cluster failed to start."; cleanup; exit 1; }
    echo "Cluster is up -- commencing load ..."
else
    start_db "cassandra" "vanilla" "$DB_CONFIG_FILE" "$LOAD_OUTPUT_DIR/db_cassandra.log"   # Start the database
fi

echo "Total rows to insert: $YCSB_ROW_COUNT"

# Load database with inserts
if [[ "$IS_MULTINODE" == true ]]; then
    hosts_csv=$(IFS=,; echo "${NODE_LIST[*]}")
    cqlsh_host="${SEED_NODE_IP}"  # Use the SEED_NODE_IP as the cqlsh host
else
    hosts_csv="127.0.0.1"
    cqlsh_host="127.0.0.1"
fi

cqlsh_bin="$CASSANDRA_HOME_DIR/bin/cqlsh"  # Path to cqlsh tool

# Create only the schema. Otherwise disable and enable auto compaction commands will not apply to 
# the new keyspace which was created after these enable/disable commands are executed.
# Create schema using cqlsh
schema_cql_file="$LOAD_OUTPUT_DIR/create_ycsb_schema.cql"

# This doesnt work for C5
# cat > "$schema_cql_file" <<EOF
# CREATE KEYSPACE IF NOT EXISTS ycsb
#     WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $YCSB_REPLICATION_FACTOR };
# USE ycsb;
# CREATE TABLE IF NOT EXISTS usertable (
#     y_id   varchar PRIMARY KEY,
#     field0 varchar,
#     field1 varchar,
#     field2 varchar,
#     field3 varchar,
#     field4 varchar,
#     field5 varchar,
#     field6 varchar,
#     field7 varchar,
#     field8 varchar,
#     field9 varchar
# ) WITH compression = {
#     'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor',
#     'chunk_length_kb'     : '$YCSB_CHUNK_LENGTH_KB'
# } $YCSB_COMPACTION_STRATEGY ;
# EOF


#This is for C5
cat > "$schema_cql_file" <<EOF
CREATE KEYSPACE IF NOT EXISTS ycsb
    WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $YCSB_REPLICATION_FACTOR };
USE ycsb;
CREATE TABLE IF NOT EXISTS usertable (
    y_id   varchar PRIMARY KEY,
    field0 varchar,
    field1 varchar,
    field2 varchar,
    field3 varchar,
    field4 varchar,
    field5 varchar,
    field6 varchar,
    field7 varchar,
    field8 varchar,
    field9 varchar
) WITH compression = {
    'class' : 'LZ4Compressor',
    'chunk_length_in_kb'     : '$YCSB_CHUNK_LENGTH_KB'
} $YCSB_COMPACTION_STRATEGY ;
EOF

# cat > "$schema_cql_file" <<EOF
# CREATE KEYSPACE IF NOT EXISTS ycsb
#     WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $YCSB_REPLICATION_FACTOR };
# USE ycsb;
# CREATE TABLE IF NOT EXISTS usertable (
#     y_id   varchar PRIMARY KEY,
#     field0 varchar,
#     field1 varchar,
#     field2 varchar,
#     field3 varchar,
#     field4 varchar,
#     field5 varchar,
#     field6 varchar,
#     field7 varchar,
#     field8 varchar,
#     field9 varchar
# ) WITH compression = {'enabled': false} $YCSB_COMPACTION_STRATEGY ;
# EOF

# cat > "$schema_cql_file" <<EOF
# CREATE KEYSPACE IF NOT EXISTS ycsb
#     WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $YCSB_REPLICATION_FACTOR };
# USE ycsb;
# CREATE TABLE IF NOT EXISTS usertable (
#     y_id   varchar PRIMARY KEY,
#     field0 varchar,
# ) WITH compression = {
#     'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor',
#     'chunk_length_kb'     : '$YCSB_CHUNK_LENGTH_KB'
# } $YCSB_COMPACTION_STRATEGY ;
# EOF


CQLSH_PYTHON=/usr/bin/python3.11 "$cqlsh_bin" "$cqlsh_host" -f "$schema_cql_file"
  
# For 1 kb value sizes, we have observed that the force-compaction as well as wait-compaction are faster when BG compactions are kept on.
# For 32 kb value sizes, the difference between the 2 scenarios is not that much, so we will keep BG compactions enabled always while loading.
# cassandra_disable_autocompaction "$IS_MULTINODE" || { echo "Failed to disable auto compaction on nodes."; exit 1; }

pushd "$YCSB_HOME_DIR" || { echo "Failed to change to YCSB dir"; exit 1; }

echo "Launching $LOAD_YCSB_INSTANCES_PER_NODE parallel YCSB loaders..."
rows_per_job=$((YCSB_ROW_COUNT / LOAD_YCSB_INSTANCES_PER_NODE))

for ((i = 0; i < LOAD_YCSB_INSTANCES_PER_NODE; i++)); do
  insert_start=$((i * rows_per_job))
  insert_count=$rows_per_job
  log_file="$LOAD_OUTPUT_DIR/ycsb_load_part_${i}.out"

  echo "  ➤ Launching YCSB load part $i: insertstart=$insert_start, insertcount=$insert_count"
  
  # The coreconnections, maxconnections, maxpendingrequests properties are set to try to avoid
  # GC storm in YCSB
  ycsb_load_cmd="./bin/ycsb load cassandra-cql -s \
        -threads $YCSB_LOAD_THREADS \
        -jvm-args \"$YCSB_JAVA_OPTS\" \
        -p hosts=$hosts_csv \
        -P $YCSB_WORKLOADS_DIR/workloada \
        -p insertstart=$insert_start \
        -p insertcount=$insert_count \
        -p recordcount=$YCSB_ROW_COUNT \
        -p requestdistribution=uniform \
        -p cassandra.coreconnections=2 \
        -p cassandra.maxconnections=2 \
        -p cassandra.maxpendingrequests=256 \
        -p cassandra.readconsistencylevel=$YCSB_CASSANDRA_READ_CONSISTENCY_LEVEL \
        -p cassandra.writeconsistencylevel=ALL \
        -p operationretrylimit=10 \
        -p cassandra.readtimeoutmillis=12000000 \
        -p cassandra.retrylimit=10 \
        -target 400000 \
        $YCSB_CUSTOM_FIELD_PARAMS "
  # echo "  ➤ Running command: $ycsb_load_cmd"
  eval "$ycsb_load_cmd 2>&1 | tee \"$log_file\" &"

  pids[$i]=$!
done

# Wait for all to complete
for ((i = 0; i < LOAD_YCSB_INSTANCES_PER_NODE; i++)); do
  wait "${pids[$i]}"
  echo "✅ YCSB load part $i completed with exit code $?"
done

popd || { echo "Failed to return from YCSB dir"; exit 1; }


cassandra_flush "$IS_MULTINODE" || { echo "Cassandra flush failed."; exit 1; }

# Enable auto compaction again
# cassandra_enable_autocompaction "$IS_MULTINODE" || { echo "Failed to enable auto compaction on nodes."; exit 1; }

# Now either force major compaction or wait for background compactions to finish
echo "Compacting ..."
compaction_start_time=$(date +%s)
if [[ "$FORCE_MAJOR_COMPACTION" == true ]]; then
    cassandra_force_major_compaction "$IS_MULTINODE" || { echo "Major compaction failed."; exit 1; }
else
    # Wait for all background compactions to finish
    cassandra_wait_for_background_compactions "$IS_MULTINODE"
fi
compaction_end_time=$(date +%s)
compaction_elapsed_time=$((compaction_end_time - compaction_start_time))
echo "Compaction Time taken: ${compaction_elapsed_time} seconds"

# echo "Invalidating cassandra caches ..."
# invalidate_caches_cassandra "$IS_MULTINODE" || { echo "Cassandra key and row cache invalidation failed."; exit 1; }

cleanup

# echo "Creating data tarball..."
# create_data_tarball "$IS_MULTINODE" "$YCSB_DATA_FOLDER" "$YCSB_TARBALL_PATH" || { echo "Data tarball creation failed."; exit 1; }

echo "load_ycsb.sh finished successfully."
echo "Results are in: $LOAD_OUTPUT_DIR"

