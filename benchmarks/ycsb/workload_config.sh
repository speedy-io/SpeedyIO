export DB_TYPE="cassandra5"  # Database type
# export DB_TYPE="cassandra4"  # Database type
# export DB_TYPE="cassandra3"  # Database type
export CASSANDRA_HOME_DIR="$HOME/ssd/cassandra5"   # adjust if needed
# export CASSANDRA_HOME_DIR="$HOME/ssd/cassandra4"   # adjust if needed
# export CASSANDRA_HOME_DIR="$HOME/ssd/cassandra"   # adjust if needed

export YCSB_HOME_DIR="$HOME/ssd/ycsb"  # adjust if needed
export YCSB_WORKLOADS_DIR="$BASE/benchmarks/ycsb/ycsb_workloads"  # Path to YCSB workloads directory

# YCSB_DB_SIZE_GB="2700"  # DB size in GB
YCSB_DB_SIZE_GB="20"  # DB size in GB

YCSB_DATA_FOLDER="$HOME/ssd/ycsb_data_c5/${YCSB_DB_SIZE_GB}GB"  # Data folder path
# YCSB_DATA_FOLDER="$HOME/ssd/ycsb_data/${YCSB_DB_SIZE_GB}GB"  # Data folder path

YCSB_TARBALL_PATH="$HOME/ssd/compacted/${YCSB_DB_SIZE_GB}GB.tar.gz"  # Change this to something else if you want to put the tarball somewhere else

YCSB_OUTPUT_DIR="$HOME/ssd/results/c5/load"


# Set to true to force major compaction after loading, otherwise it will wait for normal background compactions to finish
FORCE_MAJOR_COMPACTION=false


declare -A CASSANDRA3_YAML_OVERRIDES=(
#      ["memtable_allocation_type"]="offheap_buffers"
#     ["disk_access_mode"]="mmap_index_only"
#     ["row_cache_size_in_mb"]=10240
      ["concurrent_reads"]=32
      ["concurrent_writes"]=32
      ["file_cache_size_in_mb"]=4096
#      ["chunk_cache_size_in_mb"]=256
#     ["buffer_pool_use_heap_if_exhausted"]=true
#     ["disk_optimization_strategy"]="ssd"
      ["memtable_flush_writers"]=4
#      ["trickle_fsync"]=true
#      ["commitlog_sync"]="batch"
#      ["commitlog_sync_batch_window_in_ms"]=2
      ["concurrent_compactors"]=6
      ["compaction_throughput_mb_per_sec"]=32
#     ["stream_throughput_outbound_megabits_per_sec"]=7000
      ["memtable_heap_space_in_mb"]=4096
      ["dynamic_snitch"]=false
)

declare -A CASSANDRA4_YAML_OVERRIDES=(
  # ["memtable_allocation_type"]="offheap_buffers"   
  # ["row_cache_size"]="10240MiB"                    

    ["disk_access_mode"]="mmap_index_only"  # This disk_access_mode option is not specified in cassandra 4 yaml, but if you supply it then it works
    ["concurrent_reads"]=32                            
    ["concurrent_writes"]=32                          

    ["file_cache_size"]="4096MiB"

    # ["buffer_pool_use_heap_if_exhausted"]=true      
    # ["disk_optimization_strategy"]="ssd"             

    ["memtable_flush_writers"]=4                       

    # ["trickle_fsync"]=true                           
    # ["commitlog_sync"]="batch"                      
    # commitlog_sync_batch_window_in_ms is deprecated in 4.x; use group mode if you *really* need a window:
    # ["commitlog_sync"]="group"
    # ["commitlog_sync_group_window"]="2ms"

    ["concurrent_compactors"]=6                       

    ["compaction_throughput"]="32MiB/s"

    # 3.x: stream_throughput_outbound_megabits_per_sec=7000
    # 4.x: stream_throughput_outbound in MiB/s (~7000 Mbit/s ≈ 875 MiB/s)
    # ["stream_throughput_outbound"]="875MiB/s"

    # 3.x: memtable_heap_space_in_mb
    # 4.x: memtable_heap_space (Min unit: MiB)
    ["memtable_heap_space"]="4096MiB"
    ["dynamic_snitch"]=false
)

declare -A CASSANDRA5_YAML_OVERRIDES=(
    # Still valid but generally discouraged unless you know exactly why you want it.
    # ["memtable_allocation_type"]="offheap_buffers"
    # ["row_cache_size"]="10240MiB"

    # Disk & IO
#    ["disk_access_mode"]="mmap_index_only"
    ["disk_access_mode"]="standard"
    ["file_cache_size"]="4096MiB"

    ["concurrent_reads"]=32
    ["concurrent_writes"]=32
    ["concurrent_compactors"]=6

    # Memtables
    ["memtable_flush_writers"]=4
    ["memtable_heap_space"]="4096MiB"

    # Compaction
    ["compaction_throughput"]="32MiB/s"

    # Snitching
    ["dynamic_snitch"]=false

    # Still valid but intentionally omitted:
    # ["disk_optimization_strategy"]="ssd"
    # ["commitlog_sync"]="group"
    # ["commitlog_sync_group_window"]="2ms"
    # ["stream_throughput_outbound"]="875MiB/s"
)


declare -A CASSANDRA3_JVM_OVERRIDES=(
    # ["-Dcassandra.debugrefcount"]="=true"

    ["-XX:+UseParNewGC"]="__COMMENT__"
    ["-XX:+UseConcMarkSweepGC"]="__COMMENT__"
    ["-XX:+CMSParallelRemarkEnabled"]="__COMMENT__"
    ["-XX:SurvivorRatio"]="__COMMENT__"
    ["-XX:MaxTenuringThreshold"]="__COMMENT__"
    ["-XX:CMSInitiatingOccupancyFraction"]="__COMMENT__"
    ["-XX:+UseCMSInitiatingOccupancyOnly"]="__COMMENT__"
    ["-XX:CMSWaitDuration"]="__COMMENT__"
    ["-XX:+CMSParallelInitialMarkEnabled"]="__COMMENT__"
    ["-XX:+CMSEdenChunksRecordAlways"]="__COMMENT__"
    ["-XX:+CMSClassUnloadingEnabled"]="__COMMENT__"

    ["-Xms"]="16G"
    ["-Xmx"]="16G"
    # ["-Xms"]="8G"
    # ["-Xmx"]="8G"
    ["-XX:+UseG1GC"]=""
    # ["-XX:MaxGCPauseMillis"]="=200"

    # ["-XX:G1RSetUpdatingPauseTimePercent"]="=5"

    # ["-XX:InitiatingHeapOccupancyPercent"]="=30"

    ## keep young gen in check (prevents multi-GB Eden swings)
    # ["-XX:+UnlockExperimentalVMOptions"]=""
    # ["-XX:G1NewSizePercent"]="=5"
    # ["-XX:G1MaxNewSizePercent"]="=20"
 
    ## Reference processing to multithreaded
    # ["-XX:+ParallelRefProcEnabled"]=""

    ## To reduce Old GC operations
    # ["-XX:G1ReservePercent"]="=20"

    ## reduce early promotion
    # ["-XX:MaxTenuringThreshold"]="=12"
    # ["-XX:SurvivorRatio"]="=8"

    ## Deduplicates strings payloads
    # ["-XX:+UseStringDeduplication"]=""

    ## GC threads
    # ["-XX:ParallelGCThreads"]="=16"
    # ["-XX:ConcGCThreads"]="=10"

    ## Disable Explicit GC calls from an external entity
    ## ["-XX:+DisableExplicitGC"]="" ##This should never be on
    # ["-XX:+ExplicitGCInvokesConcurrent"]=""

    # Disable biased locking
    # ["-XX:-UseBiasedLocking"]=""
)

declare -A CASSANDRA4_JVM_OVERRIDES=(
    ["-XX:+UseParNewGC"]="__COMMENT__"
    ["-XX:+UseConcMarkSweepGC"]="__COMMENT__"
    ["-XX:+CMSParallelRemarkEnabled"]="__COMMENT__"
    ["-XX:SurvivorRatio"]="__COMMENT__"
    ["-XX:MaxTenuringThreshold"]="__COMMENT__"
    ["-XX:CMSInitiatingOccupancyFraction"]="__COMMENT__"
    ["-XX:+UseCMSInitiatingOccupancyOnly"]="__COMMENT__"
    ["-XX:CMSWaitDuration"]="__COMMENT__"
    ["-XX:+CMSParallelInitialMarkEnabled"]="__COMMENT__"
    ["-XX:+CMSEdenChunksRecordAlways"]="__COMMENT__"
    ["-XX:+CMSClassUnloadingEnabled"]="__COMMENT__"

    ["-Xms"]="16G"
    ["-Xmx"]="16G"

    ["-XX:+UseG1GC"]=""
)


declare -A CASSANDRA5_JVM_OVERRIDES=(
    # Old CMS / ParNew era flags — must be removed/commented for modern JVMs (JDK 17+)
    ["-XX:+UseParNewGC"]="__COMMENT__"
    ["-XX:+UseConcMarkSweepGC"]="__COMMENT__"
    ["-XX:+CMSParallelRemarkEnabled"]="__COMMENT__"
    ["-XX:SurvivorRatio"]="__COMMENT__"
    ["-XX:MaxTenuringThreshold"]="__COMMENT__"
    ["-XX:CMSInitiatingOccupancyFraction"]="__COMMENT__"
    ["-XX:+UseCMSInitiatingOccupancyOnly"]="__COMMENT__"
    ["-XX:CMSWaitDuration"]="__COMMENT__"
    ["-XX:+CMSParallelInitialMarkEnabled"]="__COMMENT__"
    ["-XX:+CMSEdenChunksRecordAlways"]="__COMMENT__"
    ["-XX:+CMSClassUnloadingEnabled"]="__COMMENT__"

    # Heap sizing
    ["-Xms"]="16G"
    ["-Xmx"]="16G"

    # GC (G1 is the normal choice for Cassandra on modern JVMs)
    ["-XX:+UseG1GC"]=""
)

#----- Settings for cgroup v2 -----------------------------------#
# i3.xlarge —>    750R   270W
# i3.2xlarge —>   1500R  700W
# i3.4xlarge —>   3000R 1400W (RAID0 on the 2 drives)
: "${CGROUPV2_ENABLED:=false}"        # true or false — whether cgroup v2 is enabled
: "${CGROUPV2_RBPS:=3000000000}"      # Read bytes per second limit. Set to "max" to disable
: "${CGROUPV2_WBPS:=1400000000}"      # Write bytes per second limit.  Set to "max" to disable
declare -p CGROUPV2_ENABLED CGROUPV2_RBPS CGROUPV2_WBPS

CGROUPV2_CGROUP_NAME="speedyio-ycsb-cgroup"  # Cgroup name for Cassandra stress
CGROUPV2_RIOPS="max"  # Read IOPS limit.  Set to "max" to disable
CGROUPV2_WIOPS="max"  # Write IOPS limit.  Set to "max" to disable
CGROUPV2_DATA_FOLDER=$YCSB_DATA_FOLDER  # The device number will be determined from this path.
#-----------------------------------------------------------------#


#------------------- Settings for default ycsb ------------------- #
YCSB_VALUE_SIZE_BYTES="1000"  # Value size in bytes
YCSB_CUSTOM_FIELD_PARAMS="  "  # No need to specify anything for the default schema

# YCSB_VALUE_SIZE_BYTES="32768"  # Value size in bytes
# YCSB_CUSTOM_FIELD_PARAMS=" -p fieldcount=1 -p fieldlength=$YCSB_VALUE_SIZE_BYTES -p fieldlengthdistribution=constant "
#-----------------------------------------------------------------#

# The -jvm-args flag must be provided before the YCSB command's arguments (e.g., -P, -p, etc.)
# It works only with the YCSB shell scripts (bin/ycsb) that wrap the Java launcher and pass the arguments to the JVM.
# To check if the correct java flags are being used, you can run:
#    1. Run "jps -l" to find the java process ID.
#    2. Run "jinfo -flags <pid>" to see the JVM flags for that process.
# YCSB_JAVA_OPTS=" -Dycsb.exit.on.failure=true -Xms16g -Xmx16g -XX:+UseG1GC "  # Java options for YCSB
# YCSB_JAVA_OPTS=" -Dycsb.exit.on.failure=true -Xms8g -Xmx8g -XX:+UseG1GC "  # Java options for YCSB
YCSB_JAVA_OPTS=" -Dycsb.exit.on.failure=true -Xms512m -Xmx512m -XX:+UseSerialGC -XX:MaxDirectMemorySize=512m "

YCSB_LOAD_THREADS=8      # Number of threads for loading
LOAD_YCSB_INSTANCES_PER_NODE=1  # Number of YCSB instances per node
# RUN_YCSB_INSTANCES_PER_NODE=4  # Number of YCSB instances per node

YCSB_REPLICATION_FACTOR=1  # Replication factor for the keyspace
# Keep YCSB_COMPACTION_STRATEGY empty if you want to use the default compaction strategy
YCSB_COMPACTION_STRATEGY=" AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '256'} "
YCSB_CHUNK_LENGTH_KB="16"  # Chunk length in KB for compression. Cassandra 3.11 default is 64KB
#---------------------------------------------------------------------------------- #

# Calculate number of rows based on DB size and value size
YCSB_TOTAL_BYTES=$((YCSB_DB_SIZE_GB * 1024 * 1024 * 1024))
YCSB_ROW_COUNT=$((YCSB_TOTAL_BYTES / YCSB_VALUE_SIZE_BYTES))

# YCSB uses QUORUM as the default consistency level
YCSB_CASSANDRA_READ_CONSISTENCY_LEVEL="QUORUM"
# YCSB_CASSANDRA_READ_CONSISTENCY_LEVEL="ONE"
YCSB_CASSANDRA_WRITE_CONSISTENCY_LEVEL="QUORUM"

IS_MULTINODE=true
# ---------------------------------------------------------------------------
# Remote Cassandra hosts (no localhost).  First entry is the seed.
# ---------------------------------------------------------------------------
NODE_LIST=( $(seq -f "10.10.1.%g" 2 3) )  # This will generate a list of IPs, both start and end inclusive

# V_NODE_LIST=("10.10.1.2" "10.10.1.3" "10.10.1.4" "10.10.1.5" "10.10.1.6" "10.10.1.7")
# V_NODE_LIST=( $(seq -f "10.10.1.%g" 2 9) )
# OTHER_NODE_LIST=( $(seq -f "10.10.1.%g" 10 33) )

REQUESTER_NODE_IP="10.10.1.1"  # IP of the requester node (this node)
# EXTRA_REQUESTER_NODES=("10.10.1.2")  # List of additional requester nodes if needed
SEED_NODE_INDEX=0
SEED_NODE_IP="${NODE_LIST[$SEED_NODE_INDEX]}"  # Seed node IP

#Stores which nodes is doing which config
declare -A NODE_CFG_MAP=()
# ---------------------------------------------------------------------------


# Validations
source ./validations.sh
if [[ "$IS_MULTINODE" == true ]]; then
    multinode_validations
fi

echo "before python check"
ycsb_python_check
echo "before python check"

if [ -z "$CASSANDRA_HOME_DIR" ]; then
    echo "Error: CASSANDRA_HOME_DIR is not set. Exiting."
    exit 1
fi

if [ -z "$YCSB_HOME_DIR" ]; then
    echo "Error: YCSB_HOME_DIR is not set. Exiting."
    exit 1
fi

validate_cassandra_version "$CASSANDRA_HOME_DIR" "$DB_TYPE"

