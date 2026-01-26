#!/bin/bash

#set -e

# Unset flags if needed
set +e  # Disable exit on error
set +x  # Disable command tracing

# Accept LOG_DIR as a parameter
LOG_DIR=$1

# Define file paths for logging
MEMORY_LOG="$LOG_DIR/memory_usage.json"
DISK_USAGE_LOG="$LOG_DIR/disk_usage.json"
DISK_IO_LOG="$LOG_DIR/disk_io_usage.json"
NETWORK_USAGE_LOG="$LOG_DIR/network_usage.json"

# Function to get the current timestamp
get_timestamp() {
    date +%Y-%m-%dT%H:%M:%S
}

# Function to check if all required commands are available
check_required_commands() {
    # For centos: sudo dnf update -y && sudo dnf install htop nmon vim tree sysstat jq -y

    REQUIRED_COMMANDS=("free" "df" "iostat" "jq" "sar" "awk" "sync" "sudo")
    MISSING_COMMANDS=0

    for cmd in "${REQUIRED_COMMANDS[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            echo "monitor_system_usage.sh : Error: Required command '$cmd' is not available."
            MISSING_COMMANDS=$((MISSING_COMMANDS + 1))
        fi
    done

    if [ "$MISSING_COMMANDS" -ne 0 ]; then
        echo "montior_system_usage.sh : One or more required commands are missing. Exiting."
        exit 1
    fi
}

# Function to start monitoring system usage
start_monitor_system_usage() {
    # Check if all required commands are available
    check_required_commands

    echo "Starting monitoring..."

    # Memory usage
    while true; do
        TIMESTAMP=$(get_timestamp)
        MEMORY=$(free -m | awk -v ts="$TIMESTAMP" 'NR==2 {printf "{\"timestamp\": \"%s\", \"values\": [{\"total\": %s, \"used\": %s, \"free\": %s}]}\n", ts, $2, $3, $4}')
        echo "$MEMORY" >> "$MEMORY_LOG"
        sleep 1
    done &
    MEMORY_PID=$!

    # Disk usage
    while true; do
        TIMESTAMP=$(get_timestamp)
        DISK=$(df -h --output=source,fstype,size,used,avail,pcent | tail -n +2 | awk -v ts="$TIMESTAMP" 'BEGIN {printf "{\"timestamp\": \"%s\", \"values\": [", ts; first = 1} {if (!first) printf ", "; first = 0; printf "{\"source\": \"%s\", \"fstype\": \"%s\", \"size\": \"%s\", \"used\": \"%s\", \"avail\": \"%s\", \"pcent\": \"%s\"}", $1, $2, $3, $4, $5, $6} END {print "]}" }')
        echo "$DISK" >> "$DISK_USAGE_LOG"
        sleep 1
    done &
    DISK_PID=$!

    # Disk I/O usage
    while true; do
        TIMESTAMP=$(get_timestamp)
        IO=$(iostat -dxmty -o JSON 1 1)
        IO_DISKS=$(echo "$IO" | jq -c --arg ts "$TIMESTAMP" '.sysstat.hosts[0].statistics[0].disk | {"timestamp": $ts, "values": .}')
        echo "$IO_DISKS" >> "$DISK_IO_LOG"
        sleep 1
    done &
    IO_PID=$!

    # Network usage
    while true; do
        TIMESTAMP=$(get_timestamp)
        NETWORK=$(sar -n DEV 1 1 | awk -v ts="$TIMESTAMP" 'BEGIN {printf "{\"timestamp\": \"%s\", \"values\": [", ts; first = 1} NR>3 && $2 ~ /^[a-z]/ {if (!first) printf ", "; first = 0; printf "{\"interface\": \"%s\", \"rxpck/s\": \"%s\", \"txpck/s\": \"%s\", \"rxkB/s\": \"%s\", \"txkB/s\": \"%s\"}", $2, $3, $4, $5, $6} END {print "]}" }')
        echo "$NETWORK" >> "$NETWORK_USAGE_LOG"
        sleep 1
    done &
    NETWORK_PID=$!

    MONITOR_SYSTEM_USAGE_PIDS=("$MEMORY_PID" "$DISK_PID" "$IO_PID" "$NETWORK_PID")
    echo "Monitoring PIDs: ${MONITOR_SYSTEM_USAGE_PIDS[*]}"
}

# Function to stop monitoring system usage
stop_monitor_system_usage() {
    echo "Stopping monitor system usage script..."
    for pid in "${MONITOR_SYSTEM_USAGE_PIDS[@]}"; do
        echo "stop_monitor_system_usage.sh : Stopping PID: $pid"
        kill "$pid"
        wait "$pid"
        echo "stop_monitor_system_usage.sh : PID $pid has been stopped."
    done
}

clear_cache() {
  echo "Clearing cache"

  sync
  sleep 10
  sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
  sleep 10

  echo "Finished clearing cache"
}
