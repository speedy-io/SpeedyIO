#!/bin/bash

#set -e
#set -x # TODO: Remove

# Ensure LOG_DIR is passed as an argument to the script
if [ -z "$1" ]; then
    echo "Usage: $0 <LOG_DIR>"
    exit 1
fi

LOG_DIR=$1

# Global variable for BCC tools directory
BCC_TOOLS_DIR="/usr/share/bcc/tools/"

# Configurable temporary directory for storing PID files
TMP_DIR="/tmp"  # You can change this to any directory you prefer

# Declare associative array to store PID files
declare -A PID_FILES

# Function to run a command with sudo, capture its PID, and store it in a file
run_with_sudo_and_get_pid() {
    local command_string="$1"
    local pid_file="$2"

    # Run the command with sudo and capture the PID
    sudo bash -c "$command_string & echo \$! > '$pid_file'"
}

# Function to run a BCC tool and log its output
run_bcc_tool() {
    local tool_name="$1"
    local flags="$2"
    local log_file="$3"
    local pid_var_name="$4"

    # Ensure BCC_TOOLS_DIR is set
    if [ -z "$BCC_TOOLS_DIR" ]; then
        echo "Error: BCC_TOOLS_DIR is not set."
        exit 1
    fi

    # Generate a unique PID file name
    local pid_file="${TMP_DIR}/pid_${tool_name}_$$_$RANDOM"

    # Build the command to run
    local command="${BCC_TOOLS_DIR}${tool_name} ${flags}"

    # Run the command and capture the PID
    run_with_sudo_and_get_pid "${command} >> '${log_file}' 2>&1" "$pid_file"

    # Wait briefly to ensure the PID file is created
    local retries=5
    while [ ! -f "$pid_file" ] && [ $retries -gt 0 ]; do
        sleep 0.5
        ((retries--))
    done

    if [ ! -f "$pid_file" ]; then
        echo "Error: Failed to retrieve PID for ${tool_name}."
        exit 1
    fi

    # Read the PID from the PID file
    local pid
    pid=$(cat "$pid_file")

    # Assign the PID to the variable name passed
    printf -v "$pid_var_name" '%d' "$pid"

    # Print log message
    echo "Started ${tool_name} with PID: $pid, PID stored in file: $pid_file"

    # Store PID file paths for cleanup
    PID_FILES["$pid"]="$pid_file"
}

# Function to start monitoring with BCC tools
monitor_bcc_tools() {
    echo "Starting BCC monitoring..."

    # Define file paths for logging
    local FILETOP_LOG="$LOG_DIR/filetop.txt"
    local FILEGONE_LOG="$LOG_DIR/filegone.txt"
    local SYNCSNOOP_LOG="$LOG_DIR/syncsnoop.txt"
    local OPENSNOOP_LOG="$LOG_DIR/opensnoop.txt"
    local SYSCOUNT_LOG="$LOG_DIR/syscount.txt"

    # Run all required BCC tools using the run_bcc_tool function
    run_bcc_tool "filetop" "1 --noclear" "$FILETOP_LOG" FILETOP_PID
    run_bcc_tool "filegone" "" "$FILEGONE_LOG" FILEGONE_PID
    run_bcc_tool "syncsnoop" "" "$SYNCSNOOP_LOG" SYNCSNOOP_PID
    run_bcc_tool "opensnoop" "--extended_fields --timestamp " "$OPENSNOOP_LOG" OPENSNOOP_PID
    run_bcc_tool "syscount" "--latency --milliseconds --top 100 --interval 1" "$SYSCOUNT_LOG" SYSCOUNT_PID

    # Store all tool PIDs for later termination
    PIDS=("$FILETOP_PID" "$FILEGONE_PID" "$SYNCSNOOP_PID" "$OPENSNOOP_PID" "$SYSCOUNT_PID")
    echo "BCC Monitoring PIDs: ${PIDS[*]}"
}

# Function to stop monitoring with BCC tools
stop_bcc_monitoring() {
    echo "Sleeping 5 seconds to allow bcc processes to print data"
    sleep 5
    echo "Stopping BCC monitoring..."
    for pid in "${!PID_FILES[@]}"; do
        echo "Stopping BCC Monitoring PID: $pid"

        # Using -2 flag here because some tools like syscount will only flush output when receiving SIGINT (Ctrl+C)
        # but may not respond properly to SIGTERM (which is the default kill signal)
        # Send SIGTERM to the process
        # Note: Do not use kill -2 here to send SIGINT
        sudo kill "$pid"

        # Wait until the process has terminated
        while sudo kill -0 "$pid" 2>/dev/null; do
            sleep 0.1
        done

        # Remove PID file
        sudo rm -f "${PID_FILES[$pid]}"

        echo "BCC Monitoring PID $pid has been stopped."
    done
}

# Handle script termination gracefully
bcc_cleanup() {
    stop_bcc_monitoring
    echo "BCC Monitoring Script terminated."
    exit 0
}


# Check if the script is being executed directly or sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "Script executed directly."
    # Set up signal handling
    trap bcc_cleanup SIGINT SIGTERM

    # Start monitoring
    monitor_bcc_tools

    # Wait for all background processes to finish
    wait
else
    echo "monitor_bcc_tools.sh sourced. Functions loaded but not executed."
fi


