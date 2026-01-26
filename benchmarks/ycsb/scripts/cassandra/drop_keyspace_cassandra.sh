#!/usr/bin/env bash

set -e

# ------------------------------------------------------------------------------
# This script connects to Cassandra via cqlsh and drops the 'ucsb_keyspace'.
# Make sure cqlsh is installed and accessible in your $PATH.
#
# Usage:
#   ./drop_ucsb_keyspace.sh /path/to/cassandra/data
#
# Arguments:
#   1. CASSANDRA_DATA_FOLDER - Path to Cassandra's data directory
#
# Requirements:
#   - Ensure Cassandra is running before executing this script.
#   - `nodetool` must be available in the PATH to check Cassandra's status.
# ------------------------------------------------------------------------------

KEYSPACE="ucsb_keyspace"

# Check if the script received an argument for the Cassandra data folder
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <CASSANDRA_DATA_FOLDER>" >&2
    exit 1
fi

CASSANDRA_DATA_FOLDER="$1"

# Validate if the folder exists
# if [ ! -d "$CASSANDRA_DATA_FOLDER" ]; then
#     echo "Error: Directory CASSANDRA_DATA_FOLDER:$CASSANDRA_DATA_FOLDER does not exist. Did you specify the correct path?" >&2
#     exit 1
# fi

# Check if Cassandra is running using nodetool status
if ! nodetool status > /dev/null 2>&1; then
    echo "Error: Cassandra is not running. Please start Cassandra and try again." >&2
    exit 1
fi

# If Cassandra is running on a non-default host or port, specify with -u (host)
# and -p (port). For example:
#   HOST="1.2.3.4"
#   PORT=9042
#   cqlsh $HOST $PORT -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"

echo "Dropping keyspace: $KEYSPACE"
if cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"; then
    echo "Keyspace '$KEYSPACE' dropped successfully."
else
    echo "Failed to drop keyspace '$KEYSPACE'. Check the logs or cqlsh output for more details." >&2
    exit 1
fi

#  By default, Cassandra takes a snapshot of the data before dropping a keyspace or a table, and the on-disk directory
#  structure can remain (with or without leftover files). Specifically, Cassandra has an auto_snapshot setting
#  (in cassandra.yaml) which is enabled by default. When you drop a keyspace/table, Cassandra automatically creates
#  a snapshot to protect you from accidental data loss.

# Run nodetool clearsnapshot to remove all snapshots from every keyspace and
# table (or use the -t option to remove a specific snapshot).

echo "Clearing all Cassandra snapshots..."
if nodetool clearsnapshot; then
    echo "Snapshots cleared successfully."
else
    echo "Warning: Failed to clear snapshots. Ensure nodetool is available and Cassandra is running." >&2
fi

# Construct the keyspace data folder path
KEYSPACE_DATA_FOLDER="$CASSANDRA_DATA_FOLDER/$KEYSPACE"

# Ensure the keyspace data folder exists before attempting deletion
if [ -d "$KEYSPACE_DATA_FOLDER" ]; then
    echo "Deleting data folder for keyspace: $KEYSPACE"
    rm -rfv "$KEYSPACE_DATA_FOLDER"
    echo "Data folder removed successfully."
else
    echo "Warning: Data folder for keyspace '$KEYSPACE' not found. It may have been deleted already."
fi

echo "Cleanup completed."

