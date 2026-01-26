#!/bin/bash

# Print CSV header
echo "timestamp,MemTotal_kB,MemAvailable_kB,Active_anon_kB,Active_file_kB,Inactive_anon_kB,Inactive_file_kB,Dirty_kB"

while true; do
    awk '
        /MemTotal:/             {mem_total_kb = $2}
        /MemAvailable:/         {mem_available_kb = $2}
        /Active\(anon\):/       {active_anon_kb = $2}
        /Active\(file\):/       {active_file_kb = $2}
        /Inactive\(anon\):/     {inactive_anon_kb = $2}
        /Inactive\(file\):/     {inactive_file_kb = $2}
        /^Dirty:/               {dirty_kb = $2}
        END {
            cmd = "date +\"%Y-%m-%d %H:%M:%S\""
            cmd | getline timestamp
            close(cmd)
            printf "%s,%d,%d,%d,%d,%d,%d,%d\n", timestamp, mem_total_kb, mem_available_kb, active_anon_kb, active_file_kb, inactive_anon_kb, inactive_file_kb, dirty_kb
        }
    ' /proc/meminfo
    sleep 1
done
