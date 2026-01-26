#!/bin/bash

# Print CSV header
echo "timestamp,pgscan_kswapd,pgscan_direct,pgsteal_kswapd,pgsteal_direct,nr_dirty,nr_writeback,workingset_refault_file,allocstall_dma,allocstall_dma32,allocstall_normal,allocstall_movable"

while true; do
    awk '
    BEGIN {
        # Initialize all fields to -1 (to mark missing values)
        pgscan_kswapd = -1
        pgscan_direct = -1
        pgsteal_kswapd = -1
        pgsteal_direct = -1
        nr_dirty = -1
        nr_writeback = -1
        workingset_refault_file = -1
        allocstall_dma = -1
        allocstall_dma32 = -1
        allocstall_normal = -1
        allocstall_movable = -1
    }

    /^pgscan_kswapd /            {pgscan_kswapd = $2}
    /^pgscan_direct /            {pgscan_direct = $2}
    /^pgsteal_kswapd /           {pgsteal_kswapd = $2}
    /^pgsteal_direct /           {pgsteal_direct = $2}
    /^nr_dirty /                 {nr_dirty = $2}
    /^nr_writeback /             {nr_writeback = $2}
    /^workingset_refault_file /  {workingset_refault_file = $2}
    /^allocstall_dma /           {allocstall_dma = $2}
    /^allocstall_dma32 /         {allocstall_dma32 = $2}
    /^allocstall_normal /        {allocstall_normal = $2}
    /^allocstall_movable /       {allocstall_movable = $2}

    END {
        cmd = "date +\"%Y-%m-%d %H:%M:%S\""
        cmd | getline ts
        close(cmd)
        printf "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", ts, pgscan_kswapd, pgscan_direct, pgsteal_kswapd, pgsteal_direct, nr_dirty, nr_writeback, workingset_refault_file, allocstall_dma, allocstall_dma32, allocstall_normal, allocstall_movable
    }
    ' /proc/vmstat
    sleep 1
done
