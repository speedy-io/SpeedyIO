#!/usr/bin/env python3

import sys
from collections import defaultdict

def parse_syscount_output(filename):
    """
    Parses the syscount output file and returns a dictionary with syscall names,
    their aggregated counts, and total time in milliseconds.

    This function:
    1. Skips all lines until it encounters the "Tracing syscalls" line.
    2. Ensures the column headers are 'SYSCALL', 'COUNT', and 'TIME (ms)'.
    3. Aggregates counts and sums up the times for repeated syscalls.
    4. Ignores timestamp lines (e.g., '[10:43:58]') and empty lines.

    Args:
        filename (str): Path to the syscount output file.

    Returns:
        dict: A dictionary where the keys are syscall names and the values are
              dictionaries containing 'count' and 'total_time_ms'.
    """
    syscalls = defaultdict(lambda: {'count': 0, 'total_time_ms': 0.0})
    tracing_started = False

    with open(filename, 'r') as f:
        for line in f:
            # Skip empty lines
            if not line.strip():
                continue

            # Look for the "Tracing syscalls" line to start processing
            if "Tracing syscalls" in line:
                tracing_started = True
                continue  # Skip the "Tracing syscalls" line

            # If tracing hasn't started, warn about random output
            if not tracing_started:
                print("Warning: Random output encountered before 'Tracing syscalls' line.")
                continue

            # Skip timestamp lines (e.g., '[10:43:58]')
            if line.startswith('[') and line.endswith(']\n'):
                continue

            # Handle and validate the column header line
            if line.strip() == "SYSCALL                   COUNT        TIME (ms)":
                # Assert that the column headers are correct
                assert line.split() == ["SYSCALL", "COUNT", "TIME", "(ms)"], \
                    "Error: Unexpected column headers. Ensure correct syscount flags are used."
                continue  # Skip the header line

            # Parse lines containing syscall data
            parts = line.split()
            if len(parts) < 3:
                print(f"Skipping malformed line: {line.strip()}")
                continue  # Ignore lines that don't have enough columns

            # Validate that the count and time are numeric
            try:
                count = int(parts[1])
                time_ms = float(parts[2])
            except ValueError:
                print(f"Skipping malformed line with non-numeric data: {line.strip()}")
                continue  # Ignore lines with invalid numeric data

            # Extract syscall name and aggregate count and time
            syscall_name = parts[0]
            syscalls[syscall_name]['count'] += count
            syscalls[syscall_name]['total_time_ms'] += time_ms

    return syscalls

def print_syscall_stats(filename):
    """
    Parses the syscount output file, sorts the syscalls by count in decreasing order,
    and prints the result.
    """
    syscalls = parse_syscount_output(filename)

    # Sort syscalls by 'count' in decreasing order
    sorted_syscalls = sorted(syscalls.items(), key=lambda x: x[1]['count'], reverse=True)

    # Print the sorted syscalls
    print(f"{'Syscall':<20} {'Count':<10} {'Total Time (ms)':<15}")
    print("=" * 50)
    for syscall, data in sorted_syscalls:
        print(f"{syscall:<20} {data['count']:<10} {data['total_time_ms']:<15.2f}")

if __name__ == "__main__":
    # If the script is executed directly, get the filename from command-line arguments
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <syscount_output_file>")
        sys.exit(1)

    filename = sys.argv[1]
    print_syscall_stats(filename)