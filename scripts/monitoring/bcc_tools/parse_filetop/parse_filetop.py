# Example filetop command:
# sudo /usr/share/bcc/tools/filetop 1 --noclear --maxrows 1000000
# When running benchmarks, set a high maxrows value like 1e5 so that data doesn't get dropped


import argparse
import re
from collections import defaultdict
from humanfriendly import format_size
from tabulate import tabulate


def parse_filetop_output(filename, filter_type, suffixes):
    file_metrics = defaultdict(lambda: {"READS": 0, "WRITES": 0, "R_Kb": 0, "W_Kb": 0})
    suffixes = suffixes.split(",") if suffixes else []

    with open(filename, 'r') as file:
        lines = file.readlines()

    # Regex to match the filetop data rows
    pattern = re.compile(r'^\d+\s+\S+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\S+\s+(\S+)$')

    for line in lines:
        match = pattern.match(line)
        if match:
            reads, writes, r_kb, w_kb, file_name = map(str.strip, match.groups())
            reads, writes, r_kb, w_kb = int(reads), int(writes), int(r_kb), int(w_kb)

            if suffixes and not any(file_name.endswith(suffix) for suffix in suffixes):
                continue

            if filter_type == "reads":
                file_metrics[file_name]["READS"] += reads
                file_metrics[file_name]["R_Kb"] += r_kb
            elif filter_type == "writes":
                file_metrics[file_name]["WRITES"] += writes
                file_metrics[file_name]["W_Kb"] += w_kb
            else:  # filter_type == "both"
                file_metrics[file_name]["READS"] += reads
                file_metrics[file_name]["WRITES"] += writes
                file_metrics[file_name]["R_Kb"] += r_kb
                file_metrics[file_name]["W_Kb"] += w_kb

    # Convert to sorted list based on R_Kb, then W_Kb
    sorted_metrics = sorted(file_metrics.items(), key=lambda x: (x[1]["R_Kb"], x[1]["W_Kb"]), reverse=True)

    return sorted_metrics


def main():
    parser = argparse.ArgumentParser(description="Parse filetop output and compute metrics.")
    parser.add_argument("filename", type=str, help="Path to the filetop output file.")
    parser.add_argument(
        "--filter_type", type=str, choices=["reads", "writes", "both"], default="both",
        help="Filter for only reads, writes, or both."
    )
    parser.add_argument(
        "--suffixes", type=str, default="",
        help="Comma-separated list of file suffixes to filter. (e.g., .txt,.log)"
    )

    args = parser.parse_args()
    sorted_metrics = parse_filetop_output(args.filename, args.filter_type, args.suffixes)

    # Prepare table data
    table_data = []
    total_reads, total_writes, total_r_kb, total_w_kb = 0, 0, 0, 0

    if args.filter_type == "reads":
        headers = ["FILE (Max 50 chars)", "READS", "R_Kb", "Reads (HumanFriendly)"]
    elif args.filter_type == "writes":
        headers = ["FILE (Max 50 chars)", "WRITES", "W_Kb", "Writes (HumanFriendly)"]
    else:  # both
        headers = [
            "FILE (Max 50 chars)", "READS", "WRITES",
            "R_Kb", "W_Kb", "Reads (HumanFriendly)", "Writes (HumanFriendly)"
        ]

    for file_name, metrics in sorted_metrics:
        truncated_file_name = file_name[:50]  # Limit filename to 50 characters
        reads_hf = format_size(metrics["R_Kb"] * 1024)  # Convert KB to bytes for humanfriendly
        writes_hf = format_size(metrics["W_Kb"] * 1024)

        total_reads += metrics["READS"]
        total_writes += metrics["WRITES"]
        total_r_kb += metrics["R_Kb"]
        total_w_kb += metrics["W_Kb"]

        if args.filter_type == "reads":
            table_data.append([truncated_file_name, metrics["READS"], metrics["R_Kb"], reads_hf])
        elif args.filter_type == "writes":
            table_data.append([truncated_file_name, metrics["WRITES"], metrics["W_Kb"], writes_hf])
        else:  # both
            table_data.append([
                truncated_file_name, metrics["READS"], metrics["WRITES"],
                metrics["R_Kb"], metrics["W_Kb"], reads_hf, writes_hf
            ])

    # Add totals row
    if args.filter_type == "reads":
        table_data.append(["TOTAL", total_reads, total_r_kb, format_size(total_r_kb * 1024)])
    elif args.filter_type == "writes":
        table_data.append(["TOTAL", total_writes, total_w_kb, format_size(total_w_kb * 1024)])
    else:  # both
        table_data.append([
            "TOTAL", total_reads, total_writes,
            total_r_kb, total_w_kb,
            format_size(total_r_kb * 1024), format_size(total_w_kb * 1024)
        ])

    # Print the table using tabulate
    print(tabulate(table_data, headers=headers, tablefmt="heavy_outline"))


if __name__ == "__main__":
    main()