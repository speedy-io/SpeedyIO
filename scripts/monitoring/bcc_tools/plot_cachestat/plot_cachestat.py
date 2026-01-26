# Example cachestat command:
#   sudo /usr/share/bcc/tools/cachestat --timestamp 1 > cachestat_output.txt
# Example command to run in background (to install the "unbuffer" command, do "sudo yum install expect"):
#   sudo unbuffer /usr/share/bcc/tools/cachestat --timestamp 1 > cachestat_output.txt &
# Use the following command to kill the cachestat command (apart from manually Ctrl+c):
#   sudo pkill -f "python3 /usr/share/bcc/tools/cachestat" || true
#   The " || true" condition is there for the line to work in bash scripts where set -e
#   is enabled, to take care of the exit code where no cachestat process was running.


"""
System metrics to PDF

Parses and visualizes:
  - cachestat logs
  - meminfo (CSV with timestamp)
  - vmstat  (CSV with timestamp)
  - nodetool compactionstats
  - nodetool tablestats  (SSTables in each level)
  - nodetool tablehistograms (SSTables percentiles)

Key design goals:
  * Robust parsers:
      - tolerate trailing "header-only" blocks when the logger was killed
      - ignore connection/refusal noise and shutdown/stacktrace failure sections
      - strict validation where requested (e.g., expected keyspace/table)
  * PDF pages auto-size to content (width & height) for preformatted tables/text
  * Plots grow in height to fit large legends; heavy line art can be simplified
    and rasterized to keep file size reasonable
  * Vector text whenever possible; no pagination per the requirements

Author: <you>
"""

import argparse
import os
import re
import datetime as dt
import math
from io import StringIO

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from tabulate import tabulate
from tqdm import tqdm
from typing import Optional
import textwrap

# -----------------------------------------------------------------------------
# Matplotlib defaults: small PDFs, vector text
# -----------------------------------------------------------------------------
mpl.rcParams.update({
    "pdf.compression": 9,          # max stream compression
    "pdf.use14corefonts": True,    # use core PDF fonts (ASCII/Latin-1)
    "font.family": "monospace",    # readable for preformatted tables
    "savefig.dpi": 150,            # affects only rasterized artists
})

# -----------------------------------------------------------------------------
# Constants / Config
# -----------------------------------------------------------------------------
# Fields to subtract initial value from, per source type
SUBTRACT_FROM_START = {
    "meminfo": [],  # Nothing for meminfo
    "proc_vmstat": [
        "pgscan_kswapd", "pgscan_direct", "pgsteal_kswapd", "pgsteal_direct",
        "allocstall_dma", "allocstall_dma32", "allocstall_normal", "allocstall_movable",
        "workingset_refault_file",
    ],
    "file_values": [
        "/sys/kernel/mm/transparent_hugepage/khugepaged/pages_collapsed",
        # Add more file paths here if they are monotonic and should be shifted to start at 0
    ],
}


# Lines that indicate a nodetool/Cassandra failure block which we should skip.
FAIL_LINE_RE = re.compile(
    r"(Cassandra has shutdown\."
      r"|^\[WARN\]\s*nodetool.*failed\b"
      r"|^error:\s*null\b"
      r"|^--\s*StackTrace\s*--"
      r"|^\s*at\s+\S+)",   # stacktrace: "at com.foo.Bar ..."
    re.IGNORECASE
)

# Fixed colors for CPU stacked charts (consistent across pages)
CPU_COLOR_MAP = {
    "us": "#1f77b4",   # blue
    "sy": "#ff7f0e",   # orange
    "id": "#2ca02c",   # green
    "wa": "#d62728",   # red
    "st": "#9467bd",   # purple
}

# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------
def _ignore_common_noise(line: str) -> bool:
    """Filter routine noise lines found across nodetool outputs."""
    s = line.strip()
    if not s:
        return False
    if "Detached" in s or "Terminated" in s:
        return True
    if s.startswith("nodetool:") and "Failed to connect" in s:
        return True
    if "ConnectException" in s and "Failed to connect" in s:
        return True
    if "Connection refused" in s:
        return True
    return False


def _read_filtered_lines(path: str) -> list[str]:
    """Read a text file and drop common noise lines (keeps blank lines)."""
    with open(path, "r") as f:
        raw = f.readlines()
    return [ln.rstrip("\n") for ln in raw if not _ignore_common_noise(ln)]


def _block_has_failure(lines: list[str], start_idx: int, header_re: re.Pattern) -> bool:
    """
    Return True if any line from start_idx up to (but not including) the next header
    matches FAIL_LINE_RE. Used to skip shutdown/stacktrace failure sections entirely.
    """
    k = start_idx
    while k < len(lines) and not header_re.match(lines[k].strip()):
        if FAIL_LINE_RE.search(lines[k]):
            return True
        k += 1
    return False


def _safe_tight_layout(fig: plt.Figure, rect=None, pad=1.08) -> None:
    """
    Run tight_layout without tripping the Agg 2^16 px limit.
    If pixel size would exceed the cap, temporarily lower DPI for layout.
    """
    MAX = (1 << 16) - 1024  # safety margin under 65536
    wpx = fig.get_figwidth() * fig.dpi
    hpx = fig.get_figheight() * fig.dpi
    if wpx >= MAX or hpx >= MAX:
        scale = 0.98 * min(MAX / max(1, wpx), MAX / max(1, hpx))
        fig._orig_dpi = getattr(fig, "_orig_dpi", fig.dpi)
        fig.set_dpi(fig.dpi * scale)
    try:
        if rect is None:
            fig.tight_layout(pad=pad)
        else:
            fig.tight_layout(rect=rect, pad=pad)
    except Exception:
        pass
    # restore DPI if we changed it for layout
    if hasattr(fig, "_orig_dpi"):
        fig.set_dpi(fig._orig_dpi)
        delattr(fig, "_orig_dpi")


def _decimate_xy(x: np.ndarray, y: np.ndarray, max_points: Optional[int]):
    """Uniformly sample down to at most max_points to reduce vector size."""
    if max_points is None or len(x) <= max_points:
        return x, y
    idx = np.linspace(0, len(x) - 1, max_points).astype(int)
    return x[idx], y[idx]


# -----------------------------------------------------------------------------
# Parsers
# -----------------------------------------------------------------------------
def parse_cachestat_file(filename: str) -> pd.DataFrame:
    """
    Parse cachestat output (as printed by bcc's cachestat).

    Expected input includes a header line with TIME column, then HH:MM:SS rows.
    Handles day rollover for TIME to compute a monotonic 'elapsed' in seconds.
    """
    with open(filename, "r") as f:
        lines = f.readlines()

    header_line = None
    data_lines = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if header_line is None and "TIME" in s and "HITS" in s:
            header_line = s
            continue
        if re.match(r"^\d{2}:\d{2}:\d{2}", s):
            data_lines.append(s)
        elif "Detached" in s or "Terminated" in s:
            continue

    if header_line is None:
        raise ValueError(f"{filename}: missing cachestat header with TIME column")

    combined = header_line + "\n" + "\n".join(data_lines)
    df = pd.read_csv(StringIO(combined), sep=r"\s+")

    if "TIME" not in df.columns:
        raise ValueError(f"{filename}: TIME column not found")

    if "HITRATIO" in df.columns:
        df["HITRATIO"] = df["HITRATIO"].astype(str).str.rstrip("%").astype(float)

    # Make numeric where applicable
    for col in ["HITS", "MISSES", "DIRTIES", "BUFFERS_MB", "CACHED_MB"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build elapsed with 24h rollover handling
    times = []
    for t in df["TIME"]:
        h, m, s = map(int, t.split(":"))
        times.append(h * 3600 + m * 60 + s)

    elapsed = []
    base = times[0]
    prev = base
    cum_offset = 0
    for t in times:
        if t < prev:
            cum_offset += 24 * 3600
        elapsed.append(t - base + cum_offset)
        prev = t
    df["elapsed"] = elapsed
    return df


def parse_timestamped_file(filename: str) -> pd.DataFrame:
    """
    Parse a CSV file that includes a 'timestamp' column.
    Drops lines containing 'Terminated' (common shutdown noise).
    """
    with open(filename, "r") as f:
        lines = [line for line in f if "Terminated" not in line]
    df = pd.read_csv(StringIO("".join(lines)))
    if "timestamp" not in df.columns:
        raise ValueError(f"'timestamp' column not found in {filename}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    return df


def parse_compactionstats_file(filename: str, expected_table: str = "ycsb.usertable") -> pd.DataFrame:
    """
    Parse nodetool compactionstats sampled blocks.
    Output columns: timestamp, pending_tasks, active_compactions, elapsed (s)

    Validations:
      - Pending subtotals must reference only expected_table
      - Sum for expected_table must equal total pending tasks

    Tolerances:
      - Trailing 'header-only' blocks are skipped
      - Cassandra/nodetool failure blocks (shutdown/stacktrace) are skipped
      - Connection-refused/noise lines are ignored
      - If all blocks are skipped, returns an empty DataFrame with expected columns
    """
    lines = _read_filtered_lines(filename)

    records = []
    i = 0
    header_re = re.compile(r"^===== compactionstats @ (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) =====$")
    pending_re = re.compile(r"^pending tasks:\s*(\d+)\s*$")
    sublist_re = re.compile(r"^\-\s+([A-Za-z0-9_.\-]+)\s*:\s*(\d+)\s*$")

    while i < len(lines):
        m = header_re.match(lines[i].strip())
        if not m:
            i += 1
            continue

        ts_str = m.group(1)
        ts = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        i += 1

        # Skip blank lines
        j = i
        while j < len(lines) and lines[j].strip() == "":
            j += 1

        # Header-only (EOF or next header)
        if j >= len(lines) or header_re.match(lines[j].strip()):
            i = j
            continue

        # Failure block? skip to next header
        if _block_has_failure(lines, j, header_re):
            i = j
            while i < len(lines) and not header_re.match(lines[i].strip()):
                i += 1
            continue

        if not pending_re.match(lines[i].strip()):
            raise ValueError(f'{filename}: {ts_str}: expected "pending tasks: <N>" after header')
        pending_total = int(pending_re.match(lines[i].strip()).group(1))
        i += 1

        # Parse per-table pending list
        ycsb_sum = 0
        other_seen = []
        while i < len(lines) and lines[i].strip().startswith("-"):
            sm = sublist_re.match(lines[i].strip())
            if not sm:
                raise ValueError(f"{filename}: {ts_str}: malformed pending sublist line: {lines[i]!r}")
            name, cnt = sm.group(1), int(sm.group(2))
            if name == expected_table:
                ycsb_sum += cnt
            else:
                other_seen.append(name)
            i += 1

        if other_seen:
            raise ValueError(f"{filename}: {ts_str}: pending list includes non-target table(s): {', '.join(sorted(set(other_seen)))}")

        if ycsb_sum != pending_total:
            raise ValueError(f"{filename}: {ts_str}: {expected_table} count {ycsb_sum} != total pending tasks {pending_total}")

        # Skip blanks
        while i < len(lines) and lines[i].strip() == "":
            i += 1

        # Optional active table header
        if i < len(lines) and re.match(r"^\s*id\s+compaction type\s+keyspace\s+table\s+completed", lines[i]):
            i += 1

        # Count rows until "Active compaction remaining time" or next header
        active_count = 0
        while i < len(lines):
            s = lines[i].strip()
            if s.startswith("Active compaction remaining time"):
                i += 1
                break
            if header_re.match(s):
                break
            if s:
                active_count += 1
            i += 1

        records.append({
            "timestamp": ts,
            "pending_tasks": pending_total,
            "active_compactions": active_count,
        })

    if not records:
        return pd.DataFrame(columns=["timestamp", "pending_tasks", "active_compactions", "elapsed"])

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    return df


def parse_tablestats_file(filename: str, expected_keyspace: str = "ycsb", expected_table: str = "usertable") -> pd.DataFrame:
    """
    Parse nodetool tablestats blocks (keyspace=ycsb).
    Extract 'SSTables in each level' for the expected table.

    Output columns: timestamp, sstables_in_each_level (raw string), elapsed (s)

    Validations:
      - Header keyspace (if present) and any 'Keyspace :' lines must be 'ycsb'
      - Must find 'SSTables in each level' inside table 'usertable'

    Tolerances:
      - Trailing header-only and failure blocks are skipped
      - Connection-refused/noise lines are ignored
      - If all blocks are skipped, returns an empty DataFrame with expected columns
    """
    lines = _read_filtered_lines(filename)

    records = []
    i = 0

    header_re = re.compile(r"^===== tablestats(?:\s+keyspace=([^\s]+))?\s*@\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) =====$")
    keyspace_line_re = re.compile(r"^Keyspace\s*:\s*([^\s]+)\s*$")
    table_line_re = re.compile(r"^\s*Table\s*:\s*([^\s]+)\s*$")
    levels_re = re.compile(r"^\s*SSTables in each level:\s*(\[[^\]]*\])\s*$")

    while i < len(lines):
        m = header_re.match(lines[i].strip())
        if not m:
            i += 1
            continue

        header_keyspace = m.group(1)
        ts_str = m.group(2)
        ts = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        i += 1

        # Skip blank lines
        j = i
        while j < len(lines) and lines[j].strip() == "":
            j += 1

        # Header-only section?
        if j >= len(lines) or header_re.match(lines[j].strip()):
            i = j
            continue

        # Failure block? skip
        if _block_has_failure(lines, j, header_re):
            i = j
            while i < len(lines) and not header_re.match(lines[i].strip()):
                i += 1
            continue

        if header_keyspace and header_keyspace != expected_keyspace:
            raise ValueError(f"{filename}: {ts_str}: header keyspace={header_keyspace!r} != expected {expected_keyspace!r}")

        seen_other_keyspaces = set()
        in_expected_table = False
        found_levels = None

        # Consume until next header or EOF
        while i < len(lines):
            s = lines[i].strip()

            if header_re.match(s):
                break

            if s.startswith("-----") or s.startswith("Total number of tables:"):
                i += 1
                continue

            km = keyspace_line_re.match(s)
            if km:
                ks = km.group(1)
                if ks != expected_keyspace:
                    seen_other_keyspaces.add(ks)
                in_expected_table = False
                i += 1
                continue

            tm = table_line_re.match(s)
            if tm:
                in_expected_table = (tm.group(1) == expected_table)
                i += 1
                continue

            lm = levels_re.match(s)
            if lm and in_expected_table:
                found_levels = lm.group(1)
                i += 1
                continue

            i += 1

        if seen_other_keyspaces:
            raise ValueError(f"{filename}: {ts_str}: found non-target keyspace(s): {', '.join(sorted(seen_other_keyspaces))}")

        if not found_levels:
            raise ValueError(f"{filename}: {ts_str}: missing 'SSTables in each level' for table '{expected_table}' in keyspace '{expected_keyspace}'")

        records.append({
            "timestamp": ts,
            "sstables_in_each_level": found_levels,
        })

    if not records:
        return pd.DataFrame(columns=["timestamp", "sstables_in_each_level", "elapsed"])

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    return df


def parse_tablehistograms_file(filename: str, expected_keyspace: str = "ycsb", expected_table: str = "usertable") -> pd.DataFrame:
    """
    Parse nodetool tablehistograms blocks for ycsb/usertable.
    Extract SSTables column for common percentiles.

    Output columns: timestamp, sstables_p50, p75, p95, p98, p99, min, max, elapsed (s)

    Tolerances/validations similar to the other parsers.
    """
    lines = _read_filtered_lines(filename)

    header_re = re.compile(r"^===== tablehistograms\s+([A-Za-z0-9_.\-]+)\.([A-Za-z0-9_.\-]+)\s*@\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*=====$")
    banner_re = re.compile(r"^([A-Za-z0-9_.\-]+)/([A-Za-z0-9_.\-]+)\s+histograms\s*$")
    row_re = re.compile(r"^(?P<label>(?:\d+%|Min|Max))\s+(?P<sstables>\d+(?:\.\d+)?)\b")

    records = []
    i = 0

    while i < len(lines):
        m = header_re.match(lines[i].strip())
        if not m:
            i += 1
            continue

        ks_dot, tbl_dot, ts_str = m.group(1), m.group(2), m.group(3)
        ts = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        i += 1

        if ks_dot != expected_keyspace or tbl_dot != expected_table:
            raise ValueError(f"{filename}: {ts_str}: header keyspace.table {ks_dot}.{tbl_dot} != expected {expected_keyspace}.{expected_table}")

        # Skip blanks
        j = i
        while j < len(lines) and lines[j].strip() == "":
            j += 1

        # Header-only?
        if j >= len(lines) or header_re.match(lines[j].strip()):
            i = j
            continue

        # Failure block? skip
        if _block_has_failure(lines, j, header_re):
            i = j
            while i < len(lines) and not header_re.match(lines[i].strip()):
                i += 1
            continue

        # Validate banner "ycsb/usertable histograms"
        while i < len(lines) and not lines[i].strip():
            i += 1
        if i >= len(lines):
            # nothing in this block; skip
            continue
        bm = banner_re.match(lines[i].strip())
        if not bm:
            raise ValueError(f"{filename}: {ts_str}: malformed banner: {lines[i]!r}")
        ks_slash, tbl_slash = bm.group(1), bm.group(2)
        if ks_slash != expected_keyspace or tbl_slash != expected_table:
            raise ValueError(f"{filename}: {ts_str}: banner keyspace/table {ks_slash}/{tbl_slash} != expected {expected_keyspace}/{expected_table}")
        i += 1

        # Skip header/units lines until data or next header
        while i < len(lines):
            s = lines[i].strip()
            if row_re.match(s) or header_re.match(s) or not s:
                break
            i += 1

        # Collect SSTables column values
        vals = {}
        while i < len(lines):
            s = lines[i].strip()
            if not s:
                break
            if header_re.match(s):
                break
            rm = row_re.match(s)
            if rm:
                label = rm.group("label")
                sst = float(rm.group("sstables"))
                vals[label] = sst
            i += 1

        # No percentile rows? skip block
        if not vals:
            continue

        records.append({
            "timestamp": ts,
            "sstables_p50": vals.get("50%"),
            "sstables_p75": vals.get("75%"),
            "sstables_p95": vals.get("95%"),
            "sstables_p98": vals.get("98%"),
            "sstables_p99": vals.get("99%"),
            "sstables_min": vals.get("Min"),
            "sstables_max": vals.get("Max"),
        })

    if not records:
        cols = ["timestamp", "sstables_p50", "sstables_p75", "sstables_p95", "sstables_p98", "sstables_p99", "sstables_min", "sstables_max", "elapsed"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()
    return df

def parse_file_values_logger_file(filename: str) -> pd.DataFrame:
    """
    Parse file-values CSVs produced by values_logger.sh:
      columns: timestamp, <full_file_path_1>, <full_file_path_2>, ...
    Uses _read_filtered_lines() to drop common noise.
    Returns: timestamp (datetime), elapsed (s), numeric columns per file path.
    """
    lines = _read_filtered_lines(filename)
    if not lines:
        return pd.DataFrame(columns=["timestamp", "elapsed"])  # empty

    df = pd.read_csv(StringIO("\n".join(lines)))
    if "timestamp" not in df.columns:
        raise ValueError(f"{filename}: missing 'timestamp' column")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise")
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()

    # Convert every non-time column to numeric (quoted strings -> numbers)
    for col in df.columns:
        if col not in ("timestamp", "elapsed"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def parse_kswapd_wake_trace_file(filename: str) -> pd.DataFrame:
    """
    STRICT parser for bcc/tools/trace output run with:
      trace -T 't:vmscan:mm_vmscan_kswapd_wake "nid=%d,order=%d", args->nid, args->order'

    Requirements:
      - The first whitespace token must be a float seconds value from -T
      - Time-of-day tokens (HH:MM:SS[.ms]) from -t are NOT allowed (error)
      - No rebasing/subtraction; elapsed is kept AS-IS from the log

    Returns DataFrame columns: elapsed (float), nid (int), order (int)
    """
    lines = _read_filtered_lines(filename)
    if not lines:
        return pd.DataFrame(columns=["elapsed", "nid", "order"])

    time_of_day_re = re.compile(r"^\d{2}:\d{2}:\d{2}(?:\.\d+)?$")
    payload_re = re.compile(r"\bnid=(\d+),order=(\d+)\b")
    func_re = re.compile(r"\bmm_vmscan_kswapd_wake\b")

    elapsed_vals, nids, orders = [], [], []

    for raw in lines:
        s = raw.strip()
        if not s or s.startswith("TIME") or s.startswith("#"):
            continue
        if not func_re.search(s):
            continue

        m = payload_re.search(s)
        if not m:
            continue

        # First whitespace token is the time field
        tok = s.split(None, 1)[0]

        # Hard-fail if this looks like -t (time-of-day)
        if ":" in tok or time_of_day_re.match(tok):
            raise ValueError(
                f"{filename}: detected time-of-day token '{tok}'. "
                "This parser only accepts -T (float seconds). Re-run trace with -T."
            )

        # Must be float seconds (from -T). Keep AS-IS (no baseline subtraction).
        try:
            elapsed = float(tok)
        except ValueError as e:
            raise ValueError(
                f"{filename}: invalid time token '{tok}'. "
                "Expected -T float seconds."
            ) from e

        nid = int(m.group(1))
        order = int(m.group(2))

        elapsed_vals.append(elapsed)
        nids.append(nid)
        orders.append(order)

    if not elapsed_vals:
        return pd.DataFrame(columns=["elapsed", "nid", "order"])

    df = pd.DataFrame({"elapsed": elapsed_vals, "nid": nids, "order": orders})
    df = df.sort_values("elapsed").reset_index(drop=True)
    return df

# -----------------------------------------------------------------------------
# Summary + tables
# -----------------------------------------------------------------------------
def compute_summary_stats(df: pd.DataFrame, metric: str, include_total: bool = True):
    """
    Compute total/avg and percentiles (25/50/75/99) for a numeric column.
    Returns a tuple of 6 values (strings 'N/A' if the series is empty).
    """
    series = pd.to_numeric(df[metric], errors="coerce").dropna() if metric in df.columns else pd.Series(dtype=float)
    if series.empty:
        return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
    total = series.sum() if include_total else "N/A"
    avg = series.mean()
    p25, p50, p75, p99 = np.percentile(series, [25, 50, 75, 99])
    return (total, avg, p25, p50, p75, p99)


def create_summary_tables(data: dict[str, pd.DataFrame], source: str) -> str:
    """
    Build preformatted summary tables for all numeric columns in 'data'.
    Returns a single multi-line string to render on the cover page.
    """
    if not data:
        return ""
    # pick a sample df to discover columns (skip empties)
    sample_df = None
    for df in data.values():
        if isinstance(df, pd.DataFrame) and not df.empty:
            sample_df = df
            break
    if sample_df is None:
        return ""

    combined = []
    numeric_metrics = [
        m for m in sample_df.columns
        if m not in ["timestamp", "elapsed", "TIME"] and pd.api.types.is_numeric_dtype(sample_df[m])
    ]
    for m in numeric_metrics:
        headers = ["File", "Total", "Avg", "P25", "P50", "P75", "P99"]
        rows = []
        for fname, df in data.items():
            if m not in df.columns:
                continue
            total, avg, p25, p50, p75, p99 = compute_summary_stats(df, m)
            def fmt(x): return f"{x:.2f}" if isinstance(x, (int, float, np.floating)) else x
            wrapped_fname = os.path.basename(fname)
            rows.append([wrapped_fname, fmt(total), fmt(avg), fmt(p25), fmt(p50), fmt(p75), fmt(p99)])
        if rows:
            table_str = tabulate(rows, headers=headers, tablefmt="grid")
            combined.append(f"{source} - {m} Stats:\n{table_str}")
    return ("\n\n".join(combined)).strip()


def subtract_initial(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Subtract the initial value from specified columns (in-place safe on a copy)."""
    for col in columns:
        if col in df.columns and not df.empty:
            df[col] = df[col] - df[col].iloc[0]
    return df


def subtract_all_data_sources(data_dict: dict[str, pd.DataFrame], source: str) -> dict[str, pd.DataFrame]:
    """Apply subtract_initial() to all dfs in a dict for the configured columns."""
    subtract_cols = SUBTRACT_FROM_START.get(source, [])
    for fname in list(data_dict.keys()):
        data_dict[fname] = subtract_initial(data_dict[fname], subtract_cols)
    return data_dict


def _format_levels_trim_zeros(levels_str: str) -> str:
    """
    Take a string like "[8/4, 45/10, 104/100, 918, 0, 0, 0]"
    -> "8/4, 45/10, 104/100, 918"  (trim trailing zeros; keep X/Y as-is)
    """
    if not isinstance(levels_str, str) or not levels_str.strip():
        return ""
    s = levels_str.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    parts = [p.strip() for p in s.split(",")]

    def _is_zero(tok: str) -> bool:
        if "/" in tok:
            return False
        try:
            return float(tok) == 0.0
        except ValueError:
            return False

    end = len(parts)
    while end > 0 and _is_zero(parts[end - 1]):
        end -= 1
    return ", ".join(p.replace(" ", "") for p in parts[:end])


def parse_vmstat_command_file(filename: str) -> pd.DataFrame:
    """
    Parse vmstat pass-through output (wide, timestamp, one header, no-first).
    Keep only: timestamp, elapsed, r, b, us, sy, id, wa, st.
    If vmstat doesn't emit 'st' (bare metal), synthesize st=0 for a consistent schema.
    Validate per row that us+sy+id+wa+st sums to ~100; raise on first violation.
    """
    lines = []
    with open(filename, "r") as f:
        for ln in f:
            s = ln.rstrip("\n")
            if s:
                lines.append(s)

    if not lines:
        return pd.DataFrame(columns=["timestamp", "elapsed", "r", "b", "us", "sy", "id", "wa", "st"])

    # Find the vmstat column header row (the one starting with "r  b ...")
    hdr2_idx = None
    for i, s in enumerate(lines):
        st = s.lstrip()
        if st.startswith("r ") and " swpd " in f" {st} " and " id " in f" {st} ":
            hdr2_idx = i
            break
    if hdr2_idx is None:
        for i in range(len(lines) - 1):
            if lines[i].lower().startswith("procs ") and lines[i+1].lstrip().startswith("r "):
                hdr2_idx = i + 1
                break
    if hdr2_idx is None:
        raise ValueError(f"{filename}: could not find vmstat column header")

    header_row = re.findall(r"\S+", lines[hdr2_idx])
    if not header_row:
        raise ValueError(f"{filename}: empty vmstat header")

    # Replace the last header token (TZ label under '-----timestamp-----') with 'timestamp'
    col_names = header_row[:-1] + ["timestamp"]
    n_numeric = len(col_names) - 1

    records = []
    for s in lines[hdr2_idx + 1:]:
        st = s.lstrip()
        if st.startswith("procs ") or st.startswith("r "):  # skip any repeated headers
            continue
        toks = re.findall(r"\S+", st)
        if len(toks) < n_numeric + 1:
            continue  # incomplete/truncated line

        data_tokens = toks[:n_numeric]
        ts_str = " ".join(toks[n_numeric:])  # supports "YYYY-MM-DD HH:MM:SS" etc.

        rec = {}
        for i, name in enumerate(col_names[:-1]):
            rec[name] = pd.to_numeric(data_tokens[i], errors="coerce")
        rec["timestamp"] = ts_str
        records.append(rec)

    if not records:
        return pd.DataFrame(columns=["timestamp", "elapsed", "r", "b", "us", "sy", "id", "wa", "st"])

    df = pd.DataFrame.from_records(records)

    # Parse timestamp and compute elapsed
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=False)
    df = df.dropna(subset=["timestamp"]).reset_index(drop=True)
    df["elapsed"] = (df["timestamp"] - df["timestamp"].iloc[0]).dt.total_seconds()

    # Ensure 'st' exists; if missing, synthesize zeros
    if "st" not in df.columns:
        df["st"] = 0

    # Keep only requested columns (and in this order)
    keep_cols = ["timestamp", "elapsed", "r", "b", "us", "sy", "id", "wa", "st"]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    # Validate CPU % sum per row (allow ±1 due to integer rounding). 
    # This also ensures any extra cpu cols appearing in newer vmstat versions are not silently ignored.
    for idx, row in df.iterrows():
        for c in ["us", "sy", "id", "wa", "st"]:
            if pd.isna(row[c]):
                raise ValueError(f"{filename}: NaN in CPU column '{c}' at row {idx}, ts={row.get('timestamp')}")
        total = int(row["us"]) + int(row["sy"]) + int(row["id"]) + int(row["wa"]) + int(row["st"])
        if total < 99 or total > 101:
            raise ValueError(
                f"{filename}: CPU% sum {total} outside [99,101] at row {idx}, "
                f"ts={row.get('timestamp')} (us+sy+id+wa+st)"
            )
    # print(df)
    # raise ValueError("test")
    return df


# -----------------------------------------------------------------------------
# Rendering big preformatted pages (NO wrapping by Matplotlib)
# -----------------------------------------------------------------------------

def add_preformatted_block_page(
    pdf,
    text: str,
    *,
    font_size: float = 8.0,
    left: float = 0.03,
    right: float = 0.03,
    top: float = 0.98,
    bottom: float = 0.02,
    base_width_in: float = 11.0,
    min_height_in: float = 6.0,
    font_family: str = "monospace",
) -> None:
    """
    Render a preformatted multi-line string *as-is* (NO Matplotlib wrapping),
    auto-size the page (width & height), and clamp DPI before any draw so the
    Agg renderer never exceeds its 2^16 pixel limit.
    """
    if not text:
        fig = plt.figure(figsize=(base_width_in, min_height_in))
        plt.axis("off")
        pdf.savefig(fig)
        plt.close(fig)
        return

    # --- helpers ---
    def _safe_dpi_for_size(w_in: float, h_in: float, base_dpi: float) -> float:
        MAXPX = (1 << 16) - 1024  # safety margin under 65536
        max_dpi_w = MAXPX / max(w_in, 1e-6)
        max_dpi_h = MAXPX / max(h_in, 1e-6)
        dpi = min(base_dpi, max_dpi_w, max_dpi_h) * 0.98
        return max(1.0, dpi)

    # Rough first guess (monospace metrics) to avoid undersized first draw
    lines = text.splitlines() or [""]
    max_chars = max(len(s) for s in lines)
    char_w_in = (font_size / 72.0) * 0.60   # ~width per monospace char
    line_h_in = (font_size / 72.0) * 1.35   # line height
    rough_w_in = max_chars * char_w_in
    rough_h_in = len(lines) * line_h_in

    init_w_in = max(base_width_in, rough_w_in / max(1e-6, (1 - left - right)) + 0.5)
    init_h_in = max(min_height_in, rough_h_in / max(1e-6, (top - bottom)) + 0.5)

    # Create figure with a DPI that is safe for this initial size (prevents first-draw crash)
    base_dpi = mpl.rcParams.get("figure.dpi", 100)
    safe_dpi = _safe_dpi_for_size(init_w_in, init_h_in, base_dpi)
    fig = plt.figure(figsize=(init_w_in, init_h_in), dpi=safe_dpi)
    plt.axis("off")
    txt = plt.figtext(
        left, top, text,
        fontsize=font_size, fontfamily=font_family,
        va="top", ha="left", wrap=False
    )
    txt.set_clip_on(False)

    # First draw to measure exact bbox (already safe thanks to clamped DPI)
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    bb = txt.get_window_extent(renderer=renderer)
    text_w_in = bb.width  / fig.dpi
    text_h_in = bb.height / fig.dpi

    usable_w_frac = max(1e-6, (1 - left - right))
    usable_h_frac = max(1e-6, (top - bottom))

    need_w_in = max(init_w_in, text_w_in / usable_w_frac + 0.25)
    need_h_in = max(init_h_in, text_h_in / usable_h_frac + 0.25)

    # If we need to resize, also recompute a safe DPI for the new size *before* drawing again
    if abs(need_w_in - init_w_in) > 1e-3 or abs(need_h_in - init_h_in) > 1e-3:
        fig.set_size_inches(need_w_in, need_h_in, forward=True)
        new_safe_dpi = _safe_dpi_for_size(need_w_in, need_h_in, fig.dpi)
        if abs(new_safe_dpi - fig.dpi) > 1e-6:
            fig.set_dpi(new_safe_dpi)
        fig.canvas.draw()

    # Final save (vector text; DPI only affects internal Agg layout/rasterized artists)
    pdf.savefig(fig)
    plt.close(fig)

# -----------------------------------------------------------------------------
# Tables / Plots
# -----------------------------------------------------------------------------
def add_compactionstats_table(
    pdf: PdfPages,
    compactionstats_data: dict[str, pd.DataFrame],
    title: str = "Compactionstats: Pending (Active)",
    header_wrap: int = 8,
    use_basename: bool = True,
) -> None:
    """
    Build a single huge, preformatted table:
      - columns = experiments (dict keys)
      - first column = elapsed (s, rounded)
      - cells = "Pending (Active)"
    """
    if not compactionstats_data:
        return

    # Build unified elapsed index (integer seconds)
    all_elapsed = set()
    per_exp_maps = {}
    for key, df in compactionstats_data.items():
        if df.empty:
            continue
        dfi = df.copy()
        dfi["elapsed_int"] = dfi["elapsed"].round().astype(int)
        dfi = dfi.sort_values("timestamp").drop_duplicates("elapsed_int", keep="last")
        per_exp_maps[key] = {
            int(row.elapsed_int): f"{int(row.pending_tasks)} ({int(row.active_compactions)})"
            for _, row in dfi.iterrows()
        }
        all_elapsed.update(per_exp_maps[key].keys())

    if not all_elapsed:
        return

    elapsed_sorted = sorted(all_elapsed)
    exp_keys = list(compactionstats_data.keys())
    if use_basename:
        col_labels = [textwrap.fill(os.path.basename(k), header_wrap) for k in exp_keys]
    else:
        col_labels = [textwrap.fill(k, header_wrap) for k in exp_keys]
    headers = ["elapsed (s)"] + col_labels

    rows = []
    for t in elapsed_sorted:
        row = [t]
        for key in exp_keys:
            row.append(per_exp_maps.get(key, {}).get(t, ""))
        rows.append(row)

    table_str = tabulate(rows, headers=headers, tablefmt="grid", stralign="center", numalign="center")

    # Render as a preformatted, non-wrapped page
    title_block = f"{title}\n\n" if title else ""
    add_preformatted_block_page(pdf, title_block + table_str, font_size=8.0)


def add_tablestats_levels_table(
    pdf: PdfPages,
    tablestats_data: dict[str, pd.DataFrame],
    title: str = "SSTables in each level (trim trailing zeros)",
    header_wrap: int = 12,
    use_basename: bool = True,
) -> None:
    """
    Build a single huge, preformatted table from tablestats data (no wrapping by Matplotlib).
    Cells display the levels string with trailing zeros trimmed.
    """
    if not tablestats_data:
        return

    all_elapsed = set()
    per_exp_maps = {}
    for key, df in tablestats_data.items():
        if df.empty:
            continue
        dfi = df.copy()
        dfi["elapsed_int"] = dfi["elapsed"].round().astype(int)
        dfi = dfi.sort_values("timestamp").drop_duplicates("elapsed_int", keep="last")
        per_exp_maps[key] = {
            int(row.elapsed_int): _format_levels_trim_zeros(
                row.sstables_in_each_level if isinstance(row.sstables_in_each_level, str) else ""
            )
            for _, row in dfi.iterrows()
        }
        all_elapsed.update(per_exp_maps[key].keys())

    if not all_elapsed:
        return

    elapsed_sorted = sorted(all_elapsed)

    exp_keys = list(tablestats_data.keys())
    if use_basename:
        col_labels = [textwrap.fill(os.path.basename(k), header_wrap) for k in exp_keys]
    else:
        col_labels = [textwrap.fill(k, header_wrap) for k in exp_keys]
    headers = ["elapsed (s)"] + col_labels

    rows = []
    for t in elapsed_sorted:
        row = [t]
        for key in exp_keys:
            row.append(per_exp_maps.get(key, {}).get(t, ""))
        rows.append(row)

    table_str = tabulate(rows, headers=headers, tablefmt="grid", stralign="left", numalign="center")
    title_block = f"{title}\n\n" if title else ""
    add_preformatted_block_page(pdf, title_block + table_str, font_size=8.0)


def plot_all_metrics(
    pdf: PdfPages,
    data: dict[str, pd.DataFrame],
    source: str,
    base_figsize=(11, 9),
    legend_fontsize=6,
    max_legend_rows=1000,
    top_pad_in=0.5,
    bottom_pad_in=0.35,
    target_axes_height_in=7.2,
    # filesize controls:
    enable_simplify=True,
    simplify_threshold=0.5,        # higher -> fewer vertices
    max_points_per_series=None,    # e.g., 2000 to decimate long series
    rasterize_points_threshold=50_000,  # total points across lines to trigger raster
    rasterized_dpi=120,
) -> None:
    """
    Plot all numeric metrics (excluding timestamp/elapsed/TIME) for each dataset.
    - Auto-grows figure height to fit big legends (bottom placement)
    - Simplifies and optionally decimates line vertices
    - Rasterizes line artists when total points exceed a threshold
    """
    if not data:
        return

    # Choose a sample df for columns (skip empties)
    sample_df = None
    for df in data.values():
        if isinstance(df, pd.DataFrame) and not df.empty:
            sample_df = df
            break
    if sample_df is None:
        return

    metrics = [m for m in sample_df.columns if m not in ["timestamp", "elapsed", "TIME"]]

    # Set/restore global simplify rcparams around plotting
    old_simplify = mpl.rcParams.get("path.simplify", True)
    old_thresh = mpl.rcParams.get("path.simplify_threshold", 0.1)
    if enable_simplify:
        mpl.rcParams["path.simplify"] = True
        mpl.rcParams["path.simplify_threshold"] = simplify_threshold

    for m in metrics:
        fig, ax = plt.subplots(figsize=base_figsize)

        labels = []
        lines = []
        total_points = 0

        for fname, df in data.items():
            if m not in df.columns or df.empty:
                continue
            x = df["elapsed"].to_numpy()
            y = pd.to_numeric(df[m], errors="coerce").to_numpy()
            mask = np.isfinite(x) & np.isfinite(y)
            x, y = x[mask], y[mask]
            x, y = _decimate_xy(x, y, max_points_per_series)
            ln, = ax.plot(x, y, label=os.path.basename(fname), linewidth=0.75)
            lines.append(ln)
            labels.append(os.path.basename(fname))
            total_points += len(x)

        if not labels:
            plt.close(fig)
            continue

        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel(f"{source} {m}")
        ax.set_title(f"{source} {m} vs Elapsed Time")

        # Legend layout & measurement
        N = len(labels)
        ncol = max(1, math.ceil(N / max_legend_rows))
        leg = fig.legend(
            loc="lower center", ncol=ncol, fontsize=legend_fontsize,
            frameon=True, borderaxespad=0.2, columnspacing=0.8,
            handlelength=1.2, handletextpad=0.4, bbox_to_anchor=(0.5, 0.01)
        )

        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        legend_h_in = leg.get_window_extent(renderer=renderer).height / fig.dpi

        # Grow figure if needed to preserve target axes height
        base_w_in, base_h_in = fig.get_size_inches()
        new_h_in = max(base_h_in, target_axes_height_in + top_pad_in + bottom_pad_in + legend_h_in)
        if new_h_in > base_h_in:
            fig.set_size_inches(base_w_in, new_h_in, forward=True)

        bottom_frac = (legend_h_in + bottom_pad_in) / new_h_in
        top_frac = 1.0 - (top_pad_in / new_h_in)

        # Rasterize heavy pages
        save_kwargs = {}
        if total_points >= rasterize_points_threshold:
            for ln in lines:
                ln.set_rasterized(True)
            save_kwargs["dpi"] = rasterized_dpi

        _safe_tight_layout(fig, rect=[0.0, bottom_frac, 1.0, top_frac])
        pdf.savefig(fig, **save_kwargs)
        plt.close(fig)

    # restore rcParams
    mpl.rcParams["path.simplify"] = old_simplify
    mpl.rcParams["path.simplify_threshold"] = old_thresh

def add_command_vmstat_stacked_cpu(
    pdf: PdfPages,
    command_vmstat_data: dict[str, pd.DataFrame],
    *,
    base_figsize=(11, 9),
    legend_fontsize=8,
    title_fontsize=8,             # smaller title text
    title_wrap=72,                # wrap long experiment names
    max_bars=1000,
    rasterize_bars_threshold=15000
) -> None:
    """
    For each experiment (file) in command_vmstat_data, add one page with a
    100% stacked column chart of CPU breakdown over time.
    - x-axis: elapsed seconds
    - stacks: us, sy, id, wa, st (fixed colors via CPU_COLOR_MAP)
    - normalization: each column sums to 100%
    - title: smaller font, wrapped experiment name to avoid overflow
    """
    if not command_vmstat_data:
        return

    stack_order = ["us", "sy", "id", "wa", "st"]
    colors = [CPU_COLOR_MAP[k] for k in stack_order]

    for fname, df in command_vmstat_data.items():
        if df is None or df.empty:
            continue

        missing = [c for c in ["elapsed", *stack_order] if c not in df.columns]
        if missing:
            continue

        dfi = df.sort_values("elapsed").reset_index(drop=True).copy()
        for c in stack_order:
            dfi[c] = pd.to_numeric(dfi[c], errors="coerce")
        dfi = dfi.dropna(subset=stack_order + ["elapsed"])
        if dfi.empty:
            continue

        total = dfi[stack_order].sum(axis=1).replace(0, np.nan)
        pct = dfi[stack_order].div(total, axis=0).fillna(0.0) * 100.0
        x = dfi["elapsed"].to_numpy()

        n = len(x)
        if n == 0:
            continue
        if n > max_bars:
            step = int(math.ceil(n / max_bars))
            sel = np.arange(0, n, step)
            x = x[sel]
            pct = pct.iloc[sel].reset_index(drop=True)

        if len(x) > 1:
            dx = np.diff(x)
            width = (np.median(dx) if np.all(np.isfinite(dx)) else 1.0) * 0.9
        else:
            width = 1.0

        fig, ax = plt.subplots(figsize=base_figsize)

        bottoms = np.zeros(len(x), dtype=float)
        bars = []
        for i, col in enumerate(stack_order):
            height = pct[col].to_numpy()
            b = ax.bar(x, height, width=width, bottom=bottoms, label=col, color=colors[i], linewidth=0)
            bars.append(b)
            bottoms += height

        ax.set_ylim(0, 100)
        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel("CPU time (%)")

        # Smaller, wrapped title to prevent overflow
        title_main = "command_vmstat CPU breakdown (100% stacked)"
        exp_name = os.path.basename(fname)
        exp_wrapped = textwrap.fill(exp_name, width=title_wrap)
        ax.set_title(f"{title_main}\n{exp_wrapped}", fontsize=title_fontsize)

        ax.legend(loc="upper right", fontsize=legend_fontsize, frameon=True)

        total_rects = sum(len(bc) for bc in bars)
        save_kwargs = {}
        if total_rects >= rasterize_bars_threshold:
            for bc in bars:
                for rect in bc:
                    rect.set_rasterized(True)
            save_kwargs["dpi"] = 120

        _safe_tight_layout(fig, rect=[0.03, 0.06, 0.97, 0.97])
        pdf.savefig(fig, **save_kwargs)
        plt.close(fig)

def plot_file_values_logger(
    pdf: PdfPages,
    file_values_data: dict[str, pd.DataFrame],
    *,
    base_figsize=(11, 9),
    legend_fontsize=6,
    title_wrap=72,
    enable_simplify=True,
    simplify_threshold=0.5,
    max_points_per_series=None,
    rasterize_points_threshold=50_000,
    rasterized_dpi=120,
) -> None:
    """
    One plot per *file path column* across all experiments.
    Lines = experiments (file names), X = elapsed (s), Y = value.
    """
    if not file_values_data:
        return

    # Find union of metric columns across all experiments
    metric_cols = set()
    for df in file_values_data.values():
        if isinstance(df, pd.DataFrame) and not df.empty:
            metric_cols.update([c for c in df.columns if c not in ("timestamp", "elapsed")])
    metric_cols = sorted(metric_cols)
    if not metric_cols:
        return

    # Set/restore simplify rcparams
    old_simplify = mpl.rcParams.get("path.simplify", True)
    old_thresh = mpl.rcParams.get("path.simplify_threshold", 0.1)
    if enable_simplify:
        mpl.rcParams["path.simplify"] = True
        mpl.rcParams["path.simplify_threshold"] = simplify_threshold

    for m in metric_cols:
        fig, ax = plt.subplots(figsize=base_figsize)
        labels = []
        lines = []
        total_points = 0

        for fname, df in file_values_data.items():
            if df is None or df.empty or m not in df.columns:
                continue
            x = df["elapsed"].to_numpy()
            y = pd.to_numeric(df[m], errors="coerce").to_numpy()
            mask = np.isfinite(x) & np.isfinite(y)
            x, y = x[mask], y[mask]
            if len(x) == 0:
                continue
            if max_points_per_series is not None and len(x) > max_points_per_series:
                idx = np.linspace(0, len(x) - 1, max_points_per_series).astype(int)
                x, y = x[idx], y[idx]
            ln, = ax.plot(x, y, linewidth=0.9, label=os.path.basename(fname))
            lines.append(ln)
            labels.append(os.path.basename(fname))
            total_points += len(x)

        if not labels:
            plt.close(fig)
            continue

        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel(m)
        ax.set_title(textwrap.fill(f"file_values: {m}", width=title_wrap))

        # --- legend below the axes, measure, and grow figure height if needed ---
        max_legend_rows = 1000
        ncol = max(1, math.ceil(len(labels) / max_legend_rows))
        leg = fig.legend(
            loc="lower center",
            ncol=ncol,
            fontsize=legend_fontsize,
            frameon=True,
            borderaxespad=0.2,
            columnspacing=0.8,
            handlelength=1.2,
            handletextpad=0.4,
            bbox_to_anchor=(0.5, 0.01),
        )

        # Measure legend height and adjust layout so axes keep a usable height
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        legend_h_in = leg.get_window_extent(renderer=renderer).height / fig.dpi

        # Keep axes area similar to other pages
        top_pad_in = 0.5
        bottom_pad_in = 0.35
        target_axes_height_in = 7.2

        base_w_in, base_h_in = fig.get_size_inches()
        new_h_in = max(base_h_in, target_axes_height_in + top_pad_in + bottom_pad_in + legend_h_in)
        if new_h_in > base_h_in:
            fig.set_size_inches(base_w_in, new_h_in, forward=True)

        bottom_frac = (legend_h_in + bottom_pad_in) / new_h_in
        top_frac = 1.0 - (top_pad_in / new_h_in)

        save_kwargs = {}
        if total_points >= rasterize_points_threshold:
            for ln in lines:
                ln.set_rasterized(True)
            save_kwargs["dpi"] = rasterized_dpi

        _safe_tight_layout(fig, rect=[0.0, bottom_frac, 1.0, top_frac])
        pdf.savefig(fig, **save_kwargs)
        plt.close(fig)

    # restore rcParams
    mpl.rcParams["path.simplify"] = old_simplify
    mpl.rcParams["path.simplify_threshold"] = old_thresh

def plot_kswapd_wake(
    pdf: PdfPages,
    kswapd_wake_data: dict[str, pd.DataFrame],
    *,
    base_figsize=(11, 9),
    legend_fontsize=7,
    title_wrap=72,
    rasterize_points_threshold=40_000,
    rasterized_dpi=120,
) -> None:
    """
    One plot per *workload file*. Lines = NIDs, Y = order, X = elapsed (s).
    """
    if not kswapd_wake_data:
        return

    for fname, df in kswapd_wake_data.items():
        if df is None or df.empty:
            continue
        req = ["elapsed", "nid", "order"]
        if any(c not in df.columns for c in req):
            continue

        dfi = df.copy()
        dfi["elapsed"] = pd.to_numeric(dfi["elapsed"], errors="coerce")
        dfi["nid"] = pd.to_numeric(dfi["nid"], downcast="integer", errors="coerce")
        dfi["order"] = pd.to_numeric(dfi["order"], downcast="integer", errors="coerce")
        dfi = dfi.dropna(subset=["elapsed", "nid", "order"]).sort_values("elapsed")
        if dfi.empty:
            continue

        fig, ax = plt.subplots(figsize=base_figsize)

        total_points = 0
        collections = []
        for nid, g in dfi.groupby("nid", sort=True):
            x = g["elapsed"].to_numpy()
            y = g["order"].to_numpy()
            total_points += len(x)
            sc = ax.scatter(
                x, y,
                s=9,            # point size
                alpha=0.75,     # slight transparency for dense regions
                linewidths=0,   # faster; no marker edge
                label=f"nid={int(nid)}"
            )
            collections.append(sc)

        ax.set_xlim(left=0)
        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel("kswapd wake order")
        title = f"kswapd wake (mm_vmscan_kswapd_wake)\n{textwrap.fill(os.path.basename(fname), width=title_wrap)}"
        ax.set_title(title)
        ax.legend(loc="best", fontsize=legend_fontsize, frameon=True)

        # integer y-ticks if small range
        ymin, ymax = int(dfi["order"].min()), int(dfi["order"].max())
        if ymax - ymin <= 16:
            ax.set_yticks(range(ymin, ymax + 1))

        save_kwargs = {}
        if total_points >= rasterize_points_threshold:
            for sc in collections:
                sc.set_rasterized(True)
            save_kwargs["dpi"] = rasterized_dpi

        _safe_tight_layout(fig)
        pdf.savefig(fig, **save_kwargs)
        plt.close(fig)

# -----------------------------------------------------------------------------
# Cover page
# -----------------------------------------------------------------------------
def add_summary_cover_page(
    pdf: PdfPages,
    summary_text: str,
    font_size: float = 8.0,
) -> None:
    """
    Cover page: preformatted (already-wrapped) text, no Matplotlib wrapping.
    Uses the generic preformatted page renderer.
    """
    add_preformatted_block_page(pdf, summary_text, font_size=font_size)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Plot cachestat, meminfo, vmstat, and nodetool logs to a PDF")
    parser.add_argument("--output-dir", required=True, help="Output directory for the PDF")
    parser.add_argument("--cachestat-files", nargs="+", required=False, default=[], help="cachestat output files")
    parser.add_argument("--meminfo-files", nargs="+", required=False, default=[], help="meminfo CSV log files")
    parser.add_argument("--proc-vmstat-files", nargs="+", required=False, default=[], help="proc vmstat CSV log files")
    parser.add_argument("--command-vmstat-files", nargs="+", required=False, default=[], help="command vmstat CSV log files")
    parser.add_argument("--file-values-files", nargs="+", required=False, default=[], help="CSV logs from values_logger.sh (timestamp + file-path columns)")
    parser.add_argument("--kswapd-wake-files", nargs="+", required=False, default=[], help="bcc trace logs for mm_vmscan_kswapd_wake (TIME ... nid=NN,order=OO)")
    parser.add_argument("--nodetool-compactionstats-files", nargs="+", required=False, default=[], help="nodetool compactionstats log files")
    parser.add_argument("--nodetool-tablehistograms-files", nargs="+", required=False, default=[], help="nodetool tablehistograms log files")
    parser.add_argument("--nodetool-tablestats-files", nargs="+", required=False, default=[], help="nodetool tablestats log files")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_pdf = os.path.join(args.output_dir, f"metrics_plot_{timestamp}.pdf")

    # Parse inputs
    cache_data = {fname: parse_cachestat_file(fname) for fname in tqdm(args.cachestat_files, desc="Parsing cachestat")}
    meminfo_data = {fname: parse_timestamped_file(fname) for fname in tqdm(args.meminfo_files, desc="Parsing meminfo")}
    proc_vmstat_data = {fname: parse_timestamped_file(fname) for fname in tqdm(args.proc_vmstat_files, desc="Parsing proc vmstat")}
    command_vmstat_data = {fname: parse_vmstat_command_file(fname) for fname in tqdm(args.command_vmstat_files, desc="Parsing command vmstat")}
    compactionstats_data = {fname: parse_compactionstats_file(fname) for fname in tqdm(args.nodetool_compactionstats_files, desc="Parsing compactionstats")}
    file_values_data = {fname: parse_file_values_logger_file(fname) for fname in tqdm(args.file_values_files, desc="Parsing file-values")}
    kswapd_wake_data = {fname: parse_kswapd_wake_trace_file(fname) for fname in tqdm(args.kswapd_wake_files, desc="Parsing kswapd-wake")}
    tablestats_data = {fname: parse_tablestats_file(fname) for fname in tqdm(args.nodetool_tablestats_files, desc="Parsing tablestats")}
    tablehistograms_data = {fname: parse_tablehistograms_file(fname) for fname in tqdm(args.nodetool_tablehistograms_files, desc="Parsing tablehistograms")}

    # Global transforms
    meminfo_data = subtract_all_data_sources(meminfo_data, "meminfo")
    proc_vmstat_data = subtract_all_data_sources(proc_vmstat_data, "proc_vmstat")
    file_values_data = subtract_all_data_sources(file_values_data, "file_values")

    # Build summary cover text (preformatted)
    summary_blocks = []
    sum_cache = create_summary_tables(cache_data, "cachestat")
    if sum_cache:
        summary_blocks.append(sum_cache)
    sum_meminfo = create_summary_tables(meminfo_data, "meminfo")
    if sum_meminfo:
        summary_blocks.append(sum_meminfo)
    sum_vmstat = create_summary_tables(proc_vmstat_data, "proc_vmstat")
    if sum_vmstat:
        summary_blocks.append(sum_vmstat)
    sum_command_vmstat = create_summary_tables(command_vmstat_data, "command_vmstat")
    if sum_command_vmstat:
        summary_blocks.append(sum_command_vmstat)
    sum_file_values = create_summary_tables(file_values_data, "file_values")
    if sum_file_values:
        summary_blocks.append(sum_file_values)
    sum_compact = create_summary_tables(compactionstats_data, "compactionstats")
    if sum_compact:
        summary_blocks.append(sum_compact)
    sum_hist = create_summary_tables(tablehistograms_data, "tablehistograms")
    if sum_hist:
        summary_blocks.append(sum_hist)

    cover_text = "System Metrics Summary\n\n" + ("\n\n".join(summary_blocks))

    # Emit PDF
    with PdfPages(output_pdf) as pdf:
        add_summary_cover_page(pdf, cover_text, font_size=8.0)

        # Primary plots
        plot_all_metrics(pdf, cache_data, "cachestat")
        plot_all_metrics(pdf, meminfo_data, "meminfo")
        plot_all_metrics(pdf, proc_vmstat_data, "proc_vmstat")
        plot_all_metrics(pdf, command_vmstat_data, "command_vmstat")
        add_command_vmstat_stacked_cpu(pdf, command_vmstat_data)
        plot_file_values_logger(pdf, file_values_data)
        plot_kswapd_wake(pdf, kswapd_wake_data)
        plot_all_metrics(pdf, compactionstats_data, "compactionstats")
        plot_all_metrics(pdf, tablehistograms_data, "tablehistograms")

        # Big preformatted pages
        add_compactionstats_table(pdf, compactionstats_data)
        add_tablestats_levels_table(pdf, tablestats_data)

    print(f"PDF saved to {output_pdf}")


if __name__ == "__main__":
    main()
