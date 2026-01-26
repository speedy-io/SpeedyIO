#!/usr/bin/env python3
"""
ycsb_aggregate.py
~~~~~~~~~~~~~~~~~
Aggregate final *and* time–series latency/throughput from YCSB HDR logs
(no reliance on the .out text).

For each *.out file (only used to locate the matching .hdr logs):

  • discovers *.out.0.READ.hdr*, *.UPDATE.hdr* … (skips keys in KEYS_TO_IGNORE)
  • calls MergeHdrStats once for OVERALL and once per key
  • derives throughput:  thr = totalCount / (runtime_us / 1e6)
  • stores:
        overall_final, overall_series,
        per_key_final[key], per_key_series[key]

CSV output prints only the final numbers.
"""

from __future__ import annotations
import argparse, csv, json, pathlib, subprocess, sys, tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import datetime
import textwrap
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from tabulate import tabulate
import matplotlib.colors as mcolors

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------

ALLOWED_KEYS = ["READ", "UPDATE", "INSERT", "SCAN"]
# READ-MODIFY-WRITE main ycsb displays read info, update info as well as combined info for the whole RMW
# Reads from RMW are counted in reads, and updates from RMW are counted in updates as well
# So, ignore READ-MODIFY-WRITE operation, its numbers are already included in other basic operations.
# While calculating throughput in .out files, ycsb considers RMW as 1 operation. So when we get our throughput
# numbers from .hdr files, they come out higher because we add operations from READ and UPDATE both, leading to 
# double counting. For GC-storm check, we are manually doing the same double-counting to the throughput obtained
# from the .out files so that the throughput obtained from both sources follow the same convention of ignoring RMW 
# and only consider the base operations.
KEYS_TO_IGNORE = ["READ-MODIFY-WRITE", "CLEANUP"]
KEYS_TO_ERROR = ["READ-FAILED", "UPDATE-FAILED", "INSERT-FAILED"]
CSV_COLS     = ["thr", "min", "p50", "p95", "p99", "p999", "p9999", "max"]

FAILED_EXPT_IDENTIFIER = "FAILED"

# -------------------------------------------------------------------------
# DATACLASSES
# -------------------------------------------------------------------------

COMPUTED_EXPT_PREFIXES = set()

@dataclass
class LatencyStats:
    thr:    Optional[int] = None
    min:    Optional[int]   = None
    p50:    Optional[int]   = None
    p95:    Optional[int]   = None
    p99:    Optional[int]   = None
    p999:   Optional[int]   = None
    p9999:  Optional[int]   = None
    max:    Optional[int]   = None

    def as_list(self) -> List[Optional[float]]:
        return [self.thr, self.min, self.p50, self.p95,
                self.p99, self.p999, self.p9999, self.max]

    def __str__(self):
        return ("thr={thr}, min={min}, p50={p50}, p95={p95}, "
                "p99={p99}, p999={p999}, p9999={p9999}, max={max}"
               ).format(**{k: (v if v is not None else float('nan'))
                            for k, v in self.__dict__.items()})

@dataclass
class IntervalStats:
    t0_epoch_us: int
    count: int
    thr: float
    min: int
    p50: int
    p95: int
    p99: int
    p999: int
    p9999: int
    max: int

    def __str__(self):
        return (f"t0={self.t0_epoch_us}, cnt={self.count}, thr={self.thr:.1f}, "
                f"min={self.min}, p50={self.p50}, p95={self.p95}, p99={self.p99}")

@dataclass
class OutFileStats:
    filename: str
    overall_stats: LatencyStats
    failed_expt: bool = False

    def __str__(self):
        lines = [f"OutFileStats for {self.filename}  [ONLY STORING THROUGHOUT FOR NOW]"]
        lines.append(f"        {self.overall_stats}")
        return "\n".join(lines)

@dataclass
class WorkloadResult:
    expt_prefix: str
    filenames: List[str]                             = field(default_factory=list)
    overall_final:  LatencyStats                     = field(default_factory=LatencyStats)
    overall_series: List[IntervalStats]              = field(default_factory=list)
    per_key_final:  Dict[str, LatencyStats]          = field(default_factory=dict)
    per_key_series: Dict[str, List[IntervalStats]]   = field(default_factory=dict)
    out_files_stats: List[OutFileStats]              = field(default_factory=list)

    def __str__(self):
        lines = [f"\nWorkloadResult for {self.expt_prefix}"]
        for fname in self.filenames:
            lines.append(f"        {fname}")
        lines.append(f"  OVERALL({self.overall_final})")
        for i in self.overall_series:
            lines.append(f"    {i}")
        for k in ALLOWED_KEYS:
            if k in self.per_key_final:
                lines.append(f"  {k}({self.per_key_final[k]})")
                for i in self.per_key_series[k]:
                    lines.append(f"    {i}")
        for ofs in self.out_files_stats:
            lines.append(f"  {ofs}")
        return "\n".join(lines)
    
    def is_gc_storm_detected(self):
        for ofs in self.out_files_stats:
            if ofs.failed_expt:  # Failed expt, no final data would have been displayed in the .out files
                return False

        threshold_percentage = 5.
        final_overall_thr_from_hdrs = self.overall_final.thr
        final_overall_thr_from_out_files = sum([ofs.overall_stats.thr for ofs in self.out_files_stats]) * 1.0

        deviation = (abs(final_overall_thr_from_hdrs - final_overall_thr_from_out_files) / final_overall_thr_from_out_files) * 100
        # print(f"GC storm deviation % = {deviation:.2f}", file=sys.stderr)
        gc_storm_detected = deviation > threshold_percentage
        if gc_storm_detected:
            print(
                f"GC storm detected, deviation % = {deviation:.2f} , final_overall_thr_from_hdrs={final_overall_thr_from_hdrs} , "
                f"final_overall_thr_from_out_files={final_overall_thr_from_out_files}", file=sys.stderr
            )

        return gc_storm_detected


# -------------------------------------------------------------------------
# JAVA WRAPPER
# -------------------------------------------------------------------------

def run_mergehdr(java_cp: str, hdr_files: List[str], ignore_start_seconds: int, ignore_end_seconds: int) -> dict:
    """Run MergeHdrStats and parse its JSON."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as fp:
        tmp_json = fp.name
    try:
        java_script_args = [
            "java", "-cp", java_cp, "MergeHdrStats",
            f"--ignore-head-sec={int(ignore_start_seconds)}",
            f"--ignore-tail-sec={int(ignore_end_seconds)}",
            tmp_json, *hdr_files
        ]
        subprocess.run(java_script_args,
                       check=True,
                       # Comment out these stdout redirections to see the actual java errors if this errors out
                       )#stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return json.load(open(tmp_json))
    finally:
        pathlib.Path(tmp_json).unlink(missing_ok=True)

# def run_mergehdr(java_cp: str,
#                  hdr_files: List[str],
#                  ignore_start_seconds: int,
#                  ignore_end_seconds: int) -> Dict[str, Any]:
#     """
#     Run MergeHdrStats and parse its JSON.

#     - ignore_start_seconds  -> --ignore-head-sec
#     - ignore_end_seconds    -> --ignore-tail-sec
#     """
#     if not hdr_files:
#         raise ValueError("hdr_files must be a non-empty list")

#     with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as fp:
#         tmp_json = fp.name

#     # Build CLI: flags FIRST, then output.json, then the .hdr files
#     args = [
#         "java", "-cp", java_cp, "MergeHdrStats",
#         f"--ignore-head-sec={int(ignore_start_seconds)}",
#         f"--ignore-tail-sec={int(ignore_end_seconds)}",
#         tmp_json, *hdr_files
#     ]

#     try:
#         # Let stdout/stderr flow so you can see validation and drift logs.
#         # If you prefer to capture and raise a neat error message, uncomment capture bits below.
#         proc = subprocess.run(
#             args,
#             check=False,           # we’ll raise with stderr on non-zero
#             text=True
#             # capture_output=True, # <- enable if you want to suppress console spam
#         )
#         if proc.returncode != 0:
#             # If you enabled capture_output above, switch proc.stderr/proc.stdout accordingly.
#             raise RuntimeError(f"MergeHdrStats failed (exit {proc.returncode}).")

#         with open(tmp_json, "r") as f:
#             return json.load(f)
#     finally:
#         pathlib.Path(tmp_json).unlink(missing_ok=True)

# -------------------------------------------------------------------------
# JSON → dataclasses
# -------------------------------------------------------------------------

def json_to_latency_final(js: dict) -> LatencyStats:
    o = js["overall"]
    return LatencyStats(min=o["min_us"], p50=o["p50_us"], p95=o["p95_us"],
                        p99=o["p99_us"], p999=o["p999_us"],
                        p9999=o["p9999_us"], max=o["max_us"])

def json_to_series(js: dict) -> List[IntervalStats]:
    out = []
    for b in js["intervals"]:
        # This is the actual time covered by this interval/bucket.
        # Generally it should be close to the configured interval_sec,
        # but may be shorter at start/end or if ignore_head/tail_sec is used.
        # This is because the start and end intervals might be partial intervals when 
        # ignore_head/tail_sec is used.
        covered_sec = b["covered_us"] / 1_000_000.0  
        thr = b["count"] / covered_sec
        out.append(IntervalStats(
            t0_epoch_us=b["t0_epoch_us"], count=b["count"], thr=thr,
            min=b["min_us"], p50=b["p50_us"], p95=b["p95_us"],
            p99=b["p99_us"], p999=b["p999_us"],
            p9999=b["p9999_us"], max=b["max_us"]))
    return out

# -------------------------------------------------------------------------
# PROCESS ONE *.out*  (only HDR logs are used)
# -------------------------------------------------------------------------

def process_out_file(
        out_path: pathlib.Path, java_cp: str, ignore_failures: bool,
        ignore_start_seconds: int, ignore_end_seconds: int
    ) -> Optional[WorkloadResult]:
    expt_prefix = out_path.name.split('__node_')[0] + '__node_'
    if expt_prefix in COMPUTED_EXPT_PREFIXES:  # Return if result is already computed for this prefix
        return None
    COMPUTED_EXPT_PREFIXES.add(expt_prefix)

    hdrs = sorted(out_path.parent.glob(expt_prefix + "*.hdr"))
    if not hdrs:
        raise FileNotFoundError(f"No HDR logs for expt_prefix: {expt_prefix}")

    # group hdr files by key
    by_key: Dict[str, List[str]] = {}
    for f in hdrs:
        key = f.suffixes[-2][1:]  # ".READ.hdr" → READ
        if key in KEYS_TO_IGNORE:
            continue
        if key in KEYS_TO_ERROR:
            if ignore_failures:
                continue
            else:
                raise ValueError(f"Failure key: {key} detected, remove all data related to experiments with failures.")
        if key not in ALLOWED_KEYS:
            raise ValueError(f"Unexpected HDR key {key} in {f}")
        by_key.setdefault(key, []).append(str(f))

    res = WorkloadResult(expt_prefix=expt_prefix, filenames=hdrs)

    # ----- OVERALL ------------------------------------------------------
    j_all = run_mergehdr(java_cp, sum(by_key.values(), [], ), ignore_start_seconds, ignore_end_seconds)
    res.overall_final  = json_to_latency_final(j_all)
    res.overall_series = json_to_series(j_all)
    runtime_s = j_all["runtime_us"] / 1_000_000.0
    if runtime_s > 0:
        res.overall_final.thr = round(j_all["overall"]["count"] / runtime_s)

    # ----- PER KEY ------------------------------------------------------
    for key, files in by_key.items():
        j = run_mergehdr(java_cp, files, ignore_start_seconds, ignore_end_seconds)
        res.per_key_final[key]  = json_to_latency_final(j)
        res.per_key_series[key] = json_to_series(j)
        r_s = j["runtime_us"] / 1_000_000.0
        if r_s > 0:
            res.per_key_final[key].thr = round(j["overall"]["count"] / r_s)
    
    # ----- Out file stats ------------------------------------------------
    out_files = sorted(out_path.parent.glob(expt_prefix + "*.out"))
    if not out_files:
        raise FileNotFoundError(f"No .out files for expt_prefix: {expt_prefix}")
    res.out_files_stats = [parse_throughput_from_out_file(f) for f in out_files]

    return res


# -------------------------------------------------------------------------
# Process .out file
# -------------------------------------------------------------------------

def _get_specific_value_from_out_file(out_path: Path, line_prefix: str) -> Optional[float]:
    if line_prefix is None or len(line_prefix) == 0:
        raise ValueError(f"line_prefix: {line_prefix} passed but expects non-empty string to match with lines in .out file")
    with out_path.open() as f:
        for line in f:
            # if line.startswith("[OVERALL], Throughput(ops/sec)"):
            if line.startswith(line_prefix):
                parts = line.strip().split(",")
                if len(parts) != 3:
                    raise ValueError("Unknown line format encountered, expected 3 comma separated values in ycsb .out file with float as the last value")
                val = float(parts[2].strip())
                return val
    return None

def parse_throughput_from_out_file(out_path: Path) -> OutFileStats:
    """
        Get the total operations from the ycsb out file by adding the operations for each individual keys such as 
        READ, UPDATE etc. for all ALLOWED keys. 
    """
    overall_thr = _get_specific_value_from_out_file(out_path, "[OVERALL], Throughput(ops/sec)")
    ops_count = 0
    for key in ALLOWED_KEYS:
        val_from_out_file = _get_specific_value_from_out_file(out_path, f"[{key}], Operations")
        if val_from_out_file is not None:
            ops_count +=  int(val_from_out_file)

    runtime_ms = _get_specific_value_from_out_file(out_path, "[OVERALL], RunTime(ms)")

    if runtime_ms is None:
        runtime_ms = FAILED_EXPT_IDENTIFIER
        return OutFileStats(filename=str(out_path), overall_stats=None, failed_expt=True)
    else:
        final_thr = (ops_count) / (runtime_ms / 1000.)
        latency_stats = LatencyStats(thr=final_thr)
        return OutFileStats(filename=str(out_path), overall_stats=latency_stats)


# -------------------------------------------------------------------------
# CSV
# -------------------------------------------------------------------------

def emit_csv(results: List[WorkloadResult]):
    header = (["expt_prefix"] +
              [f"OVERALL_{c}" for c in CSV_COLS] +
              [f"{k}_{c}" for k in ALLOWED_KEYS for c in CSV_COLS])
    w = csv.writer(sys.stdout)
    w.writerow(header)
    for r in results:
        row = [r.expt_prefix]
        row.extend(r.overall_final.as_list())
        for k in ALLOWED_KEYS:
            row.extend(r.per_key_final.get(k, LatencyStats()).as_list())
        w.writerow(row)



def plot_results(
    results: list[WorkloadResult],
    ignore_start: int = 0,
    ignore_end: int = 0,
    legend_wrap: bool = True,
    legend_fontsize: int = 6,
    legend_wrap_width_combined: int = 120,
    legend_wrap_width_individual: int = 250
):
    """
    Generate a PDF with:
      1) Page 1: tables of final stats per experiment, for OVERALL and each ALLOWED_KEY.
         The 'experiment' column is wrapped at 70 chars.
      2) Page 2: combined time‐series of OVERALL thr & p99 for all experiments.
      3) Pages 3..: for each key in ALLOWED_KEYS, combined thr & p99 timeseries (skip if no data).
      4) Pages after: individual metric plots per key.
    ignore_start: drop data < X seconds; ignore_end: drop data > (max_time - Y seconds).
    legend_wrap: wrap legend labels.
    legend_fontsize: legend text size.
    legend_wrap_width_combined: wrap width for thr&p99 legends.
    legend_wrap_width_individual: wrap width for individual plot legends.
    Figures are 1280×720 px (12.8×7.2in, dpi=100).
    """
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = f"metrics_timeseries_{ts}.pdf"
    pp = PdfPages(pdf_path)

    # margins
    bottom_margin = 0.2
    left_margin = 0.15
    right_margin = 0.98

    # prepare colors
    expts = [r.expt_prefix for r in results]
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    expt_colors = {exp: colors[i % len(colors)] for i, exp in enumerate(expts)}

    # legend kwargs
    legend_kwargs_combined = dict(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.15),
        borderaxespad=0.0,
        ncol=2,
        fontsize=legend_fontsize
    )
    legend_kwargs_individual = dict(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.15),
        borderaxespad=0.0,
        ncol=1,
        fontsize=legend_fontsize
    )

    warn_line = f"start seconds ignored = {ignore_start}  ;  end seconds ignored = {ignore_end}\n"

    # Page 1: tables
    fig = plt.figure(figsize=(12.8, 100), dpi=25)
    plt.axis('off')
    def build_row(r, key):
        stats = r.overall_final if key == 'OVERALL' else r.per_key_final.get(key, LatencyStats())
        exp_wrapped = textwrap.fill(r.expt_prefix, width=70)
        return [exp_wrapped, stats.thr, stats.min, stats.p50, stats.p95,
                stats.p99, stats.p999, stats.p9999, stats.max]
    headers = ['experiment','thr','min','p50','p95','p99','p999','p9999','max']
    txt = warn_line
    txt += '=== OVERALL ===\n'
    txt += tabulate([build_row(r,'OVERALL') for r in results], headers, 'grid', floatfmt='.2f') + '\n\n'
    for key in ALLOWED_KEYS:
        txt += f'=== {key} ===\n'
        txt += tabulate([build_row(r,key) for r in results], headers, 'grid', floatfmt='.2f') + '\n\n'
    fig.text(0.01, 0.99, txt, va='top', family='monospace', size=8)
    pp.savefig(fig)
    plt.close(fig)

    # prepare series and times
    series = {exp: {'OVERALL': r.overall_series, **r.per_key_series} for exp, r in zip(expts, results)}
    times = {}
    for exp in expts:
        times[exp] = {}
        for key, seq in series[exp].items():
            if not seq:
                times[exp][key] = []
            else:
                t0 = seq[0].t0_epoch_us
                times[exp][key] = [
                    ignore_start + ((pt.t0_epoch_us - t0)/1e6)
                    for pt in seq
                ]

    # Page 2: OVERALL thr & p99
    if any(series[exp]['OVERALL'] for exp in expts):
        fig, ax1 = plt.subplots(figsize=(12.8,7.2), dpi=100)
        ax2 = ax1.twinx()
        for exp in expts:
            seq = series[exp]['OVERALL']
            if not seq: continue
            t = times[exp]['OVERALL']
            th = [pt.thr for pt in seq]
            p99 = [pt.p99 for pt in seq]
            if not t: continue
            c = expt_colors[exp]
            light = tuple(ci + (1-ci)*0.5 for ci in mcolors.to_rgb(c))
            lbl_thr = textwrap.fill(f"{exp} thr", legend_wrap_width_combined) if legend_wrap else f"{exp} thr"
            lbl_p99 = textwrap.fill(f"{exp} p99", legend_wrap_width_combined) if legend_wrap else f"{exp} p99"
            ax1.plot(t, th, label=lbl_thr, color=c, linestyle='-')
            ax2.plot(t, p99, label=lbl_p99, color=light, linestyle='--')
        ax1.set_title('OVERALL thr & p99 over time' + '\n' + warn_line)
        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Throughput (ops/sec)')
        ax2.set_ylabel('p99 latency (µs)')
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        fig.subplots_adjust(left=left_margin, right=right_margin, bottom=bottom_margin)
        ax1.legend(h1+h2, l1+l2, **legend_kwargs_combined)
        fig.tight_layout()
        pp.savefig(fig)
        plt.close(fig)

    # Pages 3..: key-wise thr & p99
    for key in ALLOWED_KEYS:
        if not any(series[exp].get(key) for exp in expts):
            continue
        fig, ax1 = plt.subplots(figsize=(12.8,7.2), dpi=100)
        ax2 = ax1.twinx()
        for exp in expts:
            seq = series[exp].get(key, [])
            if not seq: continue
            t = times[exp][key]
            th = [pt.thr for pt in seq]
            p99 = [pt.p99 for pt in seq]
            if not t: continue
            c = expt_colors[exp]
            light = tuple(ci + (1-ci)*0.5 for ci in mcolors.to_rgb(c))
            lbl_thr = textwrap.fill(f"{exp} thr", legend_wrap_width_combined) if legend_wrap else f"{exp} thr"
            lbl_p99 = textwrap.fill(f"{exp} p99", legend_wrap_width_combined) if legend_wrap else f"{exp} p99"
            ax1.plot(t, th, label=lbl_thr, color=c, linestyle='-')
            ax2.plot(t, p99, label=lbl_p99, color=light, linestyle='--')
        ax1.set_title(f'{key} thr & p99 over time' + '\n' + warn_line)
        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Throughput (ops/sec)')
        ax2.set_ylabel('p99 latency (µs)')
        fig.subplots_adjust(left=left_margin, right=right_margin, bottom=bottom_margin)
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(h1 + h2, l1 + l2, **legend_kwargs_combined)
        fig.tight_layout()
        pp.savefig(fig)
        plt.close(fig)

    # individual metric pages
    metrics = ['thr','min','p50','p95','p99','p999','p9999','max']
    for key in ['OVERALL'] + ALLOWED_KEYS:
        for m in metrics:
            if not any(series[exp].get(key) for exp in expts):
                continue
            fig, ax = plt.subplots(figsize=(12.8,7.2), dpi=100)
            plotted = False
            for exp in expts:
                seq = series[exp].get(key, [])
                if not seq: continue
                t = times[exp][key]
                vals = [getattr(pt, m) for pt in seq]
                if not t: continue
                label = textwrap.fill(exp, legend_wrap_width_individual) if legend_wrap else exp
                ax.plot(t, vals, label=label, color=expt_colors[exp], linestyle='-')
                plotted = True
            if not plotted:
                plt.close(fig)
                continue
            ax.set_title(f'{key} – {m} over time' + '\n' + warn_line)
            ax.set_xlabel('Time (s)')
            ax.set_ylabel(m)
            fig.subplots_adjust(left=left_margin, right=right_margin, bottom=bottom_margin)
            ax.legend(**legend_kwargs_individual)
            fig.tight_layout()
            pp.savefig(fig)
            plt.close(fig)

    pp.close()
    print(f'Wrote PDF → {pdf_path}')

# -------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------
# Example command: python3.9 ~/ssd/fast_mongodb/benchmarks/ucsb/scripts/parse_ycsb/parse_ycsb.py --java-cp "/users/gargsaab/ssd/fast_mongodb/benchmarks/ucsb/scripts/parse_ycsb:/users/gargsaab/ssd/fast_mongodb/benchmarks/ucsb/scripts/parse_ycsb/jar/*" --ycsb-files ycsb*vanilla__node*local*out ycsb*vanilla_plus__node*local*out  > ./16-node-expt-2-cgroup.csv
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--java-cp", required=True,
                    help="Classpath containing MergeHdrStats + deps")
    ap.add_argument("--ycsb-files", nargs="+", required=True,
                    help="List of YCSB .out files (stem used to find .hdr)")
    ap.add_argument("--plot-metrics",
                        action="store_true",
                        help="If set, generate and save the metrics PDF via plot_results()")
    ap.add_argument("--ignore-start-seconds",
                        type=int,
                        default=0,
                        help="Ignore all data points earlier than this many seconds")
    ap.add_argument("--ignore-end-seconds",
                        type=int,
                        default=0,
                        help="Ignore all data points later than (max_time - this many seconds)")
    ap.add_argument("--ignore-failures",
                        type=bool,
                        default=False,
                        help="Ignore any FAILED operations and process and show whatever data there is.")
    args = ap.parse_args()

    for f in args.ycsb_files:
        if not pathlib.Path(f).exists():
            raise FileNotFoundError(f"YCSB file {f} does not exist")
        if not f.endswith(".out"):
            raise ValueError(f"YCSB file {f} must end with .out")
        if "extra_requester" in f:
            raise ValueError(
                f"YCSB file {f} should not contain 'extra_requester'. This script expects only the list of "
                f"localhost output file and finds the extra requester files.."
            )
        if not "local" in f:
            raise ValueError(f"Filename expected to contain local as local .out files are considered as the "
                             f"inputs in this script. Other files are found using this filename.")
    
    results = [process_out_file(pathlib.Path(f), args.java_cp, args.ignore_failures, 
                                args.ignore_start_seconds, args.ignore_end_seconds)
               for f in args.ycsb_files]
    results = [r for r in results if r is not None]

    gc_storm_detected = False
    if args.ignore_start_seconds == 0 and args.ignore_end_seconds == 0:
        for r in results:
            if r.is_gc_storm_detected():
                gc_storm_detected = True
                print(f"ERROR: GC STORM DETECTED FOR THE FOLLOWING WORKLOAD:", file=sys.stderr)
                print(r, file=sys.stderr)
                print("==========================================================================================",  file=sys.stderr)
        if gc_storm_detected:
            raise ValueError(f"GC storm detected, check logs for more details. Exiting...")

    emit_csv(results)

    # For interactive inspection:
    #for r in results:
    #    print(r)
    
    if args.plot_metrics:
        plot_results(
            results, 
            ignore_start=args.ignore_start_seconds,
            ignore_end=args.ignore_end_seconds,
        )
    
    if args.ignore_start_seconds > 0 or args.ignore_end_seconds > 0:
        ignore_warn_msg = f"WARNING: You used --ignore-start-seconds={args.ignore_start_seconds} " \
                          f"and/or --ignore-end-seconds={args.ignore_end_seconds}, so the final " \
                          f"stats may not reflect the entire experiment duration. GC Storm checks for YCSB " \
                          f"are disabled when ignoring any data."
        print(f"\033[1;31m{ignore_warn_msg}\033[0m", file=sys.stderr)

if __name__ == "__main__":
    main()
