/******************************************************************************
 * MergeHdrStats                                                              *
 * -------------------------------------------------------------------------- *
 * Purpose                                                                    *
 * =======                                                                    *
 *  ▸ Merge one-or-more **HdrHistogram “interval logs”** produced by YCSB     *
 *    (the *.hdr* files created when `measurementtype=hdrhistogram`).         *
 *  ▸ Re-bin every source interval into **canonical, epoch-aligned 10-second  *
 *    buckets** so different requester nodes that start at different times    *
 *    are aligned on a common timeline.                                       *
 *  ▸ If a slice from a loader straddles a bucket boundary the code **splits  *
 *    the counts proportionally** between the two buckets – so every latency  *
 *    sample is counted exactly once at the slice level (no drops/doubles).    *
 *  ▸ Optionally **trim** the first X seconds and/or last Y seconds of the    *
 *    overall run (global bounds across all HDR files) before merging.        *
 *  ▸ Writes a single JSON document with per-bucket stats + provenance, and   *
 *    an overall summary; includes trim metadata.                              *
 *                                                                            *
 * New CLI flags                                                              *
 * --------------                                                             *
 *   --ignore-head-sec=<X> | --ignore-head-sec X    (default 0)               *
 *   --ignore-tail-sec=<Y> | --ignore-tail-sec Y    (default 0)               *
 *                                                                            *
 *   Trimming is based on **global** raw bounds discovered in Pass A:         *
 *     effective_start_us = raw_min_start_us + X * 1e6                        *
 *     effective_end_us   = raw_max_end_us   - Y * 1e6                        *
 *   All merging considers only timestamps in [effective_start_us,            *
 *   effective_end_us). If the window is empty, the program errors out.       *
 *                                                                            *
 * Build                                                                      *
 * -----                                                                      *
 *  javac -cp ".:lib/*" MergeHdrStats.java                                    *
 *                                                                            *
 * Run                                                                        *
 * ---                                                                        *
 *  java -cp ".:lib/*" MergeHdrStats [--ignore-head-sec X] [--ignore-tail-sec Y] \
 *       output.json file1.hdr file2.hdr ...                                  *
 *                                                                            *
 * JSON Schema (abbreviated)                                                  *
 * --------------------------------                                           *
 * {                                                                          *
 *   "input_files"      : "fileA,fileB",                                      *
 *   "interval_seconds" : 10,                                                 *
 *   "raw_start_us"     : <min start across all>,                             *
 *   "raw_end_us"       : <max end across all>,                               *
 *   "effective_start_us": <post-trim>,                                       *
 *   "effective_end_us"  : <post-trim>,                                       *
 *   "ignored_head_seconds" : X,                                              *
 *   "ignored_tail_seconds" : Y,                                              *
 *   "kept_fraction"    : (effective_end_us - effective_start_us) /           *
 *                        (raw_end_us - raw_start_us),                        *
 *   "runtime_us"       : effective_end_us - effective_start_us,              *
 *   "intervals" : [ { "t0_epoch_us": ..., "count": ..., "p50_us": ...,       *
 *                    "sources":[ { "file":..., "slice_start_us":..., ... } ] *
 *                  }, ... ],                                                 *
 *   "overall" : { "count": …, "p95_us": …, … }                               *
 * }                                                                          *
 *                                                                            *
 * Author/maintainer: <your-name-here>                                        *
 ******************************************************************************/

import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.HistogramLogReader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

public class MergeHdrStats {

    /* ======================================================================
       SECTION 1 — CONSTANTS
       ==================================================================== */

    /** Canonical bucket width (10 s → 10 000 000 µs). */
    private static final long CANONICAL_BUCKET_US = 10_000_000L;

    /** Histograms we create can record up to 1 hour of latency (in µs). */
    private static final long HISTOGRAM_MAX_US = 3_600_000_000L;

    /** HdrHistogram significant-digits — matches YCSB’s default (3). */
    private static final int  HISTOGRAM_SIG_DIGITS = 3;

    /* ======================================================================
       SECTION 2 — INTERNAL HELPER TYPES
       ==================================================================== */

    /**
     * Holds both the merged histogram **and** the provenance list
     * (“which source slices contributed?”) for one canonical bucket.
     */
    private static final class Bucket {
        final Histogram hist  = new Histogram(HISTOGRAM_MAX_US, HISTOGRAM_SIG_DIGITS);
        final JSONArray provenance = new JSONArray();
    }

    /**
     * Maintains a per-slice **carry** across multiple proportional splits of
     * the same source slice. This ensures the sum of counts allocated into
     * different buckets equals the rounded theoretical total for the kept span.
     *
     * We intentionally use a **single** carry across all value-bins. That is
     * sufficient to conserve totals at the slice level without exploding state.
     */
    private static final class SliceScaler {
        private final Histogram src;
        private double carry = 0.0;

        SliceScaler(Histogram src) { this.src = src; }

        /**
         * Return a histogram whose counts are approximately `fraction` of src,
         * using Math.round(count * fraction + carry) while **carrying residual**
         * forward for the next fragment of the same slice.
         */
        Histogram take(double fraction) {
            Histogram out = new Histogram(HISTOGRAM_MAX_US, HISTOGRAM_SIG_DIGITS);
            if (fraction <= 0) return out;

            for (HistogramIterationValue iv : src.recordedValues()) {
                double exact   = iv.getCountAtValueIteratedTo() * fraction + carry;
                long scaledCnt = Math.round(exact);
                carry          = exact - scaledCnt; // residual to next (bin or fragment)
                if (scaledCnt > 0) {
                    out.recordValueWithCount(iv.getValueIteratedTo(), scaledCnt);
                }
            }
            return out;
        }
    }

    /**
     * Convert a timestamp that may be in **milliseconds** to micro-seconds.
     * Any timestamp < 1 × 10¹⁴ is treated as ms and multiplied by 1 000.
     */
    private static long toMicroseconds(long ts) {
        final long MILLIS_VS_MICROS_THRESHOLD = 100_000_000_000_000L; // 1e14
        return (ts < MILLIS_VS_MICROS_THRESHOLD) ? ts * 1_000L : ts;
    }

    /* ======================================================================
       SECTION 3 — ENTRY POINT
       ==================================================================== */

    public static void main(String[] args) throws Exception {

        /* ---------- 3.1  CLI parsing ---------------------------------- */
        Cli cli = Cli.parse(args);
        if (cli.showHelp) {
            Cli.printUsageAndExit(0);
        }
        if (cli.outputJsonPath == null || cli.hdrFiles.isEmpty()) {
            System.err.println("ERROR: Missing output.json and/or input .hdr files.");
            Cli.printUsageAndExit(1);
        }
        long ignoreHeadSec = cli.ignoreHeadSec;
        long ignoreTailSec = cli.ignoreTailSec;
        String outputJsonPath = cli.outputJsonPath;
        String[] hdrFiles = cli.hdrFiles.toArray(new String[0]);

        System.out.println("=== Input HDR files ===");
        StringJoiner csvInputFiles = new StringJoiner(",");
        for (String f : hdrFiles) { System.out.println("  " + f); csvInputFiles.add(f); }

        if (ignoreHeadSec < 0 || ignoreTailSec < 0) {
            throw new IllegalArgumentException("Ignore seconds must be non-negative.");
        }

        /* ---------- 3.2  Start-time drift validation (unchanged) ------- */
        System.out.println("=== Validating start times ===");
        validateStartTimes(hdrFiles);

        /* ---------- 3.3  PASS A — discover global raw bounds ----------- */
        System.out.println("=== Scanning to discover global raw bounds ===");
        Bounds raw = computeGlobalBounds(hdrFiles);
        System.out.printf("Raw bounds: start=%d, end=%d (duration %.3fs)\n",
                raw.minStartUs, raw.maxEndUs, (raw.maxEndUs - raw.minStartUs) / 1_000_000.0);

        if (raw.maxEndUs <= raw.minStartUs) {
            throw new IllegalStateException("No data found (raw end <= raw start).");
        }

        long trimStartUs = raw.minStartUs + ignoreHeadSec * 1_000_000L;
        long trimEndUs   = raw.maxEndUs   - ignoreTailSec * 1_000_000L;

        if (trimEndUs <= trimStartUs) {
            double rawSeconds = (raw.maxEndUs - raw.minStartUs) / 1_000_000.0;
            throw new IllegalStateException(String.format(
                    "Trim window is empty or negative after applying head=%ds, tail=%ds. " +
                    "Raw duration is only %.3fs.", ignoreHeadSec, ignoreTailSec, rawSeconds));
        }

        System.out.printf("Effective window: [%d, %d) (duration %.3fs)\n",
                trimStartUs, trimEndUs, (trimEndUs - trimStartUs) / 1_000_000.0);

        /* ---------- 3.4  Core data structures -------------------------- */

        // TreeMap keeps buckets globally sorted by epoch-start time.
        final Map<Long, Bucket> buckets = new TreeMap<>();

        // Grand-total histogram (merged from **fragments only**).
        final Histogram overall = new Histogram(HISTOGRAM_MAX_US, HISTOGRAM_SIG_DIGITS);

        /* ---------- 3.5  PASS B — read & merge with trimming ----------- */
        System.out.println("=== Merging with trimming ===");
        for (String file : hdrFiles) {
            System.out.println(file);
            try (HistogramLogReader reader = new HistogramLogReader(new File(file))) {

                EncodableHistogram enc;
                while ((enc = reader.nextIntervalHistogram()) != null) {
                    if (!(enc instanceof Histogram)) continue;   // skip non-data rows
                    Histogram slice = (Histogram) enc;

                    long s = toMicroseconds(slice.getStartTimeStamp());
                    long e = toMicroseconds(slice.getEndTimeStamp());
                    if (e <= s) continue; // degenerate

                    // Clip to effective global window
                    long sClipped = Math.max(s, trimStartUs);
                    long eClipped = Math.min(e, trimEndUs);
                    if (sClipped >= eClipped) continue; // slice is completely outside window

                    final long sliceSpan = e - s;
                    final long keptSpan  = eClipped - sClipped;
                    if (sliceSpan <= 0) continue;

                    // Prepare per-slice scaler with carry across all bucket fragments.
                    SliceScaler scaler = new SliceScaler(slice);

                    // Walk the clipped portion, bucket by bucket
                    long cursor = sClipped;
                    while (cursor < eClipped) {
                        long bucketKey  = (cursor / CANONICAL_BUCKET_US) * CANONICAL_BUCKET_US;
                        long bucketEnd  = bucketKey + CANONICAL_BUCKET_US;
                        long overlapEnd = Math.min(bucketEnd, eClipped);
                        long overlapSpan = overlapEnd - cursor;

                        double fraction = overlapSpan / (double) sliceSpan; // fraction of original slice

                        Histogram fragment = scaler.take(fraction);

                        // fetch/create the canonical bucket
                        Bucket bucket = buckets.computeIfAbsent(bucketKey, k -> new Bucket());
                        bucket.hist.add(fragment);
                        overall.add(fragment);

                        // provenance
                        JSONObject srcInfo = new JSONObject()
                                .put("file", file)
                                .put("slice_start_us",  s)
                                .put("slice_end_us",    e)
                                .put("portion_start_us", cursor)
                                .put("portion_end_us",   overlapEnd)
                                .put("count", fragment.getTotalCount());
                        bucket.provenance.put(srcInfo);

                        cursor = overlapEnd;
                    }

                    // Optional: sanity — allocated total equals rounded theoretical kept total
                    // (not emitted, but useful if you add asserts or logs)
                    // long theoretical = Math.round(slice.getTotalCount() * (keptSpan / (double) sliceSpan));
                    // long realized    = /* could sum fragments' counts if you track per-slice */
                }
            }
        }

        /* ---------- 3.6  Assemble JSON output + coverage sanity check ---------- */
        JSONArray intervalsJson = new JSONArray();
        long sumCoveredUs = 0L;   // accumulate per-bucket covered_us

        for (Map.Entry<Long,Bucket> entry : buckets.entrySet()) {
            long bucketStartUs = entry.getKey();
            Bucket b           = entry.getValue();

            long bucketEndUs = bucketStartUs + CANONICAL_BUCKET_US;
            long coveredUs = Math.max(0L,
                Math.min(bucketEndUs, trimEndUs) - Math.max(bucketStartUs, trimStartUs));

            sumCoveredUs += coveredUs;

            JSONObject bucketObj = histogramStatsToJson(b.hist)
                    .put("t0_epoch_us", bucketStartUs)
                    .put("covered_us",  coveredUs)
                    .put("sources",     b.provenance);

            intervalsJson.put(bucketObj);
        }

        long runtimeUs = trimEndUs - trimStartUs;

        // sanity: sum of covered_us across emitted buckets must equal runtime_us
        if (sumCoveredUs != runtimeUs) {
            throw new IllegalStateException(String.format(
                "Sanity check failed: sum(covered_us)=%d us != runtime_us=%d us. " +
                "Buckets=%d, window=[%d,%d). This indicates uncovered gaps (no bucket created) " +
                "inside the trimmed window or a coverage calculation bug.",
                sumCoveredUs, runtimeUs, buckets.size(), trimStartUs, trimEndUs));
        }

        JSONObject root = new JSONObject()
                .put("input_files",          csvInputFiles.toString())
                .put("interval_seconds",     CANONICAL_BUCKET_US / 1_000_000)
                .put("raw_start_us",         raw.minStartUs)
                .put("raw_end_us",           raw.maxEndUs)
                .put("effective_start_us",   trimStartUs)
                .put("effective_end_us",     trimEndUs)
                .put("ignored_head_seconds", ignoreHeadSec)
                .put("ignored_tail_seconds", ignoreTailSec)
                .put("kept_fraction",        (trimEndUs - trimStartUs) / (double)(raw.maxEndUs - raw.minStartUs))
                .put("runtime_us",           runtimeUs)
                .put("intervals",            intervalsJson)
                .put("overall",              histogramStatsToJson(overall));

        /* ---------- 3.7  Write file ----------------------------------- */
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputJsonPath))) {
            pw.println(root.toString(2));   // pretty-print with indent=2
        }
        System.out.println("Wrote " + outputJsonPath);

        /* ---------- 3.8  Cheap invariants (optional hard asserts) ------ */
        // You can uncomment to enforce at runtime.
        // long sumIntervals = 0L;
        // for (Bucket b : buckets.values()) sumIntervals += b.hist.getTotalCount();
        // if (sumIntervals != overall.getTotalCount()) {
        //     throw new IllegalStateException("overall.count != sum(interval.count)");
        // }
    }

    /* ======================================================================
       SECTION 4 — UTILITY FUNCTIONS
       ==================================================================== */

    /**
     * Convert a Histogram into a JSON object with commonly used
     * percentiles & min/max. Works for per-bucket and overall.
     */
    private static JSONObject histogramStatsToJson(Histogram h) {
        return new JSONObject()
                .put("count",    h.getTotalCount())
                .put("min_us",   h.getTotalCount() == 0 ? 0 : h.getMinValue())
                .put("p50_us",   h.getTotalCount() == 0 ? 0 : h.getValueAtPercentile(50))
                .put("p95_us",   h.getTotalCount() == 0 ? 0 : h.getValueAtPercentile(95))
                .put("p99_us",   h.getTotalCount() == 0 ? 0 : h.getValueAtPercentile(99))
                .put("p999_us",  h.getTotalCount() == 0 ? 0 : h.getValueAtPercentile(99.9))
                .put("p9999_us", h.getTotalCount() == 0 ? 0 : h.getValueAtPercentile(99.99))
                .put("max_us",   h.getTotalCount() == 0 ? 0 : h.getMaxValue());
    }

    /* ======================================================================
       SECTION 5 — VALIDATION & BOUNDS DISCOVERY
       ==================================================================== */

    private static final class Bounds {
        final long minStartUs;
        final long maxEndUs;
        Bounds(long minStartUs, long maxEndUs) {
            this.minStartUs = minStartUs; this.maxEndUs = maxEndUs;
        }
    }

    /**
     * Pass A: scan every interval of every file to discover the **raw**
     * global min start and max end timestamps (in µs).
     */
    private static Bounds computeGlobalBounds(String[] hdrFiles) throws Exception {
        long minStart = Long.MAX_VALUE;
        long maxEnd   = Long.MIN_VALUE;

        for (String file : hdrFiles) {
            try (HistogramLogReader r = new HistogramLogReader(new File(file))) {
                EncodableHistogram enc;
                while ((enc = r.nextIntervalHistogram()) != null) {
                    if (!(enc instanceof Histogram)) continue;
                    Histogram h = (Histogram) enc;
                    long s = toMicroseconds(h.getStartTimeStamp());
                    long e = toMicroseconds(h.getEndTimeStamp());
                    if (e <= s) continue;
                    if (s < minStart) minStart = s;
                    if (e > maxEnd)   maxEnd   = e;
                }
            }
        }

        if (minStart == Long.MAX_VALUE || maxEnd == Long.MIN_VALUE) {
            throw new IllegalStateException("No interval histograms found in any input file.");
        }
        return new Bounds(minStart, maxEnd);
    }

    /**
     * Verify that the difference between the earliest and latest
     * interval-0 timestamp of all HDR files is ≤ 30 s.
     *
     * @param hdrFiles  array of *.hdr* file paths
     * @throws Exception if the drift exceeds the threshold
     */
    private static void validateStartTimes(String[] hdrFiles) throws Exception {

        final long THRESHOLD_US = 30_000_000L;          // 30 seconds

        long minStart = Long.MAX_VALUE;
        long maxStart = Long.MIN_VALUE;

        for (String file : hdrFiles) {
            try (HistogramLogReader r = new HistogramLogReader(new File(file))) {

                /* find the first “real” histogram row (skip tags/comments) */
                EncodableHistogram enc;
                do { enc = r.nextIntervalHistogram(); }
                while (enc != null && !(enc instanceof Histogram));

                if (enc == null) {
                    throw new IllegalArgumentException(
                            "No interval histogram found in " + file);
                }

                long t0 = toMicroseconds(((Histogram) enc).getStartTimeStamp());
                System.out.printf("HDR file %s starts at %d µs epoch\n", file, t0);
                minStart = Math.min(minStart, t0);
                maxStart = Math.max(maxStart, t0);
            }
        }

        long driftUs = maxStart - minStart;
        System.out.printf("Start-time drift across HDR files: %.1f s (max-min = %d µs)\n",
                driftUs / 1_000_000.0, driftUs);
        if (driftUs > THRESHOLD_US) {
            throw new IllegalStateException(String.format(
                    "Start-time drift across HDR files is %.1f s (>%d s). " +
                    "Ensure loaders are time-synchronised or align in post-processing.",
                    driftUs / 1_000_000.0, THRESHOLD_US / 1_000_000));
        }
    }

    /* ======================================================================
       SECTION 6 — CLI PARSER
       ==================================================================== */

    /**
     * Minimal, deterministic CLI parser.
     *
     * Grammar (order-insensitive for flags):
     *   MergeHdrStats [--ignore-head-sec <X>] [--ignore-tail-sec <Y>] output.json file1.hdr [file2.hdr ...]
     *   MergeHdrStats [--ignore-head-sec=<X>] [--ignore-tail-sec=<Y>] output.json file1.hdr ...
     */
    private static final class Cli {
        final long ignoreHeadSec;
        final long ignoreTailSec;
        final String outputJsonPath;
        final List<String> hdrFiles;
        final boolean showHelp;

        private Cli(long head, long tail, String out, List<String> files, boolean help) {
            this.ignoreHeadSec = head;
            this.ignoreTailSec = tail;
            this.outputJsonPath = out;
            this.hdrFiles = files;
            this.showHelp = help;
        }

        static void printUsageAndExit(int code) {
            System.out.println(
                    "Usage:\n" +
                    "  java -cp \".:lib/*\" MergeHdrStats [--ignore-head-sec X] [--ignore-tail-sec Y] output.json file1.hdr [file2.hdr ...]\n" +
                    "  java -cp \".:lib/*\" MergeHdrStats [--ignore-head-sec=X] [--ignore-tail-sec=Y] output.json file1.hdr ...\n" +
                    "\n" +
                    "Flags:\n" +
                    "  --ignore-head-sec   Seconds to drop from the global start (default 0)\n" +
                    "  --ignore-tail-sec   Seconds to drop from the global end   (default 0)\n" +
                    "  -h, --help          Show this help\n");
            System.exit(code);
        }

        static Cli parse(String[] args) {
            long head = 0, tail = 0;
            String out = null;
            List<String> files = new ArrayList<>();
            boolean help = false;

            for (int i = 0; i < args.length; i++) {
                String a = args[i];

                if ("-h".equals(a) || "--help".equals(a)) {
                    help = true;
                    continue;
                }

                if (a.startsWith("--ignore-head-sec")) {
                    Long v = parseLongFlag(a, args, i);
                    if (v == null) { // value consumed from next arg
                        i++;
                    } else {
                        head = v;
                    }
                    if (v == null && i < args.length) head = tryParseLong(args[i]);
                    continue;
                }

                if (a.startsWith("--ignore-tail-sec")) {
                    Long v = parseLongFlag(a, args, i);
                    if (v == null) {
                        i++;
                    } else {
                        tail = v;
                    }
                    if (v == null && i < args.length) tail = tryParseLong(args[i]);
                    continue;
                }

                // positional
                if (out == null) {
                    out = a;
                } else {
                    files.add(a);
                }
            }
            return new Cli(head, tail, out, files, help);
        }

        /** Parses --flag=value; returns null if value not in same token. */
        private static Long parseLongFlag(String token, String[] args, int idx) {
            int eq = token.indexOf('=');
            if (eq >= 0) {
                String v = token.substring(eq + 1).trim();
                return tryParseLong(v);
            }
            // value expected in next token; return null to signal caller to read it
            return null;
        }

        private static long tryParseLong(String s) {
            try { return Long.parseLong(s); }
            catch (Exception e) {
                throw new IllegalArgumentException("Expected integer value, got: " + s);
            }
        }
    }
}