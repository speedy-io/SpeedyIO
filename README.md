# SpeedyIO

**SpeedyIO** is a low-level performance optimization library for **Apache Cassandra** that targets **tail latency (P99 / P99.9)** in production workloads.

It is implemented as a **shared library using `LD_PRELOAD`**, allowing it to integrate with Cassandra **without source changes, forks, or JVM modifications**.

SpeedyIO has demonstrated **~30% reductions in P99 latency** under realistic read/write workloads.

---

## Experimental Results

A detailed write-up of all experiments, including:

* Hardware configurations
* Cassandra versions and settings
* YCSB workloads
* Before/after latency distributions
* Failure modes and limitations

is available here:

**📊 Benchmark Results & Analysis:**
👉 **[LINK TO BLOG – coming soon]**

---

## Supported Cassandra Versions

SpeedyIO has been validated against:

* Cassandra **3.11**
* Cassandra **4.1.8**
* Cassandra **5.0.3**

---

## Build Requirements

* Linux (glibc)
* **gcc ≥ 11**
* x86_64 or ARM64

### Build

```bash
make -j
```

The compiled library will be available at:

```text
lib/lib_speedyio_release.so
```

---

## Running SpeedyIO on Your Cluster

SpeedyIO is enabled by adding it to Cassandra’s startup script via `LD_PRELOAD`.

Edit:

```text
cassandra/bin/cassandra
```

Add the following **before** the service launch:

```bash
# Add SpeedyIO library to LD_PRELOAD, preserving any existing values
SPEEDYIO_LDPRELOAD_PATH="/absolute/path/to/lib/lib_speedyio_release.so"

if [ -z "$LD_PRELOAD" ]; then
    export LD_PRELOAD="$SPEEDYIO_LDPRELOAD_PATH"
else
    export LD_PRELOAD="$LD_PRELOAD:$SPEEDYIO_LDPRELOAD_PATH"
fi

echo "Using LD_PRELOAD in cassandra: $LD_PRELOAD"
```

The standard Cassandra startup logic remains unchanged:

```bash
launch_service "$pidfile" "$foreground" "$properties" "$classname"
```

Restart Cassandra and run your workload or benchmarks.

---

## Benchmarking Methodology

SpeedyIO is typically evaluated using **YCSB CRUD workloads**, including:

* Read-heavy workloads
* Write-heavy workloads
* Mixed read/write workloads

Primary evaluation metrics:

* **P99 latency**
* **P99.9 latency**
* Latency stability under sustained load

---

## Partial Cluster Rollout

SpeedyIO **does not require all nodes to run the library**.

Mixed clusters are supported, allowing:

* Side-by-side comparison
* Canary deployments
* Incremental rollout strategies

---

## Known Limitations

SpeedyIO currently does **not** support:

* Nodes with **multiple NUMA sockets**
* Cassandra config `disk_access_mode = mmap` and `direct`

Recommended configurations:

```yaml
disk_access_mode: standard
```

or

```yaml
disk_access_mode: mmap_index_only
```

---

SpeedyIO is relevant for teams that:

* Operate large or latency-sensitive Cassandra clusters
* Already understand JVM, GC, compaction, and schema tuning
* Care about tail latency
* Want performance improvements in their Cassandra cluster

This is **not** a replacement for fundamental Cassandra tuning.

---

## Working With Us (Enterprise & Production)

SpeedyIO is actively being evaluated for **production use cases**.

For teams interested in:

* Guided benchmarking
* Production readiness evaluation
* Deployment and rollout strategies
* Commercial licensing or long-term support

**Contact:**
📧 **[shaleengarg.in@gmail.com](mailto:shaleengarg.in@gmail.com)**

---

## Why This Project Exists

Most Cassandra performance work stops at configuration and JVM tuning.
SpeedyIO explores the friction between OS and Cassandra that hurts performance.

If you care about improving Cassandra’s tail behavior at runtime, lets talk.