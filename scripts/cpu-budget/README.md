# smt\_numa.sh — NUMA-balanced CPU offlining by whole cores

A small Bash utility to **keep exactly N logical CPUs online**, while always:

* respecting **threads-per-core (SMT)**,
* keeping/turning off **whole physical cores only** (never leaving a lone hyperthread on a core),
* and **evenly distributing** the kept cores **across NUMA nodes** (important on AMD EPYC where each CCD/CCX has its own caches and memory locality).

It also includes commands to restore all CPUs and to show topology/status.

---

## Features at a glance

* ✔️ **Whole-core semantics**: never leaves a single thread online on any core.
* ✔️ **NUMA-balanced**: keeps the same number of cores per NUMA node.
* ✔️ **Input validation**:

  * `N ≥ 2`
  * `N` is a multiple of **threads-per-core (SMT)** (e.g., 2 on EPYC with SMT2)
  * `N` is a multiple of **the number of NUMA nodes**
* ✔️ **Safe application**: only writes `/sys/devices/system/cpu/cpu*/online` when available.
* ✔️ **Restore**: one command to bring all CPUs back online.
* ✔️ **Status**: prints `lscpu` tables and per-core sibling lists for easy verification.

---

## Requirements & assumptions

* **OS**: Linux with CPU hotplug via sysfs (tested on CentOS 8 only).
* **Kernel**: CPU hotplug enabled (`CONFIG_HOTPLUG_CPU`).
* **Tools**: `bash`, `lscpu`, `awk`, and standard coreutils.
* **Permissions**: writing to `/sys/devices/system/cpu/cpu*/online` usually requires `root`. The script will use `sudo tee` automatically when needed.
* **Topology**: Assumes a *uniform* SMT level across cores (typical on x86\_64).
* **NUMA**: Relies on `lscpu -p` to map CPUs→cores→NUMA nodes; heterogeneous/odd mappings are handled by enumerating real tuples rather than assuming ranges.

> ⚠️ Some platforms don’t expose `cpu0/online`. The script won’t try to force it and will log a note.

---

## Usage

```bash
# Keep exactly N hyperthreads (logical CPUs) online, evenly across NUMA nodes.
./budget_cpu.sh set <N>

# Restore all CPUs
./budget_cpu.sh restore

# Show current topology and online state
./budget_cpu.sh status
```

### Examples (AMD EPYC 9354P, 1 socket, SMT=2, 4 NUMA nodes, 64 CPUs total)

* Keep 16 logical CPUs → **8 cores total**, **2 cores per NUMA node**:

  ```bash
  ./smt_numa.sh set 16
  ```
* Keep 32 logical CPUs → **16 cores total**, **4 cores per NUMA node**:

  ```bash
  ./smt_numa.sh set 32
  ```
* Bring everything back:

  ```bash
  ./smt_numa.sh restore
  ```
* Inspect:

  ```bash
  ./smt_numa.sh status
  ```

If you pass a value that violates the rules (e.g., not divisible by SMT or by NUMA node count), the script exits with a clear error.

---

## What exactly the script does

1. **Discovers topology**

   * `threads-per-core (SMT)` from `lscpu` (fallback to `thread_siblings_list`).
   * `NUMA node count` from `lscpu`.
   * Enumerates **(CPU, CORE, NODE)** tuples from `lscpu -p`.

2. **Validates your target N**

   * `N ≥ 2`
   * `N % SMT == 0`
   * `N % NUMA_nodes == 0`

3. **Computes the plan**

   * `cores_to_keep = N / SMT`
   * `cores_per_node = cores_to_keep / NUMA_nodes`

4. **Selects cores evenly per NUMA node**

   * Builds a unique, sorted list of **core IDs per NUMA node**.
   * Picks the **lowest `cores_per_node` core IDs** in each node (you can change the selection policy if you want “last”, round-robin, or a custom order).

5. **Applies the plan**

   * Keeps **all hyperthreads** of selected cores online.
   * Offlines **all hyperthreads** of unselected cores (skipping CPUs without an `online` knob).
   * Prints `lscpu -e` and a per-core sibling summary for verification.

6. **Restores on demand**

   * `restore` iterates all `cpu*/online` and sets to `1` where possible.

---

## Verification checklist

After `set`:

```bash
# Should show 'yes' only for CPUs of the selected cores
lscpu -e=CPU,CORE,NODE,ONLINE

# Each line lists sibling thread IDs per core (both should be online/offline together)
cat /sys/devices/system/cpu/cpu*/topology/thread_siblings_list

# Online bitmap
cat /sys/devices/system/cpu/online
```

Per-node counts should match `cores_per_node * SMT` logical CPUs online.

---

## Common pitfalls & how to avoid them

* **“Could not offline cpuX (may be non-hotpluggable).”**
  Some CPUs (often `cpu0`) can’t be offlined, or the kernel/hypervisor disallows it. That’s OK; the script continues and reports the final state.

* **IRQs pinned to a CPU prevent offlining**
  If a CPU services non-migratable interrupts, offlining may fail. Consider:

  ```bash
  # Move all IRQs off a list of CPUs (example: 24–63)
  for i in /proc/irq/*/smp_affinity_list; do echo 0-23 | sudo tee "$i" >/dev/null; done
  systemctl stop irqbalance  # temporarily, if it fights you
  ```

* **Pinned tasks/cgroups**
  Tasks pinned with `taskset`/cpusets or RT threads can block offlining. Migrate them:

  ```bash
  taskset -pc 0-23 <pid>
  # or manage cpusets/cgroups to exclude offlined CPUs
  ```

* **Containers**
  Inside containers you typically **cannot** write to host sysfs; run this on the host.

---

## Return codes

* `0` success
* `1` validation/user error (bad input, divisibility rules not met, etc.)
* `2` system/internal error (missing commands, unreadable sysfs, write fails)

---

## Safety/Recovery

* Always run a quick `./smt_numa.sh status` before and after.
* To revert any change:

  ```bash
  ./smt_numa.sh restore
  ```

---

## FAQ

**Q: Why must N be a multiple of NUMA nodes?**
A: To guarantee identical counts per node (true balance). If you need an uneven split, we can add a flag like `--node-plan 4,4,2,2`.

**Q: Why whole cores only?**
A: Mixing hyperthreads from the same core with different online states is a common source of performance anomalies.

**Q: Will this change survive reboot?**
A: No—use the systemd unit shown above to reapply at boot.

---
