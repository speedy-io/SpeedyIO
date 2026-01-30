# Cassandra Node Configuration

Baseline **hardware**, **OS**, and **kernel/runtime** settings for bare-metal nodes used in Cassandra benchmarks and latency experiments.

---

## Hardware

| Component       | Value                  |
| --------------- | ---------------------- |
| **Server Type** | Bare-metal             |
| **CPU**         | Intel Xeon E5-2640     |
| **Memory**      | 64 GB                  |
| **Storage**     | SATA SSD (Single Disk) |

---

## Operating System

| Item                       | Value                                |
| -------------------------- | ------------------------------------ |
| **Distribution**           | CentOS Stream 8                      |
| **Kernel Versions Tested** | `4.18.0-553.6.1.el8`, `6.18.6-1.el8` |

---

## System Limits (`ulimit -a`)

```bash
core file size          (blocks, -c) 0
data seg size           (kbytes, -d) unlimited
scheduling priority             (-e) 0
file size               (blocks, -f) unlimited
pending signals                 (-i) 512915
max locked memory       (kbytes, -l) 8192
max memory size         (kbytes, -m) unlimited
open files                      (-n) 300000
pipe size            (512 bytes, -p) 8
POSIX message queues     (bytes, -q) 819200
real-time priority              (-r) 0
stack size              (kbytes, -s) 8192
cpu time               (seconds, -t) unlimited
max user processes              (-u) 512915
virtual memory          (kbytes, -v) unlimited
file locks                      (-x) unlimited
```

### Notable Overrides

* **Open files (`-n`)**: `300000`
* **Max user processes (`-u`)**: `512915`

---

## Block Device Settings

### Read-Ahead (`/dev/sda`)

```bash
sudo blockdev --getra /dev/sda
64
```

---

## Huge Page Settings

```bash
cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]
```

**Effective Mode:** `never`

---

## Swap Settings

Swap is **disabled** on the system, following Cassandra best practices.

Reference:
[https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/install/installRecommendSettings.html#Disableswap](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/install/installRecommendSettings.html#Disableswap)

### Swappiness

```bash
sysctl vm.swappiness
vm.swappiness = 60
```

---

## Notes

* High `ulimit` values are required to prevent file descriptor and process exhaustion under heavy load.
* Transparent Huge Pages are disabled to reduce latency spikes.
* Swap is disabled to avoid unpredictable I/O latency during memory pressure.

---
