# Release checklist

- export SPEEDYIO_CFG_ENV=/path/to/speedyio.conf
- Flags to use OBF_DBG_PRINTS
- Disable all debugging flags

- run `strings lib_speedyio_release.so` before packaging and shipping this binary. Manually go through the output and see if any
meaningful messages have been added to the it.

---

# Printing Rules for `shared_lib`

## Overview
The shared library uses different printing functions depending on whether output should be visible to customers in raw form or obfuscated before release.

---

## 1. When to Use `printf` / `fprintf`
- **Purpose:** Output that is **okay for the customer to see** directly.
- **Behavior:** These statements will **NOT** be obfuscated before release.
- **Example:**
```c
printf("Build version: %s\n", build_version);
fprintf(stdout, "Configuration loaded from %s\n", config_path);
```

---

## 2. When to Use `cprintf` / `cfprintf`
- **Purpose:**
  - Colour-coded raw error statements **and/or**
  - Output that should be **obfuscated before release**.
- **Behavior:** These statements **WILL** be obfuscated before release.

---

### 2.1 `cprintf` Format
- **General Format:**
```c
cprintf("%s your message %<other identifiers as needed> \n", __func__, ...);
```
- **Recommended Format (with severity level):**
```c
cprintf("%s:ERROR your message %<other identifiers as needed> \n", __func__, ...);
```

---

### 2.2 `cfprintf` Format
- **General Format:**
```c
cfprintf(stderr, "%s:ERROR your message %<other identifiers as needed> \n", __func__, ...);
```

---

## 3. Allowed Severity Levels
Instead of `ERROR`, the following can be used:
- `WARNING`
- `MISCONFIG`
- `NOTSUPPORTED`
- `UNUSUAL`
- `NOTE`
- `INFO`

> **Note:** Check the `cprintf` and `cfprintf` declarations for the complete specification of severity handling.

---

## Quick Reference Table

| Function    | Visible to Customer in Release | Color Support | Typical Usage |
|-------------|--------------------------------|---------------|---------------|
| `printf`    | ✅ Yes                         | ❌ No         | General info/logs safe for customers eyes |
| `fprintf`   | ✅ Yes                         | ❌ No         | General info/logs safe for customers eyes |
| `cprintf`   | ❌ No                          | ✅ Yes        | Obfuscated color-coded logs/errors |
| `cfprintf`  | ❌ No                          | ✅ Yes        | Obfuscated color-coded logs/errors to `stderr` |

---

## All Macros

| Macro Name | Files that use it | Description |
|-----------|-------------------|-------------|
| `BELADY_PROOF` | inode.cpp, interface.cpp, prefetch_evict.cpp, prefetch_evict.hpp | Exposes functions for cache simulator  |
| `CHECK_BITMAP_RA` | interface.cpp | check bitmap for file portions that are to be skipped while prefetching |
| `CHECK_FOR_FREAD_ERRORS` | interface.cpp | enables shim overloading of fopen/fread/fwrite/fseek |
| `DEBUG` | utils/thpool/thpool.c, utils/util.hpp, interface.cpp, prefetch_evict.cpp, interface.hpp | enables various debugging features like verbose logs printing |
| `DEBUG_OUTPUT_FILE` | utils/util.hpp, interface.cpp | Outputs all the logs to a file specified in makefile |
| `DEBUG_SEEK_POS` | inode.cpp | Checks the predicted seek position against OS ground truth |
| `DISABLE_FIRST_RDTSC` | prefetch_evict.cpp | disables the first_rdtsc which is subtracted from any subsequent rdtsc value |
| `ENABLE_EVICTION` | inode.cpp, inode.hpp, interface.cpp, prefetch_evict.cpp | enables all code needed to evict files |
| `ENABLE_LICENSE` | interface.cpp | enables all code required for license checking |
| `ENABLE_MINCORE_DEBUG` | inode.cpp, inode.hpp, interface.cpp, prefetch_evict.cpp | checks the cache state approximated by heap with OS groundtruth using mincore |
| `ENABLE_ONE_LRU` | prefetch_evict.cpp | LRU were each entry is a portion of a file. Only meant for BELADY_PROOF |
| `ENABLE_PER_INODE_BITMAP` | inode.cpp, inode.hpp, interface.cpp | enables a bitmap for each whitelisted inode |
| `ENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES` | interface.cpp | send FADV_RANDOM whenever a whitelisted file is opened |
| `ENABLE_PVT_HEAP` | inode.cpp, prefetch_evict.cpp | enables a per uinode heap where each entry is a portion of said uinode |
| `ENABLE_SYSTEM_INFO` | interface.cpp | enables getting system info eg. memory usage etc. using a background thread |
| `ENABLE_UINODE_LOCK` | interface.cpp, prefetch_evict.cpp | locks the whole uinode when updating anything. To be used for debugging only |
| `EVICTION_FREQ` | prefetch_evict.cpp | use frequency of accesses to the file as the indicator of its warmth for eviction |
| `EVICTION_LRU` | prefetch_evict.cpp | use last time of accesses to the file as the indicator of its warmth for eviction |
| `GET_ROCKSDB_OPTIONS_FROM_FILE` | interface.cpp | read rocksdb option files from env variable ROCKSDB_OPTIONS_FILE |
| `MAINTAIN_INODE` | interface.cpp | enable i_map ie. {ino,dev_id} -> struct uinode |
| `MAX_THPOOL_WORK` | utils/thpool/simple/thpool-simple.c | confused with MAX_THPOOL_WORKS |
| `MAX_THPOOL_WORKS` | utils/thpool/simple/thpool-simple.c | confused with MAX_THPOOL_WORK |
| `NOSYNC_BEFORE_RANGE_EVICT` | prefetch_evict.cpp | no sync_file_range before evicting |
| `PER_FD_DS` | interface.cpp | enables g_fd_map ie. fd to perfd_struct map |
| `PER_THREAD_DS` | interface.cpp, prefetch_evict.cpp, per_thread_ds.hpp | enables the per thread ds |
| `PRINT_READ_EVENTS` | interface.cpp, per_thread_ds.hpp | prints read events for replay trace files |
| `PRINT_WRITE_EVENTS` | interface.cpp, per_thread_ds.hpp | prints write events for replay trace files |
| `SET_AFFINITY_WORKER` | utils/thpool/thpool.c | sets CPU affinity for threads in the thread pool |
| `ENABLE_START_STOP` | interface.cpp | enable or disable the evictor thread in speedyio from an outside trigger |
| `SET_PVT_MIN_IN_GHEAP` | prefetch_evict.cpp | sets the min from pvt_heap in corresponding gheap entry |
| `SYNC_BEFORE_FULL_EVICT` | prefetch_evict.cpp | enables sync_file_range for full file before evicting it completely |
| `THPOOL_DEBUG` | utils/thpool/thpool.c | enables theadpool debug prints |
| `OBF_DBG_PRINTS` | utils/util.h | enables printing obfuscated codes instead of raw logs |
| `ENABLE_BG_INODE_CLEANER` | interface.cpp | enables bg thread that periodically cleans unused uinodes |
| `DISABLE_CONCURRENT_EVICTION` | interface.cpp | disables spawning the evictor thread. ONLY DEBUG |

---

# Hash table behavior (with current hash function) [utils/hashtable]

**Summary:** We store a **32-bit hash** per entry and index buckets with `index = hash % tablelength`, where `tablelength` is a **prime**. We keep load factor **α ≤ 0.65** (by resizing). With the Murmur-style finalizer upstream, bucket distribution is close to uniform. Collisions are inevitable in a 32-bit space; for typical sizes they barely matter.

---

## Hashing & indexing (utils/hashtable)

* `e->h` is a **32-bit** hash (Murmur finalizer upstream; extra Java-style whitening in `hash()`).
* Bucket index:

  ```c
  static inline unsigned int indexFor(unsigned int tablelength, unsigned int hashvalue) {
      return hashvalue % tablelength; // prime-sized table
  }
  ```
* Prime table sizes prevent low-bit pathologies; `%` uses **all bits**.

## Load factor & costs

* We resize at **α = entrycount / tablelength ≈ 0.65**.
* Separate chaining with uniform hashing → expected probes:

  * **Successful search:** \~`1 + α/2` ≈ **1.33**
  * **Unsuccessful search:** \~`1 + α` ≈ **1.65**
* Bucket sizes are \~Poisson(α): most buckets are 0–1 entries; long chains are rare at α=0.65.

## 32-bit hash collisions (birthday bound)

* Probability of ≥1 duplicate **hash value** among `n` distinct keys:

  $$
  P \approx 1 - e^{-n(n-1)/(2\cdot 2^{32})}
  $$
* Rules of thumb:

  * \~0.1% @ **\~2.9k** keys
  * \~1% @ **\~9.3k**
  * \~10% @ **\~30k**
  * \~50% @ **\~77k**
* Expected duplicates ≈ $n(n-1)/(2\cdot 2^{32})$.
  Example: `n=1e6` → \~**116** duplicate 32-bit hashes total.
* **Impact here:** negligible for performance; we also verify keys with `eqfn(k, e->k)` after matching `e->h`, so correctness is intact.

## Resizing strategy

* Table sizes step through a **prime ladder**; we re-bucket by `% newsize` using the stored `e->h` (no need to rehash the key payload).
* Insertion is head-insert into the bucket list (recent inserts are found first).

## Practical guidance

* If you expect order of **million** of entries: keep as-is. Bucket collisions (α) dominate; 32-bit hash duplicates are a rounding error.
* If you expect **tens of millions+** and want to squash even rare hash duplicates, widen `e->h` (and `hashf`) to **64-bit**; the 50% birthday point jumps to \~**5.1×10⁹** keys.
* `%` is a division; it’s slower than bitmasking, but pointer chasing in chains dominates anyway. Stick with primes for robustness.

## Takeaway

* With α≈0.65, primes, and a strong finalizer, this hashtable is **well-behaved** for now, ie. doesnt need to change to 64 bit key.
* 32-bit collisions don’t hurt correctness and barely hurt performance at sane scales.
