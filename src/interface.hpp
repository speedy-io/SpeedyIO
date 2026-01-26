#ifndef _INTERFACE_HPP
#define _INTERFACE_HPP

#define _FILE_OFFSET_BITS 64

#include "utils/shim/shim.hpp"
#include "utils/whitelist/whitelist.hpp"
#include "utils/util.hpp"
#include "utils/thpool/simple/thpool-simple.h"
#include "utils/system_info/system_info.hpp"
#include "utils/events_logger/events_logger.hpp"

#include "inode.hpp"
#include "prefetch_evict.hpp"
#include "per_thread_ds.hpp"
#include "utils/start_stop/start_stop_speedyio.hpp"
#include "utils/parse_config/get_config.hpp"
#include "utils/filename_helper/filename_helper.hpp"

// #ifdef DEBUG
// #include <x86intrin.h>
// #endif

struct thread_args{
        int fd; //opened file fd
        struct perfd_struct *pfd;
        off_t offset; //where to start ?
        off_t prefetch_size; //size of each prefetch syscall

        /*Position of read and bytes read by user*/
        size_t read_bytes; //number of bytes done in read syscall
};


struct file_desc{
        int fd;
        int flags;
        const char *filename;
};


long long get_time_in_ns() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

int nanosleep_ns(long nanoseconds) {
        struct timespec req, rem;

        //printf("%s called for %ld nanoseconds\n", __func__, nanoseconds);
        // Convert nanoseconds to seconds and nanoseconds
        req.tv_sec = nanoseconds / 1000000000;
        req.tv_nsec = nanoseconds % 1000000000;

        // Call nanosleep and handle any interruptions
        while (nanosleep(&req, &rem) == -1 && errno == EINTR) {
                req = rem; // Continue sleeping for the remaining time
        }

        return 0;
}

int is_rdtsc_available() {
        unsigned int eax, ebx, ecx, edx;

        /**
         * TODO: Temporary return true
         * Fix this change made while testing on ARM
         */
        return true;

        // Call CPUID with EAX=1 to get feature flags in EDX
        __asm__ __volatile__(
                        "cpuid"
                        : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
                        : "a"(1)  // EAX = 1 for feature info
                        );

        // Bit 4 of EDX indicates if RDTSC is available
        return (edx & (1 << 4)) != 0;
}


/*
 * Checks if open flags are as expected.
 * some flags are not insane but arent supported yet (O_TRUNC)
 */
bool check_open_flag_sanity(struct file_desc file){
        bool ret = true;

        if(file.flags & O_DIRECT){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED whitelisted file:%s opened with O_DIRECT\n", "SPEEDYIO_NOTSUPPORTEDCO_0020 %s\n", file.filename);
                ret = false;
        }

        /**
         * O_CLOEXEC tells the OS to close this fd if an exec is called from the opening
         * process. This flag can be at the time of open or using fcntl later on.
         * This means the fd can be reused for some other file potentially confusing
         * add_any_fd_to_perfd_struct(). (the same fd pointing to some other inode)
         *
         * One problem with O_CLOEXEC is that it gets triggered when exec is called.
         * exec family functions replace the address space, reallocating all the data structures
         * on all shared libraries including this one (LD_PRELOAD). We haven't fixed this yet.
         * (Look at the explanation above the definition of g_fd_map)
         *
         * Note: add_any_fd_to_perfd_struct() handles cases where fd is reused due to CLOEXEC
         * look at the notes at its definition; but it is difficult to test because of the above
         * problem.
         *
         * RocksDB opens all its sst files using O_CLOEXEC but doesnt call exec beyond the start
         * Cassandra doesnt use CLOEXEC on any of its data files.
         * XXX: So we can skip this for now. Later we will need to fix and test it.
         *
        if(file.flags & O_CLOEXEC){
                // SPEEDYIO_FPRINTF("%s:ERROR whitelisted file:%s opened with O_CLOEXEC\n", "SPEEDYIO_ERRCO_0088 %s\n", file.filename);
                ret = false;
        }
        */

exit_check_open_flag_sanity:
        return ret;
}


/**
 * print_mem_usage_all - prints all /proc/self/statm fields in pages and MiB
 * Tells how much memory(anonymous) is being used by the application + shared_lib
 *
 * /proc/self/statm fields:
 *   size     - total program size (pages)
 *   resident - resident set size (pages)
 *   share    - shared pages (pages)
 *   text     - text (code) pages
 *   lib      - library pages (always 0 on modern Linux)
 *   data     - data + stack pages
 *   dt       - dirty pages (deprecated, not read here)
 */
void print_mem_usage_all(void) {
        long page_sz = sysconf(_SC_PAGESIZE);
        if (page_sz <= 0) {
                perror("sysconf(_SC_PAGESIZE)");
                return;
        }

        FILE *f = fopen("/proc/self/statm", "r");
        if (!f) {
                perror("fopen(/proc/self/statm)");
                return;
        }

        size_t size = 0, resident = 0, share = 0, text = 0, lib = 0, data = 0;
        if (fscanf(f, "%zu %zu %zu %zu %zu %zu",
                        &size, &resident, &share, &text, &lib, &data) != 6) {
                fprintf(stderr, "Failed to parse /proc/self/statm\n");
                fclose(f);
                return;
        }
        fclose(f);

        printf("MEM:\n");
        printf("  size     = %zu pages (%.2f KiB)\n", size,
                (size * (double)page_sz) / (1024.0));
        printf("  resident = %zu pages (%.2f KiB)\n", resident,
                (resident * (double)page_sz) / (1024.0));
        printf("  share    = %zu pages (%.2f KiB)\n", share,
                (share * (double)page_sz) / (1024.0));
        printf("  text     = %zu pages (%.2f KiB)\n", text,
                (text * (double)page_sz) / (1024.0));
        printf("  lib      = %zu pages (%.2f KiB)\n", lib,
                (lib * (double)page_sz) / (1024.0));
        printf("  data     = %zu pages (%.2f KiB)\n", data,
                (data * (double)page_sz) / (1024.0));

        // malloc_info(0, stdout);
}


/**
 * Compare filename (absolute/relative string) with (dirfd, pathname).
 *
 * Returns:
 *   1  -> strings resolve to the same canonical absolute path
 *   0  -> different
 *  -1  -> error (errno set)
 */
int same_pathnames(const char *filename, int dirfd, const char *pathname)
{
     char abs1[PATH_MAX];
     char abs2[PATH_MAX];

     // Canonicalize filename
     if (realpath(filename, abs1) == NULL) {
         return -1;
     }

     // Build canonical absolute for (dirfd, pathname)
     if (pathname[0] == '/') {
         // Already absolute
         if (realpath(pathname, abs2) == NULL) {
             return -1;
         }
     } else if (dirfd == AT_FDCWD) {
         // Relative to current working directory
         if (realpath(pathname, abs2) == NULL) {
             return -1;
         }
     } else {
         // Resolve relative to dirfd using openat + /proc/self/fd
         int fd = openat(dirfd, pathname, O_PATH | O_CLOEXEC);
         if (fd < 0) {
             return -1;
         }

         char procpath[64];
         snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", fd);

         ssize_t len = readlink(procpath, abs2, sizeof(abs2) - 1);
         if (len < 0) {
             int saved = errno;
             close(fd);
             errno = saved;
             return -1;
         }
         abs2[len] = '\0';
         close(fd);

         // If you want a fully canonicalized path (resolving "..", symlinks, etc.),
         // run abs2 through realpath() as well:
         char canon[PATH_MAX];
         if (realpath(abs2, canon) == NULL) {
             return -1;
         }
         strncpy(abs2, canon, sizeof(abs2));
         abs2[sizeof(abs2)-1] = '\0';
     }

     return strcmp(abs1, abs2) == 0 ? 1 : 0;
}

/**
 * For high precision performance debugging on Intel CPUs, use the following
 *
 * unsigned long long start, end;
 * start = __rdtsc();
 * SOME CODE HERE
 * end = __rdtsc();
 *
 * double elapsed_time_ns = (end - start) / (CPU_FREQ_GHZ * 1e9);
 */

#endif
