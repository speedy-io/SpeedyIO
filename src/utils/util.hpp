#ifndef _UTIL_HPP
#define _UTIL_HPP

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdarg.h>
#include <unistd.h>

#include <cstdarg>

#include <sys/syscall.h>

#include "debug_utils/debug_utils.hpp"  // Include to access write_log_syscall
#include "mock_ds.hpp"

#define PAGESIZE 4096L //Page size
#define PAGE_SHIFT 12
#define gettid() syscall(SYS_gettid)

/*Used by get_config*/
#define MAX_DEVICES   8
#define PATH_MAX      4096

#define KILLME()  \
    do{             \
        kill(0, SIGTERM); \
        exit(EXIT_FAILURE); \
    }while(0)

// #define KILLME() ((void)0)

#define KB 1024L
#define MB 1024L * KB
#define GB 1024L * MB

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

#define MAX(a, b) ((a & ~((a - b) >> 31)) | (b & ((a - b) >> 31)))

// Stringify the macro value for printing
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

#define POSIX_FADV_RANDOM 1
#define POSIX_FADV_SEQUENTIAL 2
#define POSIX_FADV_WILLNEED 3

#define ASSERT(condition) \
    typedef char compile_time_assert_failed[(condition) ? 1 : -1]

/*Returns the number of pages for the nr of bytes. Returns 1 if bytes < PAGESIZE*/
#define BYTES_TO_PG(x) ((x >> PAGE_SHIFT) + !!(x & ((1 << PAGE_SHIFT) - 1)))

/*
* Returns the portion number for the given file offset and portion SHIFT.
* this returns portions nr in 0 based indexing.
*/
//#define PORTION_NR_FROM_OFFSET(x, y) (((x) + (1 << y) - 1) >> y)  // This was buggy
#define PORTION_NR_FROM_OFFSET(x, y) ((x) >> (y))

/*Returns the page number for the given offset. Starts from 1 not 0*/
#define PG_NR_FROM_OFFSET(x) PORTION_NR_FROM_OFFSET(x, PAGE_SHIFT)

static inline void cfprintf(FILE *stream, const char *format, ...) {
    va_list args;
    char buffer[4096];

    const char *red       = "\033[31m";
    const char *yellow    = "\033[33m";
    const char *magenta   = "\033[35m";
    const char *blue      = "\033[34m";
    const char *white     = "\033[37m";
    const char *cyan      = "\033[36m";
    const char *green     = "\033[32m";
    const char *brightblue = "\033[94m";
    const char *reset     = "\033[0m";

    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    pid_t pid = getpid();
    pid_t tid = gettid();

    int use_color = isatty(fileno(stream));

    char *colon = strchr(buffer, ':');
    if (colon && use_color) {
        *colon = '\0';

        // Print prefix (before colon) with PID:TID in blue
        fprintf(stream, "%s[%d:%d] %s%s", blue, pid, tid, buffer, reset);

        *colon = ':';
        const char *rest = colon;

        const char *color = white;
        if (strstr(rest, "ERROR")) {
            color = red;
        } else if (strstr(rest, "MISCONFIG")) {
            color = magenta;
        } else if (strstr(rest, "WARNING")) {
            color = yellow;
        } else if (strstr(rest, "NOTSUPPORTED")) {
            color = cyan;
        } else if (strstr(rest, "UNUSUAL")) {
            color = green;
        } else if (strstr(rest, "NOTE")) {
            color = brightblue;
        }else if (strstr(rest, "INFO")) {
            color = brightblue;
        }

        fprintf(stream, "%s%s%s", color, rest, reset);
    } else {
        if (use_color) {
            fprintf(stream, "%s[%d:%d] %s%s", white, pid, tid, buffer, reset);
        } else {
            fprintf(stream, "[%d:%d] %s", pid, tid, buffer);
        }
    }
}

static inline void cprintf(const char *format, ...) {
    va_list args;
    char buffer[4096];  // Adjust size as needed

    const char *red        = "\033[31m";
    const char *yellow     = "\033[33m";
    const char *magenta    = "\033[35m";
    const char *blue       = "\033[34m";
    const char *white      = "\033[37m";
    const char *cyan       = "\033[36m";
    const char *green      = "\033[32m";
    const char *brightblue = "\033[94m";
    const char *reset      = "\033[0m";

    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    pid_t pid = getpid();
    pid_t tid = gettid();

    char *colon_pos = strchr(buffer, ':');

    if (colon_pos) {
        *colon_pos = '\0';  // temporarily split the string

        // Print prefix (before colon) in blue with PID and TID
        printf("%s[%d:%d] %s%s", blue, pid, tid, buffer, reset);

        *colon_pos = ':';  // restore colon
        const char *rest = colon_pos;

        const char *color = white;
        if (strstr(rest, "ERROR")) {
            color = red;
        } else if (strstr(rest, "MISCONFIG")) {
            color = magenta;
        } else if (strstr(rest, "WARNING")) {
            color = yellow;
        } else if (strstr(rest, "NOTSUPPORTED")) {
            color = cyan;
        } else if (strstr(rest, "UNUSUAL")) {
            color = green;
        } else if (strstr(rest, "NOTE")) {
            color = brightblue;
        }else if (strstr(rest, "INFO")) {
            color = brightblue;
        }

        printf("%s%s%s", color, rest, reset);
    } else {
        // No colon: print entire line with PID:TID
        printf("%s[%d:%d] %s%s", white, pid, tid, buffer, reset);
    }
}

#ifdef DEBUG

#ifdef DEBUG_OUTPUT_FILE

// Helper macro for conditional warning printing to stderr
#ifndef SUPPRESS_LINES_BEFORE_DEBUG_FILE_PTR_INITIALIZATION
#define DEBUG_WARNING_TO_STDERR(...) \
    write_log_syscall(STDERR_FILENO, "[debug_printf warning: debug_log_file pointer not yet initialized, printing to stderr] "); \
    write_log_syscall(STDERR_FILENO, __VA_ARGS__)
#else
#define DEBUG_WARNING_TO_STDERR(...) /* Do nothing */
#endif //SUPPRESS_LINES_BEFORE_DEBUG_FILE_PTR_INITIALIZATION

#define debug_printf(...) do { \
    if (debug_log_file) { \
        write_log_syscall(fileno(debug_log_file), __VA_ARGS__); \
    } else { \
        DEBUG_WARNING_TO_STDERR(__VA_ARGS__); \
    } \
} while (0)

#else //DEBUG_OUTPUT_FILE
//#define debug_printf(...) printf(__VA_ARGS__)
#define debug_printf(...) cprintf(__VA_ARGS__)
#endif //DEBUG_OUTPUT_FILE

//#define debug_fprintf(...) fprintf(__VA_ARGS__)
#define debug_fprintf(...) cfprintf(__VA_ARGS__)
#else //DEBUG

#define debug_printf(...) do { } while (0)
#define debug_fprintf(...) do { } while (0)

#endif //DEBUG

#ifndef OBF_DBG_PRINTS
    #define SPEEDYIO_FPRINTF(raw_fmt, obf_fmt, ...) \
        cfprintf(stderr, raw_fmt, __func__, ##__VA_ARGS__)

    #define SPEEDYIO_PRINTF(raw_fmt, obf_fmt, ...) \
       cprintf(raw_fmt, __func__, ##__VA_ARGS__)
#else
    #define SPEEDYIO_FPRINTF(raw_fmt, obf_fmt, ...) \
        fprintf(stderr, obf_fmt, ##__VA_ARGS__)

    #define SPEEDYIO_PRINTF(raw_fmt, obf_fmt, ...) \
        printf(obf_fmt, ##__VA_ARGS__)
#endif //OBF_DBG_PRINTS

/*This is used for debugging only*/
#define CPU_FREQ_GHZ 2.4f
#define CPU_FREQ (CPU_FREQ_GHZ * 1000000000.0)

/*number of chars an absolute path can have*/
#define MAX_ABS_PATH_LEN 4096

/**
 * (2^BITMAP_SHIFT) bytes file can be supported
 * 30 -> Every file can be 1 GB
 *
 * This is used to derive:
 * 1. MAX_FILE_SIZE_BYTES
 * 2. NR_BITMAP_BITS
 * 3. NR_PVT_HEAP_ELEMENTS [used for size of private heaps]
 *
 * XXX: Increasing this beyond 36 results in an unknown runtime error
 * FIXIT: when using ENABLE_PER_INODE_BITMAP
 */
#ifndef BITMAP_SHIFT
#define BITMAP_SHIFT 40UL
#endif //BITMAP_SHIFT

/**
 * Number of elements to allocate in
 * uinode->file_heap_node_ids at the beginning
 * Resizing will happen on the fly as required.
 */
#ifndef MIN_NR_FILE_HEAP_NODES
#define MIN_NR_FILE_HEAP_NODES 1
#endif //MIN_FILE_HEAP_NODES

/**
 * Maximum supported filesize because of BITMAP_SHIFT
 * ONLY considered if ENABLE_PER_INODE_BITMAP is enabled
 */
#define MAX_FILE_SIZE_BYTES (1UL << BITMAP_SHIFT)


/*
 * Number of bits in per file bitmap
 * This will not be altered at compile time
 */
#define NR_BITMAP_BITS (1UL << (BITMAP_SHIFT - PAGE_SHIFT))

/*
 * Used by the user inode struct
 * defines the maximum numbers of FDs for an inode
 */
#ifndef MAX_FD_PER_INODE
#define MAX_FD_PER_INODE 100
#endif


/*
 * Max number of files to handle in the inode map
 */
#ifndef MAX_IMAP_FILES
#define MAX_IMAP_FILES 50000
#endif

/* Heap macros*/

/*
 * Floats that is used by heap keys can represent
 * -2^24 to 2^24 integers exactly.
 * To make sure a given node is at the bottom of a min heap
 * we will add ADD_TO_KEY_REDUCE_PRIORITY to the key of the node
 * This will be done at the time of eviction. When the node is
 * read again, the key will be reduced by this value to get the original
 * frequency. Will be used by EVICTION_FREQ
 */
#define ADD_TO_KEY_REDUCE_PRIORITY 10000000


/*
 * For each G_HEAP_FREQ reads, updates to the gheap happen once
 * this is to reduce the lock contentions.
 */
#ifndef G_HEAP_FREQ
#define G_HEAP_FREQ 10
#endif

/*
 * Pvt Fib heap macros
 */

/*
 * PVT_HEAP_PG_ORDER defines the number of pages to have as
 * a single entry in the PVT heap.
 * nr of 4k pages per portion = pow(2, PG_ORDER)
 * eg. PG_ORDER = 1 means each entry will be 2 4K pages
 * PG_ORDER = 2 means each entry will be 4 4K pages
 * Right now 9 is chosen ie. portion size is 2MB.
 */
#ifndef PVT_HEAP_PG_ORDER
#define PVT_HEAP_PG_ORDER 9
#endif

/*PVT_HEAP_PG_SHIFT is to get correct portion index from byte index*/
#define PVT_HEAP_PG_SHIFT (PAGE_SHIFT + PVT_HEAP_PG_ORDER)
#define NR_PVT_HEAP_ELEMENTS (1UL<<(BITMAP_SHIFT-PVT_HEAP_PG_SHIFT))
#define COMPOUND_HEAP_PG_SIZE (1 << PVT_HEAP_PG_SHIFT)

/*EVICTION each syscall size*/
#define FADV_CHUNK_KB (128 * 1024)

/*
 * This is used by keep_evicting_from_this_file
 * to weigh in if evicting the next file is better than
 * to keep evicting from current file.
 * The larger this value, the more it encourages
 * eviction of the current victim file.
 * keep the value between [1, 2).
 */
#ifndef EVICTION_MULTIPLIER_THETA
#define EVICTION_MULTIPLIER_THETA 1
#endif

/*
 * Minimum number of files in the gheap to
 * consider starting eviction.
 */
#ifndef MIN_FILES_REQD_TO_EVICT
#define MIN_FILES_REQD_TO_EVICT 1
#endif


//System info macros

/*
 * Too large a number and the eviction
 * will not have much effect.
 * too small and the OS rejects eviction of file pages
 * that are being evicted concurrent to being read
 * TODO: Find what number is good enough
 */
#ifndef SYSTEM_UTIL_SLEEP_MS
//#define SYSTEM_UTIL_SLEEP_MS 2
#define SYSTEM_UTIL_SLEEP_MS 1
#endif


/**
 * This is the frequency with which the evictor
 * thread will sleep
 */
#ifndef EVICTOR_SLEEP_FREQ
#define EVICTOR_SLEEP_FREQ 25
#endif

/*
 * Used to factor in how long it has been
 * since last file access.
 * TIME_DECAY value should be below 1.
 */
#ifndef TIME_DECAY
#define TIME_DECAY 0.00008f
#endif

/*
 * Adds weight to the fact that a given
 * file has been evicted before and then re-accessed
 * The higher the GAMMA, the more its weight
 * Keep this number an int
 */
#ifndef EVICTION_GAMMA
#define EVICTION_GAMMA 5
#endif


/*
 * Amount of memory in KB in addition to
 * getMinMemoryRequiredKB to start eviction
 */
#ifndef EVICTION_LOW_MEM_WATERMARK
#define EVICTION_LOW_MEM_WATERMARK 512*1024
#endif


/**
 * time interval between start stop trigger checks in sec
 */
#ifndef START_STOP_SLEEP
#define START_STOP_SLEEP 5
#endif


/**
 * sleep time (sec) for bg_inode_cleaner
 * currently set to 15 min
 */
#ifndef BG_CLEANUP_SLEEP
#define BG_CLEANUP_SLEEP 900
#endif

/**
 * nr of unlinks before cleaning up i_map
 */
#ifndef CLEANUP_AFTER_NR_UNLINKS
#define CLEANUP_AFTER_NR_UNLINKS 100
#endif


/**
 * ENV variable to check for speedyio_config.cfg file
 */
#ifndef CFG_FILE_ENV_VAR
#define CFG_FILE_ENV_VAR "SPEEDYIO_CFG_ENV"
#endif


/**
 * Maximum times we want to retry a lock before
 * giving up.
 */
#ifndef MAX_LOCK_RETRIES
#define MAX_LOCK_RETRIES 100
#endif


/*TO BE REMOVED. ONLY FOR START STOP TESTING*/
#ifndef START_STOP_TRIGGER_FILE
#define START_STOP_TRIGGER_FILE "/path/to/trigger"
#endif

#endif
