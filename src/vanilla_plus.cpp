#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <dlfcn.h>

#include <iostream>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <map>
#include <deque>
#include <unordered_map>
#include <string>
#include <iterator>
#include <atomic>

#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>

#include "utils/shim/shim.hpp"
#include "utils/whitelist/whitelist.hpp"
#include "utils/latency_tracking/latency_tracking.hpp"

/**
 * This program only does two things:
 * 1. at open: FADV_RANDOM
 * 2. Ignore any fadvise or readahead
*/


struct lat_tracker readsyscalls_latency;
struct lat_tracker readsize;
struct lat_tracker rs_lat_0_4;
struct lat_tracker rs_lat_4_8;
struct lat_tracker rs_lat_8_256;
struct lat_tracker rs_lat_256_4096;
struct lat_tracker rs_lat_4096_8192;
struct lat_tracker rs_lat_8192_32768;
struct lat_tracker rs_lat_32768_65536;




static void construct() __attribute__((constructor));
static void destruct() __attribute__((destructor));

void construct(){
    printf("APP Starting! \n");
}


void destruct(){
    printf("APP Exiting! \n");

#ifdef TRACK_READ_LATENCY
    print_latencies("read_size bytes", &readsize);
    print_latencies("read_syscalls", &readsyscalls_latency);
    print_latencies("read_syscalls 0 -> 4 bytes", &rs_lat_0_4);
    print_latencies("read_syscalls 4 -> 8 bytes", &rs_lat_4_8);
    print_latencies("read_syscalls 8 -> 256 bytes", &rs_lat_8_256);
    print_latencies("read_syscalls 256 -> 4096 bytes", &rs_lat_256_4096);
    print_latencies("read_syscalls 4096 -> 8192 bytes", &rs_lat_4096_8192);
    print_latencies("read_syscalls 8192 -> 32768 bytes", &rs_lat_8192_32768);
    print_latencies("read_syscalls 32768 -> 65536 bytes", &rs_lat_32768_65536);
#endif //TRACK_READ_LATENCY

}

void handle_open(int fd, const char *filename){

#ifdef DISABLE_FADV_RANDOM
    goto exit_handle_open;
#endif //DISABLE_FADV_RANDOM


#ifdef ENABLE_FADV_FOR_ALL
//just do fadv
#elif defined(SKIP_FADV_FOR_DATADB)
    if(to_skip_fadv_random(filename)){
        goto exit_handle_open;
    }
    // printf("%s:INFO letting file:%s FADV_RANDOM\n", __func__, filename);
#else //only fadv on whitelisted file
    if(!is_whitelisted(filename)){
        goto exit_handle_open;
    }
#endif

// #ifndef ENABLE_FADV_FOR_ALL
//     if(!is_whitelisted(filename)){
//         goto exit_handle_open;
//     }
// #elif defined(SKIP_FADV_FOR_DATADB)
//     if(to_skip_fadv_random(filename)){
//         goto exit_handle_open;
//     }
// #endif //ENABLE_FADV_FOR_ALL


    if(real_posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM) != 0){
        fprintf(stderr, "%s:ERROR posix_fadvise failed for fd:%d, file:%s\n", __func__, fd, filename);
    }

exit_handle_open:
    return;
}

int openat(int dirfd, const char *pathname, int flags, ...){

    int fd;

    if(flags & O_CREAT){
            va_list valist;
            va_start(valist, flags);
            mode_t mode = va_arg(valist, mode_t);
            va_end(valist);
            fd = real_openat(dirfd, pathname, flags, mode);
    }else{
            fd = real_openat(dirfd, pathname, flags, 0);
    }

    if(fd < 3 || (flags & O_DIRECTORY)){
        goto exit_openat;
    }

    handle_open(fd, pathname);

exit_openat:
    return fd;
}

int open64(const char *pathname, int flags, ...){

    int fd;

    if(flags & O_CREAT){
            va_list valist;
            va_start(valist, flags);
            mode_t mode = va_arg(valist, mode_t);
            va_end(valist);
            fd = real_open(pathname, flags, mode);
    }
    else{
            fd = real_open(pathname, flags, 0);
    }

    if(fd < 3 || (flags & O_DIRECTORY)){
            goto exit_open64;
    }

    // if(real_posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM) != 0){
    //     fprintf(stderr, "%s:ERROR posix_fadvise failed for fd:%d, file:%s\n", __func__, fd, pathname);
    // }
    handle_open(fd, pathname);

exit_open64:
    return fd;
}

int open(const char *pathname, int flags, ...){

    int fd;

    if(flags & O_CREAT){
            va_list valist;
            va_start(valist, flags);
            mode_t mode = va_arg(valist, mode_t);
            va_end(valist);
            fd = real_open(pathname, flags, mode);
    }
    else{
            fd = real_open(pathname, flags, 0);
    }

    if(fd < 3 || (flags & O_DIRECTORY)){
        goto exit_open;
    }

    // if(real_posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM) != 0){
    //     fprintf(stderr, "%s:ERROR posix_fadvise failed for fd:%d, file:%s\n", __func__, fd, pathname);
    // }
    handle_open(fd, pathname);

exit_open:
    return fd;
}

int creat(const char *pathname, mode_t mode){
    int fd;

    fd = real_creat(pathname, mode);

    if(fd < 3){
        goto exit_creat;
    }

    // if(real_posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM) != 0){
    //     fprintf(stderr, "%s:ERROR posix_fadvise failed for fd:%d, file:%s\n", __func__, fd, pathname);
    // }

    handle_open(fd, pathname);

exit_creat:
    return fd;
}


#ifdef TRACK_READ_LATENCY

void bin_read_latency_sizewise(size_t size, struct timespec start, struct timespec end){

    if(size <= 4){
        bin_time_to_pow2_us(start, end, &rs_lat_0_4);
    }
    else if(size <= 8){
        bin_time_to_pow2_us(start, end, &rs_lat_4_8);
    }
    else if(size <= 256){
        bin_time_to_pow2_us(start, end, &rs_lat_8_256);
    }
    else if(size <= 4096){
        bin_time_to_pow2_us(start, end, &rs_lat_256_4096);
    }
    else if(size <= 8192){
        bin_time_to_pow2_us(start, end, &rs_lat_4096_8192);
    }
    else if(size <= 32768){
        bin_time_to_pow2_us(start, end, &rs_lat_8192_32768);
    }
    else if(size <= 65536){
        bin_time_to_pow2_us(start, end, &rs_lat_32768_65536);
    }
}


ssize_t pread64(int fd, void *data, size_t size, off_t offset){
        ssize_t amount_read;
        struct timespec start, end;
        bool track_read = false;

        if(fd >= 3) track_read = true;

serve_req:
        if(track_read){
            bin_to_pow2(size, &readsize);
            clock_gettime(CLOCK_MONOTONIC, &start);
        }
        amount_read = real_pread64(fd, data, size, offset);
        
        if(track_read){
            clock_gettime(CLOCK_MONOTONIC, &end);
            bin_time_to_pow2_us(start, end, &readsyscalls_latency);
            bin_read_latency_sizewise(size, start, end);
        }

        return amount_read;
}


ssize_t pread(int fd, void *data, size_t size, off_t offset){
        ssize_t amount_read;
        struct timespec start, end;
        bool track_read = false;

        if(fd >= 3) track_read = true;

serve_req:
        if(track_read){
            bin_to_pow2(size, &readsize);
            clock_gettime(CLOCK_MONOTONIC, &start);
        }
        
        amount_read = real_pread(fd, data, size, offset);
        
        if(track_read){
            clock_gettime(CLOCK_MONOTONIC, &end);
            bin_time_to_pow2_us(start, end, &readsyscalls_latency);
            bin_read_latency_sizewise(size, start, end);
        }

exit_pread:
        return amount_read;
}


ssize_t read(int fd, void *data, size_t size){
        ssize_t amount_read;
        struct timespec start, end;
        bool track_read = false;

        if(fd >= 3) track_read = true;

serve_req:
        if(track_read){
            bin_to_pow2(size, &readsize);
            clock_gettime(CLOCK_MONOTONIC, &start);
        }

        amount_read = real_read(fd, data, size);
        
        if(track_read){
            clock_gettime(CLOCK_MONOTONIC, &end);
            bin_time_to_pow2_us(start, end, &readsyscalls_latency);
            bin_read_latency_sizewise(size, start, end);
        }

        return amount_read;
}

#endif //TRACK_READ_LATENCY


#if 0

ssize_t readahead(int fd, off_t offset, size_t count){
    ssize_t ret = 0;

    goto exit_readahead;

real_ra:
    ret = real_readahead(fd, offset, count);

exit_readahead:
    return ret;
}

int posix_fadvise(int fd, off_t offset, off_t len, int advice){
    int ret = 0;

    goto skip_posix_fadvise;

real_fadvise:
    ret = real_posix_fadvise(fd, offset, len, advice);

skip_posix_fadvise:
    return ret;
}

int posix_fadvise64(int fd, off_t offset, off_t len, int advice){
    int ret = 0;

    goto skip_posix_fadvise64;

real_fadvise64:
    ret = real_posix_fadvise64(fd, offset, len, advice);

skip_posix_fadvise64:
    return ret;
}

int fadvise64(int fd, off_t offset, off_t len, int advice){
    int ret = 0;

    goto skip_fadvise64;

real_fadvise64:
    ret = real_fadvise64(fd, offset, len, advice);

skip_fadvise64:
    return ret;
}

#endif
