#ifndef _LATENCY_TRACKING_HPP
#define _LATENCY_TRACKING_HPP

#include <time.h>

#include <cstdint>
#include <atomic>

#define NR_POW2_LATENCY_BINS 32

// int64_t timespec_diff_ns(struct timespec start, struct timespec end);
void bin_to_pow2(int nr, struct lat_tracker *tracker);
void bin_time_to_pow2_us(struct timespec start, struct timespec end, struct lat_tracker *tracker);
void print_latencies(const char *message, struct lat_tracker *tracker);


struct lat_tracker{
    std::atomic<std::uint64_t> latencies_bin_ctr[NR_POW2_LATENCY_BINS];

    lat_tracker(){
        for (int i = 0; i < NR_POW2_LATENCY_BINS; i++){
                latencies_bin_ctr[i].store(0, std::memory_order_relaxed);
        }
    }
};


#endif //_LATENCY_TRACKING_HPP