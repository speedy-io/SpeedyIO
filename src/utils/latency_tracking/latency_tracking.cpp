#include <stdio.h>
#include "latency_tracking.hpp"


// Convert difference between two timespecs to nanoseconds
int64_t timespec_diff_ns(struct timespec start, struct timespec end) {
        int64_t sec_diff  = (int64_t)(end.tv_sec - start.tv_sec);
        int64_t nsec_diff = (int64_t)(end.tv_nsec - start.tv_nsec);

        return sec_diff * 1000000000LL + nsec_diff;
}


// Bin a nanosecond duration into power-of-two microsecond bins
void bin_time_to_pow2_us(struct timespec start, struct timespec end, struct lat_tracker *tracker) {
        // Convert to microseconds (integer division, round up)
        uint64_t ns = timespec_diff_ns(start, end);
        uint64_t us = (ns + 999) / 1000;  // ceil(ns / 1e3)
        int power_index = 0;
        uint64_t bin = 0;

        if (us == 0){
                // return 1;
                // return 1; // Anything <1µs goes to 1µs bin
                goto update_bin;
        }

        bin = 1;

        // Find next power of two >= us
        while (bin < us) {
                bin <<= 1;
                power_index += 1;
        }

        if(power_index >= NR_POW2_LATENCY_BINS){
                printf("%s:ERROR power_index:%d and NR_BINS:%d XXXXXXXXXXXXXXXXXXXX\n", __func__, power_index, NR_POW2_LATENCY_BINS);
                goto exit;
        }

update_bin:
        tracker->latencies_bin_ctr[power_index].fetch_add(1, std::memory_order_relaxed);

exit:
        return;
}

/*tracks any number in power of 2 bins*/
void bin_to_pow2(int nr, struct lat_tracker *tracker) {
        uint64_t bin = 0;
        int power_index = 0;
        
        if (nr == 0){
                goto update_bin;
        }

        bin = 1;

        // Find next power of two >= us
        while (bin < nr) {
                bin <<= 1;
                power_index += 1;
        }

update_bin:
        tracker->latencies_bin_ctr[power_index].fetch_add(1, std::memory_order_relaxed);

exit:
        return;
}


void print_latencies(const char *message, struct lat_tracker *tracker){
        printf("\nXXXXXXX Latencies: %s XXXXXXXXX\n", message);

        uint64_t val;
        for(int i=0; i < NR_POW2_LATENCY_BINS; i++){
                val = tracker->latencies_bin_ctr[i].load(std::memory_order_relaxed);
                printf("%ld -> %ld : %lu\n", (i == 0) ? 0 : (1ULL << (i - 1)), 1<<i, val);
        }

        printf("XXXXXXX DONE Latencies: %s XXXXXXXXX\n", message);
}