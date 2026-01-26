#include <stdio.h>
#include <limits.h>
#include <time.h>
#include <stdbool.h>
#include <emmintrin.h>  // Include SSE2 intrinsics

#include <sys/time.h>

#include "bitmap.h"

//#define BITS_PER_LONG (sizeof(unsigned long) * CHAR_BIT)
#define BITS_PER_LONG 64
#define LONG_SHIFT 6

long long get_time_in_ns() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

bool is_bitrange_set(unsigned long *bitmap, size_t start_bit, size_t num_bits) {
        size_t start_index = start_bit / BITS_PER_LONG;
        size_t start_offset = start_bit % BITS_PER_LONG;
        size_t end_bit = start_bit + num_bits - 1;
        size_t end_index = end_bit / BITS_PER_LONG;
        size_t end_offset = end_bit % BITS_PER_LONG;

        if (num_bits == 0) {
                return true;  // No bits to check, trivially true
        }

        // Adjust for the fact that the MSB is at the lowest index
        start_offset = BITS_PER_LONG - 1 - start_offset;
        end_offset = BITS_PER_LONG - 1 - end_offset;

        // Check the first ulong
        unsigned long first_mask = (1UL << (start_offset + 1)) - 1;
        if (start_index == end_index) {
                // All bits are within the same ulong
                unsigned long last_mask = (1UL << end_offset) - 1;
                first_mask &= ~last_mask;
                if ((bitmap[start_index] & first_mask) != first_mask) {
                        return false;
                }
        } else {
                if ((bitmap[start_index] & first_mask) != first_mask) {
                        return false;
                }

                // Check the middle ulongs
                for (size_t i = start_index + 1; i < end_index; i++) {
                        if (bitmap[i] != ~0UL) {
                                return false;
                        }
                }

                // Check the last ulong
                unsigned long last_mask = (1UL << end_offset) - 1;
                if ((bitmap[end_index] & ~last_mask) != ~last_mask) {
                        return false;
                }
        }

        return true;
}


bool BitArrayIsSet_SSE(const bit_array_t *bitmap, size_t start_bit, size_t num_bits) {
    const size_t end_bit = start_bit + num_bits;

    // Handle initial misalignment
    size_t misaligned_bits = start_bit % 64;
    if (misaligned_bits > 0) {
        size_t first_chunk_bits = 64 - misaligned_bits;
        size_t first_chunk_mask = (1UL << first_chunk_bits) - 1;
        size_t first_index = start_bit / 64;
        unsigned long first_chunk = bitmap->array[first_index] >> misaligned_bits;

        if ((first_chunk & first_chunk_mask) != first_chunk_mask) {
            return false;
        }

        start_bit += first_chunk_bits;
    }

    // Aligned to 64 bits, process 128 bits at a time using SSE2
    for (size_t i = start_bit; i + 128 <= end_bit; i += 128) {
        size_t index = i / 64;

        // Load 128 bits from the bitmap (two 64-bit unsigned longs)
        __m128i chunk1 = _mm_loadu_si128((__m128i *)&bitmap->array[index]);

        // Create a mask of all 1s (full range set)
        __m128i mask = _mm_set1_epi64x(-1);

        // Compare the chunks with the mask
        __m128i and_result = _mm_and_si128(chunk1, mask);

        // Check if the result matches the original chunk (all bits are set)
        if (_mm_movemask_epi8(_mm_cmpeq_epi8(chunk1, and_result)) != 0xFFFF) {
            return false;
        }
    }

    // Handle remaining bits
    if (start_bit < end_bit) {
        size_t last_index = start_bit / 64;
        unsigned long last_chunk = bitmap->array[last_index];

        size_t remaining_bits = end_bit - start_bit;
        unsigned long last_chunk_mask = (1UL << remaining_bits) - 1;

        if ((last_chunk & last_chunk_mask) != last_chunk_mask) {
            return false;
        }
    }

    return true;
}

/*
bool BitArrayIsSet_SSE(const bit_array_t *bitmap, size_t start_bit, size_t num_bits) {
    const size_t end_bit = start_bit + num_bits;
    
    // Handle initial misalignment
    size_t misaligned_bits = start_bit % 64;
    if (misaligned_bits > 0) {
        size_t first_chunk_bits = 64 - misaligned_bits;
        size_t first_chunk_mask = (1UL << first_chunk_bits) - 1;
        size_t first_index = start_bit / 64;
        unsigned long first_chunk = bitmap->array[first_index] >> misaligned_bits;

        if ((first_chunk & first_chunk_mask) != first_chunk_mask) {
            return false;
        }

        start_bit += first_chunk_bits;
    }

    // Aligned to 64 bits, process 128 bits at a time using SSE2
    for (size_t i = start_bit; i + 128 <= end_bit; i += 128) {
        size_t index = i / 64;

        __m128i chunk1 = _mm_loadu_si128((__m128i *)&bitmap->array[index]);
        __m128i mask = _mm_set1_epi64x(-1);  // Set mask to all 1s (full range set)

        // Compare loaded data with all 1s
        if (!_mm_testc_si128(chunk1, mask)) {
            return false;
        }
    }

    // Handle remaining bits
    if (start_bit < end_bit) {
        size_t last_index = start_bit / 64;
        unsigned long last_chunk = bitmap->array[last_index];

        size_t remaining_bits = end_bit - start_bit;
        unsigned long last_chunk_mask = (1UL << remaining_bits) - 1;

        if ((last_chunk & last_chunk_mask) != last_chunk_mask) {
            return false;
        }
    }

    return true;
}
*/

int main(){

        bit_array_t *bitmap = BitArrayCreate(128);
        BitArrayClearAll(bitmap);


        long pos_first_setbit;
        long start = 6;
        long nr_bits = 10;

        long check_start = 7;
        long nr_check_bits = 10;
        //BitArraySetBit(bitmap, 1);
        BitArraySetRange(bitmap, start, nr_bits);


        long first_unset_bit = BitArrayGetFirstUnsetBit(bitmap, check_start, nr_check_bits);
        long first_set_bit = BitArrayGetFirstSetBit(bitmap, check_start, nr_check_bits);

        printf("bitmap[0]=%lx\n", bitmap->array[0]);
        printf("bitmap[1]=%lx\n", bitmap->array[1]);

        printf("first set bit = %ld\n", first_set_bit);
        printf("first unset bit = %ld\n", first_unset_bit);



        return 0;

        long long start_time_put = get_time_in_ns();

        /*
           for(int i=0; i<1000000; i++)
           pos_first_setbit = BitArrayGetFirstSetBit(bitmap, 6, 60);
           */

        //if(is_bitrange_set(bitmap->array, 63, 2)){

        /*
           for(int i=0; i<7; i++){
           if(BitArrayTestBit(bitmap, i))
           continue;
           else
           break;
           }
           */

        if(BitArrayIsSet_SSE(bitmap, check_start, nr_check_bits)){
//                printf("bitrange set from %ld to %ld\n", check_start, check_start+nr_check_bits);
        }
        /*
           else{
           printf("bitrange not set from %ld to %ld\n", check_start, check_start+nr_check_bits);
           }
           */

        long long end_time_put = get_time_in_ns();

        printf("Total time taken to put = %lld ns\n", end_time_put - start_time_put);

        printf("pos_frist setbit = %ld\n", pos_first_setbit);
        printf("bitmap[0]=%lx\n", bitmap->array[0]);
        printf("bitmap[1]=%lx\n", bitmap->array[1]);

        return 0;
}
