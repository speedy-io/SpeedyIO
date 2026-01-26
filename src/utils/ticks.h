#pragma once
#include <stdint.h>

//
// ticks.h — cross-arch RDTSC-style timestamp counter
//
// Usage:
//   uint64_t t0 = ticks_now();
//   ... work ...
//   uint64_t t1 = ticks_now();
//   uint64_t dt = ticks_elapsed(t1, t0);
//

#if defined(__x86_64__) || defined(_M_X64)

  #include <x86intrin.h>   // works on GCC/Clang
  // or #include <intrin.h> // for MSVC
  // #include <immintrin.h>     // also fine on GCC/Clang/MSVC for __rdtsc()

  static inline __attribute__((always_inline)) uint64_t ticks_now() {
      return __rdtsc();     // native x86 timestamp counter
  }

#elif defined(__aarch64__)

  // ARMv8+ (Graviton, Neoverse, etc.)
  static inline __attribute__((always_inline)) uint64_t ticks_now() {
      uint64_t t;
      asm volatile("mrs %0, cntvct_el0" : "=r"(t));  // fast, no ISB
      return t;
  }

  static inline __attribute__((always_inline)) uint64_t ticks_freq_hz() {
      uint64_t f;
      asm volatile("mrs %0, cntfrq_el0" : "=r"(f));
      return f;   // e.g., 1_000_000_000 on Graviton
  }

#else
  #error "Unsupported architecture for ticks_now()"
#endif


// Wrap-safe subtraction for elapsed ticks
static inline __attribute__((always_inline))
uint64_t ticks_elapsed(uint64_t newer, uint64_t older) {
    return newer - older;  // unsigned subtraction handles wrap naturally
}

//
// Optional: convert to nanoseconds on ARM
//
#if defined(__aarch64__)
static inline double ticks_to_ns(uint64_t ticks) {
    const double f = (double)ticks_freq_hz();
    return (ticks * 1e9) / f;
}
#endif