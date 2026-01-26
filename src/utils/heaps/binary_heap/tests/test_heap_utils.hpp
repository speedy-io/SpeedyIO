#ifndef TEST_HEAP_UTILS_HPP
#define TEST_HEAP_UTILS_HPP

#include "heap.hpp"
#include "catch_amalgamated.hpp"
#include <vector>
#include <random>

// ------------------------------
// Randomization Helpers
// ------------------------------
int randomIndex(std::mt19937& rng, int size);
float randomScale(std::mt19937& rng, float minF, float maxF);

// ------------------------------
// Struct for Tracking Test Data
// ------------------------------
struct TestRecord {
    int id;
    float key;
};

// ------------------------------
// Heap Utility Functions
// ------------------------------
void fillHeapWithRandoms(Heap* H, int N, unsigned seed, float minK, float maxK, std::vector<TestRecord>& outRecords);
void verifyExtractAllSorted(Heap* H);

// ------------------------------
// Fixture for Heap Tests
// ------------------------------
struct HeapFixture {
    Heap* heap = nullptr;
    int defaultCapacity;

    HeapFixture(int capacity = 20000);
    ~HeapFixture();
};

#endif // TEST_HEAP_UTILS_HPP