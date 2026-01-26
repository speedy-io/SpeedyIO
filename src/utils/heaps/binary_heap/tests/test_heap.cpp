#ifndef CATCH_CONFIG_MAIN
#define CATCH_CONFIG_MAIN
#endif

#include "catch_amalgamated.hpp"
#include "test_heap_utils.hpp"
#include <random>
#include <vector>
#include <cstdlib>


// -------------------------------------------------------------
// Test Cases
// -------------------------------------------------------------

TEST_CASE_METHOD(HeapFixture, "Heap Insert and Verify Sorted Order", "[binary_heap][insert_sorted]") {
    const int N = 10000;
    std::mt19937 rng(42); // Fixed seed for reproducibility
    std::uniform_real_distribution<float> dist(1.0f, 10000.0f);

    for (int i = 0; i < N; i++) {
        float key = dist(rng);
        float* dataptr = (float*)malloc(sizeof(float));
        *dataptr = key;

        heap_insert(heap, key, dataptr);
    }

    REQUIRE(heap->size == (std::size_t)N);
    verifyExtractAllSorted(heap);
}

TEST_CASE_METHOD(HeapFixture, "Heap Stress Test - Many Decrease Keys", "[binary_heap][stress_decrease]") {
    const int N = 20000;
    const int Y = 1e7;

    std::vector<TestRecord> records;
    fillHeapWithRandoms(heap, N, /*seed=*/1234, /*minK=*/100.0f, /*maxK=*/100000.0f, records);

    std::mt19937 rng(99999);
    for (int i = 0; i < Y; i++) {
        int idx = randomIndex(rng, N);
        int id = records[idx].id;
        float oldKey = records[idx].key;

        float scale = randomScale(rng, 0.0f, 0.9f);
        float newKey = oldKey * scale;

        records[idx].key = newKey;
        heap_update_key(heap, id, newKey);
    }

    verifyExtractAllSorted(heap);
}

TEST_CASE_METHOD(HeapFixture, "Heap Stress Test - Many Increase Keys", "[binary_heap][stress_increase]") {
    const int N = 20000;
    const int Y = 1e7;

    std::vector<TestRecord> records;
    fillHeapWithRandoms(heap, N, /*seed=*/42, /*minK=*/1.0f, /*maxK=*/1000.0f, records);

    std::mt19937 rng(54321);
    for (int i = 0; i < Y; i++) {
        int idx = randomIndex(rng, N);
        int id = records[idx].id;
        float oldKey = records[idx].key;

        float scale = randomScale(rng, 1.01f, 5.0f);
        float newKey = oldKey * scale;

        records[idx].key = newKey;
        heap_update_key(heap, id, newKey);
    }

    verifyExtractAllSorted(heap);
}

TEST_CASE_METHOD(HeapFixture, "Heap Get Key by ID", "[binary_heap][get_key]") {
    const int N = 100;

    std::vector<TestRecord> records;
    fillHeapWithRandoms(heap, N, /*seed=*/12345, /*minK=*/10.0f, /*maxK=*/1000.0f, records);

    // Check retrieval of valid IDs
    for (const auto& record : records) {
        float storedKey = heap_get_key_by_id(heap, record.id);
        REQUIRE(storedKey == Catch::Approx(record.key).epsilon(0.0001f));
    }

    // Test invalid IDs 
    SECTION("Invalid ID causes failure") {
        REQUIRE_THROWS_AS(heap_get_key_by_id(heap, -1), std::runtime_error);
        REQUIRE_THROWS_AS(heap_get_key_by_id(heap, N + 100), std::runtime_error);
    }
}

TEST_CASE_METHOD(HeapFixture, "Heap Delete Key by ID", "[binary_heap][delete_key]") {
    const int N = 100;
    std::mt19937 rng(42); // Fixed seed for reproducibility
    std::uniform_real_distribution<float> dist(1.0f, 10000.0f);

    /*filling records in the heap*/
    std::vector<TestRecord> records;
    fillHeapWithRandoms(heap, N, /*seed=*/12345, /*minK=*/10.0f, /*maxK=*/1000.0f, records);
    
    /*checking the size of the generated heap*/
    REQUIRE(heap->size == (std::size_t)N);

    /*checking if an arbitrary element within range is available*/
    int id = randomIndex(rng, N);
    float storedKey = heap_get_key_by_id(heap, id);

    /*deleting the element with id*/
    heap_delete_key_by_id(heap, id);

    /*size of heap after deletion should be N-1*/
    REQUIRE(heap->size == (std::size_t)(N-1));
    
    /*getting the id should throw runtime error*/
    REQUIRE_THROWS_AS(heap_get_key_by_id(heap, id), std::runtime_error);

    /*Now checking if adding a new element increases the size of heap*/
    float key = dist(rng);
    float* dataptr = (float*)malloc(sizeof(float));
    *dataptr = key;
    int new_id = heap_insert(heap, key, dataptr);
    REQUIRE(heap->size == (std::size_t)(N));
    
    /*
    * Since old id range will be from 0 to N-1.
    * Hence the new_id should be N.
    */
    REQUIRE(new_id == (N));
}


TEST_CASE_METHOD(HeapFixture, "Heap Clear", "[binary_heap][clear]") {
    // Insert several items into the heap.
    const int N = 100;

    std::vector<TestRecord> records;
    fillHeapWithRandoms(heap, N, /*seed=*/12345, /*minK=*/10.0f, /*maxK=*/1000.0f, records);

    // Verify that the heap is populated.
    REQUIRE(heap->size == static_cast<std::size_t>(N));
    REQUIRE_FALSE(heap->storage.empty());
    REQUIRE_FALSE(heap->id2index.empty());

    // Call the clear function to reset the heap.
    heap_clear(heap);  // This should reset storage, id2index, size, and next_id.

    // Check that the heap is now in its pristine condition.
    REQUIRE(heap->size == 0);
    REQUIRE(heap->storage.empty());
    REQUIRE(heap->id2index.empty());

    // Optionally, if your implementation resets next_id to zero,
    // you can also test that.
    REQUIRE(heap->next_id == 0);
}