#include "test_heap_utils.hpp"
#include <cstdlib>

// ------------------------------
// Randomization Helpers
// ------------------------------
int randomIndex(std::mt19937& rng, int size) {
    std::uniform_int_distribution<int> dist(0, size - 1);
    return dist(rng);
}

float randomScale(std::mt19937& rng, float minF, float maxF) {
    std::uniform_real_distribution<float> dist(minF, maxF);
    return dist(rng);
}

// ------------------------------
// Heap Utility Functions
// ------------------------------
void fillHeapWithRandoms(Heap* H, int N, unsigned seed, float minK, float maxK, std::vector<TestRecord>& outRecords) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(minK, maxK);

    outRecords.clear();
    outRecords.reserve(N);

    for (int i = 0; i < N; i++) {
        float key = dist(rng);
        float* dataptr = (float*)malloc(sizeof(float));
        *dataptr = key;

        heap_insert(H, key, dataptr);
        outRecords.push_back({H->next_id - 1, key});
    }
    REQUIRE(H->size == (std::size_t)N);
}

void verifyExtractAllSorted(Heap* H) {
    float prevKey = -1e8f;
    std::size_t count = H->size;

    for (std::size_t i = 0; i < count; i++) {
        HeapItem* e = heap_extract_min(H);
        REQUIRE(e != nullptr);

        REQUIRE(e->key >= prevKey);
        prevKey = e->key;

        free(e->dataptr);
        free(e);
    }
    REQUIRE(H->size == 0);
}

// ------------------------------
// Heap Fixture Implementation
// ------------------------------
HeapFixture::HeapFixture(int capacity) : defaultCapacity(capacity) {
    const char* heap_name = "test_heap";
    heap = heap_init(defaultCapacity, heap_name);
    REQUIRE(heap != nullptr);
}

HeapFixture::~HeapFixture() {
    heap_destroy(heap);
}
