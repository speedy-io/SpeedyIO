#include "catch_amalgamated.hpp"
#include "test_heap_utils.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <iostream>
#include <chrono>
#include <mutex>

// Global mutex for protecting heap operations
std::mutex heap_mutex;

// Struct defining a thread task (heap operation)
struct ThreadTask {
    enum OpType { READ_MIN, INCREASE_KEY, DECREASE_KEY };
    OpType type;
    int id;
    float value;
};

// Function to execute heap operations with multithreading
void run_multithreaded_test(int numThreads, int numInserts, int numReads, int numIncreases, int numDecreases) {
    const int HEAP_SIZE = 1e5;  // Heap size should be larger than total elements
    REQUIRE(HEAP_SIZE >= numInserts);
    
    // Initialize heap
    const char* test_heap = "test_heap";
    Heap* heap = heap_init(HEAP_SIZE, test_heap);
    REQUIRE(heap != nullptr);

    // Step 1: Insert elements (single-threaded)
    std::vector<TestRecord> records;

    auto fillStartTime = std::chrono::high_resolution_clock::now();
    fillHeapWithRandoms(heap, numInserts, /*seed=*/12345, /*minK=*/10.0f, /*maxK=*/100000.0f, records);
    auto fillEndTime = std::chrono::high_resolution_clock::now();
    double totalFillTime = std::chrono::duration<double>(fillEndTime - fillStartTime).count();
    std::cout << "Total fill time: " << totalFillTime << " seconds\n";
    
    REQUIRE(heap->size == numInserts);

    // Step 2: Generate operations
    std::vector<ThreadTask> tasks;
    std::mt19937 rng(6789);  
    std::uniform_int_distribution<int> recordDist(0, numInserts - 1);
    std::uniform_real_distribution<float> scaleIncrease(1.1f, 5.0f);
    std::uniform_real_distribution<float> scaleDecrease(0.1f, 0.9f);

    for (int i = 0; i < numReads; i++) {
        tasks.push_back({ThreadTask::READ_MIN, -1, 0.0f});
    }

    for (int i = 0; i < numIncreases; i++) {
        int idx = recordDist(rng);
        float newKey = records[idx].key * scaleIncrease(rng);
        tasks.push_back({ThreadTask::INCREASE_KEY, records[idx].id, newKey});
        records[idx].key = newKey;
    }

    for (int i = 0; i < numDecreases; i++) {
        int idx = recordDist(rng);
        float newKey = records[idx].key * scaleDecrease(rng);
        tasks.push_back({ThreadTask::DECREASE_KEY, records[idx].id, newKey});
        records[idx].key = newKey;
    }

    // Step 3: Shuffle operations for realistic execution
    std::shuffle(tasks.begin(), tasks.end(), rng);

    // Step 4: Split tasks among threads
    std::vector<std::vector<ThreadTask>> threadTasks(numThreads);
    for (size_t i = 0; i < tasks.size(); i++) {
        threadTasks[i % numThreads].push_back(tasks[i]);
    }

    // Step 5: Execute in parallel
    std::vector<std::thread> threads;
    std::vector<std::chrono::duration<double>> threadTimes(numThreads);
    std::atomic<bool> startFlag(false);

    auto workerFunc = [&](int threadIndex) {
        while (!startFlag.load());  // Wait for all threads to start together

        auto start = std::chrono::high_resolution_clock::now();
        for (const auto& task : threadTasks[threadIndex]) {
            std::lock_guard<std::mutex> lock(heap_mutex);  // Lock heap access
            if (task.type == ThreadTask::READ_MIN) {
                heap_read_min(heap);
            } else if (task.type == ThreadTask::INCREASE_KEY) {
                heap_update_key(heap, task.id, task.value);
            } else if (task.type == ThreadTask::DECREASE_KEY) {
                heap_update_key(heap, task.id, task.value);
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        threadTimes[threadIndex] = end - start;
    };

    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back(workerFunc, t);
    }

    // Start all threads simultaneously
    startFlag.store(true);

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Print execution times per thread
    double totalTime = 0.0;
    for (int t = 0; t < numThreads; t++) {
        double threadTime = threadTimes[t].count();
        std::cout << "\tThread " << t << " execution time: " << threadTime << " seconds\n";
        totalTime += threadTime;
    }
    std::cout << "\t** Mean thread execution time: " << totalTime / numThreads << " seconds\n";


    // Step 6: Ensure correctness
    verifyExtractAllSorted(heap);

    // Cleanup
    heap_destroy(heap);
}

// ------------------------------
// Test Case: Loop Over Thread Counts
// ------------------------------
TEST_CASE("Heap Multithreaded Test with Varying Threads", "[binary_heap][multithread_variable]") {
    std::vector<int> threadCounts = {1, 2, 4, 8, 16, 32};

    for (int numThreads : threadCounts) {
        std::cout << "\n--- Running Test with " << numThreads << " Threads ---\n";

        // Start measuring total execution time (as seen by the user)
        auto globalStart = std::chrono::high_resolution_clock::now();

        run_multithreaded_test(
            numThreads, 
            /*numInserts=*/1e4, 
            /*numReads=*/5e4, 
            /*numIncreases=*/5e6, 
            /*numDecreases=*/5e6
        );

        // End time measurement
        auto globalEnd = std::chrono::high_resolution_clock::now();
        double totalElapsedTime = std::chrono::duration<double>(globalEnd - globalStart).count();

        std::cout << "Total elapsed time: " << totalElapsedTime << " seconds\n";
    }
}