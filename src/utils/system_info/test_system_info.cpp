#include <iostream>
#include <unordered_map>
#include <thread>
#include <chrono>
#include "system_info.hpp"

void updateSystemStatsInBackground() {
    updateSystemStats();
}

int main() {
    try {
        // Start the updateSystemStats function in a background thread
        std::thread statsThread(updateSystemStatsInBackground);
        
        for (int i = 0; i < 50000; ++i) {
            double ioQueueSize = getIoQueueSize();

            long availableMemory = getAvailableMemoryKB();
            long maxAvailableMemory = getMaxAvailableMemoryKB();

            long freeMemory = getFreeMemoryKB();
            long maxFreeMemory = getMaxFreeMemoryKB();

            double readAwait = getReadAwait();
            double writeAwait = getWriteAwait();

            std::cout << "Iteration " << i + 1 << ":" << std::endl;
            std::cout << "IO Queue Size is: " << ioQueueSize << std::endl;
            std::cout << "Read Await Time: " << readAwait << " ms" << std::endl;
            std::cout << "Write Await Time: " << writeAwait << " ms" << std::endl;
            std::cout << "Current Available Memory: " << availableMemory << std::endl;
            std::cout << "Maximum Available Memory Encountered: " << maxAvailableMemory << std::endl;
            std::cout << "Current Free Memory: " << freeMemory << std::endl;
            std::cout << "Maximum Free Memory Encountered: " << maxFreeMemory << std::endl << std::endl;


            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // No need to join or detach the background thread, just let the main thread exit.
        // The bg thread will be automatically terminated when the main thread exits.

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
