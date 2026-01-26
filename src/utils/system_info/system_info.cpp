// Function to read and parse /proc/diskstats
// More info on diskstats file format : https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats and https://www.kernel.org/doc/Documentation/admin-guide/iostats.rst

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>


#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <iterator>
#include <array>
#include <memory>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <chrono>
#include <unordered_map>
#include "system_info.hpp"
#include "../shim/shim.hpp"
#include "../util.hpp"

#define ZONE_DMA "DMA"
#define ZONE_DMA32 "DMA32"
#define ZONE_NORMAL "Normal"

#define PAGE_SIZE_KB 4
#define KB_IN_GB (1024 * 1024)


char device[32];


// Static cache variable to store the device mappings
static std::unordered_map<std::string, std::string> cache;

// File descriptors which will be used to read the diskstats and meminfo files
static int diskstats_fd = -1;
static int meminfo_fd = -1;

// Global structs to store DiskStats and MemoryStats
struct DiskStats {
        double ioQueueSize; // Average I/O queue size
        double rAwait; // Read await time
        double wAwait; // Write await time
        long prevReadTime = 0; // Previous read time
        long prevWriteTime = 0; // Previous write time
        long prevReadCount = 0; // Previous read count
        long prevWriteCount = 0; // Previous write count
        long prevWeightedIoTime = 0; // Previous weighted I/O time
        double prevUpdateTime = 0; // Previous update time in milliseconds since epoch
        bool isPopulated = false; // Flag to check if stats are populated
};

struct MemoryStats {
        long availableMemoryKB = -1; // MemAvailable
        long maxAvailableMemoryKB = -1; // Maximum MemAvailable encountered

        unsigned long min_memory_required_kb = 0;
        long freeMemoryKB = -1; // MemFree
        long maxFreeMemoryKB = -1; // Maximum MemFree encountered
        bool isPopulated = false; // Flag to check if stats are populated
};

static DiskStats globalDiskStats;
static MemoryStats globalMemoryStats;

// Function to execute a shell command and get the output
static std::string exec(const char* cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
        if (!pipe) throw std::runtime_error("popen() failed!");
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
                result += buffer.data();
        }
        return result;
}

// Function to split a string by whitespace
static std::vector<std::string> split(const std::string& str) {
        std::istringstream iss(str);
        std::vector<std::string> results((std::istream_iterator<std::string>(iss)),
                        std::istream_iterator<std::string>());
        return results;
}

// Function to get the device name from the path
std::string getDeviceFromPath(const std::string& path) {
        std::string cmd = "df " + path + " | awk 'NR==2 {print $1}'";
        std::string result = exec(cmd.c_str());
        result.erase(result.find_last_not_of(" \n\r\t") + 1); // Trim trailing whitespace
        return result.substr(result.find_last_of('/') + 1);   // Get the device name only
}


// Function to read and parse /proc/diskstats
void c_updateDiskStats() {
    // Define a char array for the device
    // Open /proc/diskstats file if not already opened
    if (diskstats_fd == -1) {
        diskstats_fd = real_open("/proc/diskstats", O_RDONLY, 0);
        if (diskstats_fd == -1) {
            SPEEDYIO_FPRINTF("%s:ERROR Could not open /proc/diskstats\n", "SPEEDYIO_ERRCO_0145\n");
        }
    }

    // Read the contents of /proc/diskstats using pread
    char buffer[8192];
    ssize_t bytesRead = real_pread(diskstats_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytesRead == -1) {
        printf("%s:%d\n", __func__, __LINE__);
        return;
    }
    buffer[bytesRead] = '\0';

    // Parse the contents of /proc/diskstats
    std::istringstream diskstats(buffer);
    std::string line;
    while (std::getline(diskstats, line)) {
        //std::cout << line << std::endl;
        auto fields = split(line);
        if (fields.size() >= 14 && strcmp(fields[2].c_str(), device) == 0) {
            long weightedIoTime = std::stol(fields[13]); // Field 14 contains the weighted time spent doing I/Os (in milliseconds)

            struct timespec t;
            clock_gettime(CLOCK_MONOTONIC, &t);
            // Store current time in milliseconds as a double
            double currentTime = t.tv_sec * 1000.0 + (t.tv_nsec / 1e6);

            // Calculate ioQueueSize
            if (globalDiskStats.prevUpdateTime > 0) {
                double elapsedTime = currentTime - globalDiskStats.prevUpdateTime;
                globalDiskStats.ioQueueSize = (weightedIoTime - globalDiskStats.prevWeightedIoTime) / elapsedTime;
            }

            // Calculate r_await and w_await
            long readTime = std::stol(fields[6]);  // Field 7 contains the time spent reading (in milliseconds)
            long writeTime = std::stol(fields[10]);  // Field 11 contains the time spent writing (in milliseconds)
            long readCount = std::stol(fields[3]);  // Field 4 contains the number of reads completed
            long writeCount = std::stol(fields[7]);  // Field 8 contains the number of writes completed

            // Ensure there is a positive difference in read counts to avoid division by zero
            if (readCount - globalDiskStats.prevReadCount > 0) {
                globalDiskStats.rAwait = static_cast<double>(readTime - globalDiskStats.prevReadTime) / (readCount - globalDiskStats.prevReadCount);
            } else {
                globalDiskStats.rAwait = 0.0;  // Set rAwait to 0 if no reads have occurred
            }

            // Ensure there is a positive difference in write counts to avoid division by zero
            if (writeCount - globalDiskStats.prevWriteCount > 0) {
                globalDiskStats.wAwait = static_cast<double>(writeTime - globalDiskStats.prevWriteTime) / (writeCount - globalDiskStats.prevWriteCount);
            } else {
                globalDiskStats.wAwait = 0.0;  // Set wAwait to 0 if no writes have occurred
            }

            // Only set isPopulated to true if all necessary previous values are populated
            if (globalDiskStats.prevUpdateTime > 0 && globalDiskStats.prevWeightedIoTime > 0 && globalDiskStats.prevReadTime > 0 && globalDiskStats.prevWriteTime > 0) {
                globalDiskStats.isPopulated = true;
            }

            // Update previous values for ioQueueSize, rAwait, and wAwait calculations
            globalDiskStats.prevWeightedIoTime = weightedIoTime;  // Update previous weighted I/O time
            globalDiskStats.prevUpdateTime = currentTime;  // Update previous update time
            globalDiskStats.prevReadTime = readTime;
            globalDiskStats.prevWriteTime = writeTime;
            globalDiskStats.prevReadCount = readCount;
            globalDiskStats.prevWriteCount = writeCount;

            return;
        }
    }
    SPEEDYIO_FPRINTF("%s:ERROR Device %s not found in /proc/diskstats\n", "SPEEDYIO_ERRCO_0146 %s\n", device);
    KILLME();
}


// Function to read and parse /proc/meminfo
void updateMemoryStats() {
        // Open /proc/meminfo file if not already opened
        if (meminfo_fd == -1) {
                meminfo_fd = real_open("/proc/meminfo", O_RDONLY, 0);
                if (meminfo_fd == -1) {
                        SPEEDYIO_FPRINTF("%s:ERROR Could not open /proc/meminfo\n", "SPEEDYIO_ERRCO_0147\n");
                        KILLME();
                }
        }

        // Read the contents of /proc/meminfo using pread
        char buffer[8192];
        ssize_t bytesRead = real_pread(meminfo_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytesRead == -1) {
                SPEEDYIO_FPRINTF("%s:ERROR unable to read /proc/meminfo\n", "SPEEDYIO_ERRCO_0148\n");
                KILLME();
        }
        buffer[bytesRead] = '\0';

        // Parse the contents of /proc/meminfo
        std::istringstream meminfo(buffer);
        std::string line;
        while (std::getline(meminfo, line)) {
                auto fields = split(line);
                if (fields[0] == "MemAvailable:") {
                        globalMemoryStats.availableMemoryKB = std::stol(fields[1]);
                        if (globalMemoryStats.availableMemoryKB > globalMemoryStats.maxAvailableMemoryKB) {
                                globalMemoryStats.maxAvailableMemoryKB = globalMemoryStats.availableMemoryKB;
                        }
                        globalMemoryStats.isPopulated = true;
                }
                if (fields[0] == "MemFree:") {
                        globalMemoryStats.freeMemoryKB = std::stol(fields[1]);
                        if (globalMemoryStats.freeMemoryKB > globalMemoryStats.maxFreeMemoryKB) {
                                globalMemoryStats.maxFreeMemoryKB = globalMemoryStats.freeMemoryKB;
                        }
                        globalMemoryStats.isPopulated = true;
                }
        }
}

// Function to convert total pages to kilobytes and round up to the nearest GB
unsigned long pages_to_gb(unsigned long total_pages) {
        // Convert to kilobytes
        unsigned long total_kb = total_pages * PAGE_SIZE_KB;

        // Convert to GB and round up
        unsigned long total_gb = (total_kb + (KB_IN_GB - 1)) / KB_IN_GB;

        return total_gb;
}

// Function to parse and return the total of present pages for DMA, DMA32 and high pages for Normal zone
// returns min KB memory when we should stop speedyio 
unsigned long read_zoneinfo() {
        FILE *fp = fopen("/proc/zoneinfo", "r");
        if (fp == NULL) {
                SPEEDYIO_FPRINTF("%s:ERROR Failed to open /proc/zoneinfo\n", "SPEEDYIO_ERRCO_0149\n");
                return 0;
        }

        char line[256];
        unsigned long total_present_dma = 0, total_present_dma32 = 0, total_high_normal = 0;
        char current_zone[32] = {0};

        while (fgets(line, sizeof(line), fp)) {
                // Look for lines that indicate zone names
                if (strstr(line, "zone")) {
                        sscanf(line, "Node %*d, zone %s", current_zone);
                }

                // Parse the present field for DMA and DMA32 zones
                if ((strcmp(current_zone, ZONE_DMA) == 0 || strcmp(current_zone, ZONE_DMA32) == 0) && strstr(line, "present")) {
                        unsigned long present_pages;
                        sscanf(line, " present %lu", &present_pages);

                        if (strcmp(current_zone, ZONE_DMA) == 0) {
                                total_present_dma += present_pages;
                        } else if (strcmp(current_zone, ZONE_DMA32) == 0) {
                                total_present_dma32 += present_pages;
                        }
                }

                // Parse the high field for the Normal zone
                if (strcmp(current_zone, ZONE_NORMAL) == 0 && strstr(line, "high ")) {
                        unsigned long high_pages;
                        sscanf(line, " high %lu", &high_pages);
                        total_high_normal += high_pages;
                }
        }

        fclose(fp);

        debug_printf("DMA_present:%ld, DMA32_present:%ld, NORMAL_high:%ld\n",
                        total_present_dma, total_present_dma32, total_high_normal);

        //return total_present_dma + total_present_dma32 + total_high_normal;
        return (pages_to_gb(total_present_dma + total_present_dma32 + total_high_normal) * KB_IN_GB);
}

void *update_system_stats(void *a){
        updateSystemStats();
        return nullptr;
}

void updateSystemStats() {
        debug_printf("%s: called", __func__);

        int sleep_milliseconds = SYSTEM_UTIL_SLEEP_MS;
        struct timespec ts;

        globalMemoryStats.min_memory_required_kb = read_zoneinfo();
        while (true) {
                //c_updateDiskStats();
                //updateDiskStats();
                updateMemoryStats();

                ts.tv_sec = sleep_milliseconds / 1000;              // Convert milliseconds to seconds
                ts.tv_nsec = (sleep_milliseconds % 1000) * 1000000L;  // Convert remaining milliseconds to nanoseconds

                if (nanosleep(&ts, NULL) == -1) {
                        SPEEDYIO_FPRINTF("%s:ERROR nanosleep failed\n", "SPEEDYIO_ERRCO_0150\n");
                }
        }
}

long getMinMemoryRequiredKB() {
        return (long) globalMemoryStats.min_memory_required_kb;
}

// Function to get the current IO queue size
double getIoQueueSize() {
        if (!globalDiskStats.isPopulated) {
                //std::cerr << "Error: IO Queue Size data is not populated." << std::endl;
                return -1.0; // or some other error value
        }
        return globalDiskStats.ioQueueSize;
}

// Function to get the current available memory
long getAvailableMemoryKB() {
        if (!globalMemoryStats.isPopulated) {
                return -1;
        }
        return globalMemoryStats.availableMemoryKB;
}

// Function to get the maximum available memory encountered
long getMaxAvailableMemoryKB() {
        if (!globalMemoryStats.isPopulated) {
                return -1;
        }
        return globalMemoryStats.maxAvailableMemoryKB;
}

// Function to get the current free memory
long getFreeMemoryKB() {
        if (!globalMemoryStats.isPopulated) {
                return -1;
        }
        return globalMemoryStats.freeMemoryKB;
}

// Function to get the max free memory
long getMaxFreeMemoryKB() {
        if (!globalMemoryStats.isPopulated) {
                return -1;
        }
        return globalMemoryStats.maxFreeMemoryKB;
}

// Function to get the current read await time
double getReadAwait() {
        if (!globalDiskStats.isPopulated) {
                return -1.0; // or some other error value
        }
        return globalDiskStats.rAwait;
}

// Function to get the current write await time
double getWriteAwait() {
        if (!globalDiskStats.isPopulated) {
                return -1.0; // or some other error value
        }
        return globalDiskStats.wAwait;
}
