#ifndef SYSTEM_INFO_HPP
#define SYSTEM_INFO_HPP

#include <string>

extern char device[32];

// Function to get the device name from the path
std::string getDeviceFromPath(const std::string& path);

void *update_system_stats(void *a);

void updateSystemStats();

// Function to get the current IO queue size
double getIoQueueSize();

long getMinMemoryRequiredKB();

// Function to get the current available memory
long getAvailableMemoryKB();

// Function to get the maximum available memory encountered
long getMaxAvailableMemoryKB();

// Function to get the current free memory
long getFreeMemoryKB();

// Function to get the maximum free memory encountered
long getMaxFreeMemoryKB();

// Function to get the current read await time
double getReadAwait();

// Function to get the current write await time
double getWriteAwait();

#endif // SYSTEM_INFO_HPP
