#include <iostream>
#include <thread>
#include <vector>
#include "readers_writers_lock.hpp"

ReaderWriterLock rw_lock;
int shared_resource = 0;

void reader(int id) {
    rw_lock.lock_read();
    std::cout << "Reader " << id << " reads: " << shared_resource << std::endl;
    rw_lock.unlock_read();
}

void writer(int id, int value) {
    rw_lock.lock_write();
    std::cout << "Writer " << id << " writes: " << value << std::endl;
    shared_resource = value;
    rw_lock.unlock_write();
}

int main() {
    std::vector<std::thread> readers;
    std::vector<std::thread> writers;

    for (int i = 0; i < 5; ++i) {
        readers.emplace_back(reader, i);
        writers.emplace_back(writer, i, i*10);
    }

    for (auto& th : readers) {
        th.join();
    }

    for (auto& th : writers) {
        th.join();
    }

    return 0;
}
