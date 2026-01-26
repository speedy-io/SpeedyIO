#include "readers_writers_lock.hpp"

ReaderWriterLock::ReaderWriterLock()
    : active_readers(0), active_writers(0), waiting_writers(0) {}

void ReaderWriterLock::lock_read() {
    std::unique_lock<std::mutex> lock(mtx);
    /*
     * This does not priorities writers - can starve writers
     * readers_cv.wait(lock, [this]() { return active_writers == 0; });
     */
    /*This prioritizes writers*/
    readers_cv.wait(lock, [this]() { return active_writers == 0 && waiting_writers == 0; });
    ++active_readers;
}

void ReaderWriterLock::unlock_read() {
    std::unique_lock<std::mutex> lock(mtx);
    if (--active_readers == 0 && waiting_writers > 0) {
        writers_cv.notify_one();
    }
}

void ReaderWriterLock::lock_write() {
    std::unique_lock<std::mutex> lock(mtx);
    ++waiting_writers;
    writers_cv.wait(lock, [this]() { return active_readers == 0 && active_writers == 0; });
    --waiting_writers;
    ++active_writers;
}

void ReaderWriterLock::unlock_write() {
    std::unique_lock<std::mutex> lock(mtx);
    --active_writers;
    if (waiting_writers > 0) {
        writers_cv.notify_one();
    } else {
        readers_cv.notify_all();
    }
}
