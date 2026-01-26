#ifndef READERS_WRITERS_LOCK_HPP
#define READERS_WRITERS_LOCK_HPP

#include <mutex>
#include <condition_variable>

class ReaderWriterLock {
public:
    ReaderWriterLock();

    void lock_read();
    void unlock_read();

    void lock_write();
    void unlock_write();

private:
    std::mutex mtx;
    std::condition_variable readers_cv;
    std::condition_variable writers_cv;
    int active_readers;
    int active_writers;
    int waiting_writers;
};

#endif
