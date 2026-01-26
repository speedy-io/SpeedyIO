#include "events_logger.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>

#include "../shim/shim.hpp"

void log_event_to_file(int outfile_fd, const std::string& event_string) {
    if (outfile_fd < 0) {
        throw std::runtime_error(
            std::string(__func__) + ": invalid value for outfile_fd=" +
            std::to_string(outfile_fd) + " " + std::strerror(errno)
        );
    }
    real_write(outfile_fd, event_string.c_str(), event_string.size());
}

int open_event_logger_file(const std::string& filename) {
    int outfile_fd = real_open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (outfile_fd == -1) {
        throw std::runtime_error("Error opening output file: " + filename + " - " + std::strerror(errno));
    }
    return outfile_fd;
}
