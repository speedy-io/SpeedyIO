#ifndef _EVENTS_LOGGER_HPP
#define _EVENTS_LOGGER_HPP

#include <string>

void log_event_to_file(int outfile_fd, const std::string& event_string);

int open_event_logger_file(const std::string& filename);

#endif

