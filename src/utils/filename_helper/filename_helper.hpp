#ifndef _FILENAME_HELPER_HPP
#define _FILENAME_HELPER_HPP

#include <stddef.h>

bool resolve_symlink_and_get_abs_path(int, const char *, char *, size_t);
bool get_abs_path(int, const char *, char *, size_t);

#endif
