#include <stdio.h>
#include <stdbool.h>
#include <string.h>

#include "../util.hpp"

bool endsWith(const char* str, const char* suffix) {
    if (!str || !suffix) return false;
    size_t strLen = strlen(str);
    size_t suffixLen = strlen(suffix);
    if (suffixLen > strLen) return false;
    // Compare the ending portion of str with suffix using pointer arithmetic
    return strcmp(str + (strLen - suffixLen), suffix) == 0;
}

bool containsSubstring(const char* str, const char* substr) {
    return strstr(str, substr) != nullptr;
}

const char *whitelist[] = {"Index.db", "Data.db", ".sst"};
// const char *whitelist[] = {".db", ".log", ".sst"};
//const char *whitelist[] = {"big-Data.db", ".sst"};
// const char *whitelist[] = {".db", ".log", ".sst"};

const char *fadv_whitelist[] = {"Data.db", "Index.db"};

//const char *whitelist[] = {"Data.db", "Index.db"};
// const char *whitelist[] = {"sfdaguisadf"};
//const char *whitelist[] = {".db", ".sst"};

/*
 * Returns true for only files with suffixes that are whitelisted
 * FIXME: Will need to change this for other filesystems
 */
bool is_whitelisted(const char *filename) {
        bool ret = false;
        size_t nr_whitelist = sizeof(whitelist) / sizeof(whitelist[0]);

        // printf("%s: nr_whitelist: %zu\n", __func__, nr_whitelist);

        // Compare the extension with each whitelisted extension
        for (int i = 0; i < nr_whitelist; i++){
                if (endsWith(filename, whitelist[i])){
                        ret = true;
                        goto is_whitelisted_exit;
                }
        }

is_whitelisted_exit:
        return ret;
}

/**
* returns true for files not in fadv_whitelist
* returns false for all other files
*/
bool to_skip_fadv_random(const char *filename) {
        bool ret = true;
        size_t nr_whitelist = sizeof(fadv_whitelist) / sizeof(fadv_whitelist[0]);

        // printf("%s: nr_whitelist: %zu\n", __func__, nr_whitelist);

        // Compare the extension with each whitelisted extension
        for (int i = 0; i < nr_whitelist; i++){
                if (endsWith(filename, fadv_whitelist[i])){
                        ret = false;
                        goto to_skip_fadv_random_exit;
                }
        }

to_skip_fadv_random_exit:
        return ret;
}