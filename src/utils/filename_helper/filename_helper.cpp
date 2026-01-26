#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <error.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include <stddef.h>
#include <stdint.h>

#include "filename_helper.hpp"
#include "utils/util.hpp"

/**
 * The problem we are trying to solve here are twofold:
 * 1. for a given pathname we dont know if it is a softlink to some target file.
 * 2. for a file opened with openat, ie. giving a relative path, the filename_hash
 * will not match if during unlink the absolute path is passed.
 * So, we would like a function that for a given absolute/relative path, gives
 * us the target file's absolute path. This way we can deduplicate files that are
 * opened with softlinks and real filenames etc aswell.
 *
 * So, the below functions does exactly that.
 * If orig_pathname is an absolute path, it resolves to its target filepath.
 * If orig_path is a relative path to dirfd, it resolves to its absolute path.
 * 
 * returns true if successful else false
 */
bool resolve_symlink_and_get_abs_path(int dirfd, const char *orig_pathname, char *outbuf, size_t outbuf_sz){
    bool ret = false;
    char *fd_path = nullptr;
    char *dir_path = nullptr;
    char *combined_path = nullptr;
    char *resolved_path = nullptr;
    ssize_t len;
    size_t dir_len;
    size_t file_len;

    if(!orig_pathname || !outbuf || outbuf_sz == 0){
        SPEEDYIO_FPRINTF("%s:ERROR bad input\n", "SPEEDYIO_ERRCO_0130\n");
        goto exit;
    }

    // Buffer for final resolved absolute path
    resolved_path = (char *)malloc(MAX_ABS_PATH_LEN);
    if(!resolved_path){
        SPEEDYIO_FPRINTF("%s:ERROR could not allocate resolved_path\n", "SPEEDYIO_ERRCO_0131\n");
        goto exit;
    }

    // ------------------------------------------------------------------
    // 1. If 'orig_pathname' is already absolute, just resolve it directly
    // ------------------------------------------------------------------
    if(orig_pathname[0] == '/') {
        // Attempt to resolve the absolute (possibly symlink) path
        if(realpath(orig_pathname, resolved_path)){
            if(strlen(resolved_path) >= outbuf_sz){
                SPEEDYIO_FPRINTF("%s:ERROR resolved_path exceeds outbuf_sz:%ld\n", "SPEEDYIO_ERRCO_0132 %ld\n", outbuf_sz);
            goto exit;
        }
        strncpy(outbuf, resolved_path, outbuf_sz);
        outbuf[outbuf_sz - 1] = '\0';
        ret = true;
        }else{
            //realpath() failed; fallback to just copying orig_pathname
            SPEEDYIO_FPRINTF("%s:ERROR realpath failed on '%s' resolved_path:'%s' (%s)\n", "SPEEDYIO_ERRCO_0133 %s %s %s\n", orig_pathname, resolved_path, strerror(errno));

            if(strlen(orig_pathname) >= outbuf_sz){
                    goto exit;
            }
            strncpy(outbuf, orig_pathname, outbuf_sz);
            outbuf[outbuf_sz - 1] = '\0';
            ret = true;
        }
        goto exit;
    }

    // ------------------------------------------------------------------
    // 2. If 'orig_pathname' is relative, resolve dirfd → absolute path
    // ------------------------------------------------------------------
    // Allocate a buffer to hold "/proc/self/fd/<dirfd>"
    // Allocate a buffer for the directory path
    dir_path = (char *)malloc(MAX_ABS_PATH_LEN);
    if(!dir_path){
        SPEEDYIO_FPRINTF("%s:ERROR could not allocate dir_path\n", "SPEEDYIO_ERRCO_0134\n");
        goto exit;
    }

    if(dirfd == AT_FDCWD){
        if (!getcwd(dir_path, MAX_ABS_PATH_LEN)) {
            SPEEDYIO_FPRINTF("%s:ERROR getcwd failed (%s)\n", "SPEEDYIO_ERRCO_0135 %s\n", strerror(errno));
            goto exit;
        }
    }else{
        /*there is a valid dirfd*/
        // Allocate a buffer to hold "/proc/self/fd/<dirfd>"
        fd_path = (char *)malloc(64);
        if(!fd_path){
            SPEEDYIO_FPRINTF("%s:ERROR could not allocate fd_path\n", "SPEEDYIO_ERRCO_0136\n");
            goto exit;
        }
        snprintf(fd_path, 64, "/proc/self/fd/%d", dirfd);

        // Resolve the directory path from the dirfd
        len = readlink(fd_path, dir_path, MAX_ABS_PATH_LEN - 1);
        if(len == -1){
            SPEEDYIO_FPRINTF("%s:ERROR could not readlink for dirfd:%d (%s)\n", "SPEEDYIO_ERRCO_0137 %d %s\n", dirfd, strerror(errno));
            goto exit;
        }
        dir_path[len] = '\0';
    }

    // Allocate buffer for combining dir_path + "/" + orig_pathname
    combined_path = (char *)malloc(MAX_ABS_PATH_LEN);
    if(!combined_path){
        SPEEDYIO_FPRINTF("%s:ERROR could not allocate combined_path\n", "SPEEDYIO_ERRCO_0138\n");
        goto exit;
    }

    // Build something like: <dir_path>/<orig_pathname>
    dir_len = strlen(dir_path);
    file_len = strlen(orig_pathname);

    if(dir_len + 1 + file_len + 1 > MAX_ABS_PATH_LEN){
        SPEEDYIO_FPRINTF("%s:ERROR combined path too long\n", "SPEEDYIO_ERRCO_0139\n");
        goto exit;
    }

    snprintf(combined_path, MAX_ABS_PATH_LEN, "%s/%s", dir_path, orig_pathname);

    // Now call realpath on the combined (relative→absolute) path
    if(realpath(combined_path, resolved_path)){
        // realpath resolved symlinks as well
        if(strlen(resolved_path) >= outbuf_sz){
            SPEEDYIO_FPRINTF("%s:ERROR resolved path exceeds outbuf size\n", "SPEEDYIO_ERRCO_0140\n");
            goto exit;
        }
        strncpy(outbuf, resolved_path, outbuf_sz);
        outbuf[outbuf_sz - 1] = '\0';
        ret = true;
    }else{
            // realpath() failed; fallback to the combined_path itself
            SPEEDYIO_FPRINTF("%s:ERROR realpath failed on '%s' (%s)\n", "SPEEDYIO_ERRCO_0141 %s %s\n", combined_path, strerror(errno));

        if(strlen(combined_path) >= outbuf_sz){
            SPEEDYIO_FPRINTF("%s:ERROR strlen(combined_path):%ld >= outbuf_sz:%ld\n", "SPEEDYIO_ERRCO_0142 %ld %ld\n", strlen(combined_path), outbuf_sz);
            goto exit;
        }
        strncpy(outbuf, combined_path, outbuf_sz);
        outbuf[outbuf_sz - 1] = '\0';
        ret = true;
    }

exit:
    if (resolved_path)  free(resolved_path);
    if (combined_path)  free(combined_path);
    if (dir_path)       free(dir_path);
    if (fd_path)        free(fd_path);
    return ret;
}


/**
 * Construct an absolute path for "orig_pathname" using "dirfd" as the base.
 * - If "orig_pathname" is absolute, "dirfd" is ignored.
 * - If "dirfd == AT_FDCWD", uses getcwd(...) as the base.
 * - Normalizes "." and ".." without following symlinks.
 * - Result is written into "outbuf" (size "outbuf_sz").
 * Returns true on success, false on failure (e.g., path too long).
 */
bool get_abs_path(int dirfd, const char *orig_pathname, char *outbuf, size_t outbuf_sz){
    if (!orig_pathname || !outbuf || outbuf_sz == 0)
        return false;
    
    // We'll build an intermediate path in this buffer.
    // This may be absolute right away or be formed by combining dir_path + orig_pathname.
    char combined[PATH_MAX];
    combined[0] = '\0';

    // 1) If orig_pathname is already absolute, just copy to 'combined'.
    if (orig_pathname[0] == '/') {
        if (strlen(orig_pathname) >= sizeof(combined))
            return false;  // Too long
        strcpy(combined, orig_pathname);
    } 
    // 2) Otherwise, get the absolute path for dirfd, then append orig_pathname.
    else {
        char dir_path[PATH_MAX];

        if (dirfd == AT_FDCWD) {
            // Use the current working directory
            if (!getcwd(dir_path, sizeof(dir_path)))
                return false;  // getcwd failed
        } else {
            // Read the absolute directory path from /proc/self/fd/<dirfd>
            char proc_path[64];
            snprintf(proc_path, sizeof(proc_path), "/proc/self/fd/%d", dirfd);

            ssize_t len = readlink(proc_path, dir_path, sizeof(dir_path) - 1);
            if (len < 0)
                return false;  // readlink failed
            dir_path[len] = '\0';
        }

        // Combine the two with a slash in between
        // "dir_path/orig_pathname"
        if (snprintf(combined, sizeof(combined), "%s/%s", dir_path, orig_pathname) >= (int)sizeof(combined))
            return false;  // path too long
    }

    // Now "combined" is absolute or relative turned into absolute, but it may contain
    // extraneous "." or ".." or multiple slashes. We canonicalize it in-place.

    // We'll parse the path segments (split by "/"), handle "." and "..", and rejoin.
    char *tokens[PATH_MAX];
    int token_count = 0;

    // Track whether the final path is absolute.
    bool is_abs = (combined[0] == '/');

    // We'll do in-place tokenization. Convert any leading '/' to a token boundary:
    // for absolute paths, skip the leading '/'
    char *p = combined;
    if (is_abs) {
        // If path starts with '/', temporarily skip it so that we don't get an empty token at the start.
        p++;
    }

    // Tokenize by '/'
    char *saveptr = NULL;
    char *segment = strtok_r(p, "/", &saveptr);
    while (segment) {
        if (strcmp(segment, "") == 0 || strcmp(segment, ".") == 0) {
            // Ignore empty or "." segments
        }
        else if (strcmp(segment, "..") == 0) {
            // Parent directory
            if (token_count > 0) {
                // If the last token isn't "..", pop it
                // (unless the last token was also "..", which we keep if it's relative)
                if (strcmp(tokens[token_count - 1], "..") != 0) {
                    token_count--;
                } else {
                    // last token is also "..", so add another ".."
                    tokens[token_count++] = segment;
                }
            } else {
                // No tokens to pop:
                // - if absolute, ignore ".." at root (i.e., no going above "/")
                // - if not absolute, then we keep ".." for relative references
                if (!is_abs) {
                    tokens[token_count++] = segment;
                }
            }
        }
        else {
            // Normal path segment
            tokens[token_count++] = segment;
        }

        segment = strtok_r(NULL, "/", &saveptr);
    }

    // 3) Reassemble the path into final form. Start from scratch in `outbuf`:
    //    - If absolute, begin with "/".
    //    - Then join tokens with '/'.
    //    - If it ends up empty (and was absolute), it's "/".
    char temp[PATH_MAX];
    int pos = 0;

    if (is_abs) {
        temp[pos++] = '/';
    }

    for (int i = 0; i < token_count; i++) {
        const char *tok = tokens[i];
        size_t seg_len = strlen(tok);
        // If not the very beginning (pos > 1 for absolute or pos > 0 for relative),
        // we insert a slash before the next segment
        bool need_slash = false;
        if (is_abs) {
            // Already placed '/' if pos == 1. If pos > 1, place slash
            if (pos > 1) need_slash = true;
        } else {
            // If pos > 0, place slash
            if (pos > 0) need_slash = true;
        }

        if (need_slash) {
            if ((pos + 1) >= (int)sizeof(temp)) 
                return false;  // would overflow
            temp[pos++] = '/';
        }

        // Copy the segment
        if ((pos + (int)seg_len) >= (int)sizeof(temp))
            return false;  // would overflow
        memcpy(temp + pos, tok, seg_len);
        pos += seg_len;
    }

    // If we ended up with an empty path (no tokens) and it's absolute, then it's "/"
    if (pos == 0 && is_abs) {
        temp[pos++] = '/';
    }
    temp[pos] = '\0';

    // Finally, check for size and copy to outbuf
    if (strlen(temp) >= outbuf_sz)
        return false;

    strcpy(outbuf, temp);
    return true;
}
