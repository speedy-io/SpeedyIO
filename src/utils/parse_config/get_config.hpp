#ifndef _GET_CONFIG_HPP
#define _GET_CONFIG_HPP

#include "config.hpp"

#pragma once
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------ Types ------------------ */

typedef enum {
    OPT_STR,        /* dest = char* buffer, dest_sz = size; expands $VARS unless single-quoted */
    OPT_INT,        /* dest = long long*; expands $VARS unless single-quoted */
    OPT_BOOL,       /* dest = int*; expands $VARS unless single-quoted */
    OPT_PATH,       /* dest = char* buffer, dest_sz = size; expands ~ and $VARS unless single-quoted */

    OPT_STR_LIST,   /* dest = str_list_sink_t* */
    OPT_INT_LIST,   /* dest = int_list_sink_t* */
    OPT_PATH_LIST,  /* dest = str_list_sink_t*; expands ~ and $VARS per item unless single-quoted */

    OPT_ADDR,       /* NEW: dest = net_addr_t* (host + port), value expands unless single-quoted */
    OPT_URL         /* NEW: dest = char* buffer, dest_sz = size; http/https URL validated, expands unless single-quoted */
} opt_type_t;

/* Per-key flags */
enum {
    OPTF_REQUIRED             = 1 << 0,  /* key must be present */
    OPTF_PATH_MUST_EXIST      = 1 << 1,  /* must exist (any type) */
    OPTF_OPTIONAL             = 1 << 2,  /* explicit optional (doc-only) */
    OPTF_PATH_MUSTNOT_EXIST   = 1 << 3,  /* must NOT exist */
    OPTF_PATH_MUST_BE_FILE    = 1 << 4,  /* must exist and be a regular file */
    OPTF_PATH_MUST_BE_DIR     = 1 << 5   /* must exist and be a directory */
};

/* One schema entry */
typedef struct {
    const char  *key;       /* config key name */
    opt_type_t   type;      /* expected type */
    void        *dest;      /* where to store parsed value(s) */
    size_t       dest_sz;   /* for STR/PATH/URL: buffer size; lists ignore */
    long long    min_i;     /* INT/INT_LIST: inclusive min */
    long long    max_i;     /* INT/INT_LIST: inclusive max */
    int          flags;     /* OPTF_* */
    int          seen;      /* (internal) set when encountered */
} option_spec_t;

/* Parse bools: returns 1 on success, sets *out to 0/1. Accepts 1/0,true/false,yes/no,on/off (case-insensitive). */
int parse_bool(const char *s, int *out);

/* Load config per schema.
 * allow_unknown = 0 -> unknown keys are errors; otherwise unknown keys are ignored.
 * Returns 0 on success, -1 on error (prints diagnostics to stderr).
 */
int config_load_schema(const char *filename,
                       option_spec_t *spec, size_t nspec,
                       int allow_unknown);

/* Utility to free a STR_LIST / PATH_LIST sink (frees strdup'd entries, resets len to 0). */
void config_free_str_list(str_list_sink_t *sink);

static int expand_path(const char *in, char **out);
int get_config();

#ifdef __cplusplus
}
#endif

#endif
