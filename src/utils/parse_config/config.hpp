#pragma once

#include <stddef.h>
#include "utils/util.hpp"

/* List sinks (caller-owned buffers) */
typedef struct {
    long long *vals;  /* array storage */
    size_t     cap;   /* capacity (elements) */
    size_t    *len;   /* OUT: how many written */
} int_list_sink_t;

typedef struct {
    char     **vals;  /* array of strdup'd strings */
    size_t     cap;   /* capacity (elements) */
    size_t    *len;   /* OUT: how many written */
} str_list_sink_t;

/* Network address type for OPT_ADDR */
typedef struct {
    char host[256];   /* hostname, IPv4, or IPv6 literal (no brackets) */
    int  port;        /* 1..65535 */
} net_addr_t;

struct AppCfg {
    /* scalars */
    char       start_stop_path[PATH_MAX];
    char       licensekeys_path[PATH_MAX];

    /* new */
    net_addr_t server;          /* OPT_ADDR */
    char       api_base[PATH_MAX];   /* OPT_URL */

    /* lists */
    char      *devices [MAX_DEVICES];   size_t n_devices;
};

extern struct AppCfg *cfg;