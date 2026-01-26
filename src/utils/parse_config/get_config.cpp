#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <wordexp.h>
#include <ctype.h>
#include <strings.h>

#include <sys/stat.h>

#include "get_config.hpp"
#include "utils/util.hpp"
#include "utils/system_info/system_info.hpp"
#include "config.hpp"

struct AppCfg *cfg = nullptr;

/**
 * returns:
 * 1 - if everything was correct
 * 0 - CFG_FILE_ENV_VAR was not set or was empty
 * -1 - CFG_FILE_ENV_VAR was set and file was not found at the location
 * -2 - CFG_FILE_ENV_VAR file was opened but had some formatting issue
 */
int get_config(){
        int ret = 0;
        char *path = nullptr;
        int expand_ret;
        const char *env = getenv(CFG_FILE_ENV_VAR);
        int rc;

        cfg = (struct AppCfg *)malloc(sizeof(struct AppCfg));
        if(!cfg){
                cfprintf(stderr, "%s:ERROR unable to alloc memory for cfg\n", __func__);
                KILLME();
        }
        memset(cfg, 0, sizeof(struct AppCfg));

        /* temporary list sinks */
        str_list_sink_t devices_sink  = {cfg->devices, MAX_DEVICES, &cfg->n_devices};

        option_spec_t spec[] = {
                /* key, type, dest, dest_sz, min, max, flags, seen */
                {"start_stop_file", OPT_PATH, cfg->start_stop_path, sizeof(cfg->start_stop_path), 0, 0, OPTF_REQUIRED, 0},
#ifdef ENABLE_LICENSE
                {"licensekey_dir", OPT_PATH, cfg->licensekeys_path, sizeof(cfg->licensekeys_path), 0, 0, OPTF_PATH_MUST_BE_DIR | OPTF_REQUIRED, 0},
#endif //ENABLE_LICENSE

                /* address and URL */
                {"server", OPT_ADDR, &cfg->server, 0, 0, 0, OPTF_OPTIONAL, 0},
                {"api_base", OPT_URL, cfg->api_base, sizeof(cfg->api_base), 0, 0, OPTF_OPTIONAL, 0},

                /* arrays */
                {"devices", OPT_STR_LIST, &devices_sink, 0, 0, 0, OPTF_OPTIONAL, 0}
        };

        if(!env || !*env){
                fprintf(stderr, "ERROR: ENV Variable %s is not set or empty.\n", CFG_FILE_ENV_VAR);
                ret = 0;
                goto exit_get_config;
        }

        printf("%s:%s\n", CFG_FILE_ENV_VAR, env);

        expand_ret = expand_path(env, &path);
        if(expand_ret == -1){
                fprintf(stderr, "ERROR: ENV Variable is set but unable to expand path\n");
                ret = -1;
                goto exit_get_config;
        }
        else if(expand_ret == -2){
                fprintf(stderr, "ERROR: Unable to find the file at the provided path %s\n", path);
                ret = -1;
                goto exit_get_config;
        }

        rc = config_load_schema(path, spec, sizeof(spec)/sizeof(spec[0]), true);
        if (rc != 0) {
                fprintf(stderr, "Failed loading config: %s\n", env);
                ret = -2;
                goto exit_get_config;
        }

        /* cleanup */
        if(path){
                free(path);
        }

        ret = 1;

exit_get_config:
        return ret;
}


/* ------------------ small utils ------------------ */

int parse_bool(const char *s, int *out) {
        if (!s) return 0;
        if (!strcasecmp(s,"1") || !strcasecmp(s,"true") || !strcasecmp(s,"yes") || !strcasecmp(s,"on"))  { if(out)*out=1; return 1; }
        if (!strcasecmp(s,"0") || !strcasecmp(s,"false")|| !strcasecmp(s,"no")  || !strcasecmp(s,"off")) { if(out)*out=0; return 1; }
        return 0;
}

static char *xstrdup(const char *s){
        size_t n = strlen(s) + 1;
        char *p = (char*)malloc(n);
        if (p) memcpy(p, s, n);
        return p;
}

static char *trim(char *s) {
        while (*s && isspace((unsigned char)*s)) s++;
        if (!*s) return s;
        char *e = s + strlen(s) - 1;
        while (e > s && isspace((unsigned char)e[0])) e--;
        e[1] = '\0';
        return s;
}

/* Normalize common Unicode junk to plain ASCII:
 * - U+201C/U+201D (“/”) -> "
 * - U+2018/U+2019 (‘/’) -> '
 * - U+00A0 NBSP -> ' '
 * - BOM U+FEFF and zero-width U+200B/U+200C/U+200D -> removed
 */
static void normalize_ascii(char *s) {
        unsigned char *p = (unsigned char*)s;
        unsigned char *w = (unsigned char*)s;
        while (*p) {
                if (p[0] == 0xC2 && p[1] == 0xA0) { *w++ = ' '; p += 2; continue; }                                     /* NBSP */
                if (p[0] == 0xEF && p[1] == 0xBB && p[2] == 0xBF) { p += 3; continue; }                                 /* BOM */
                if (p[0] == 0xE2 && p[1] == 0x80 && (p[2] == 0x8B || p[2] == 0x8C || p[2] == 0x8D)) { p += 3; continue; }/* ZW chars */
                if (p[0] == 0xE2 && p[1] == 0x80 && (p[2] == 0x9C || p[2] == 0x9D)) { *w++ = '"'; p += 3; continue; }   /* smart “ ” */
                if (p[0] == 0xE2 && p[1] == 0x80 && (p[2] == 0x98 || p[2] == 0x99)) { *w++ = '\''; p += 3; continue; }  /* smart ‘ ’ */
                *w++ = *p++;
        }
        *w = '\0';
}

static void strip_inline_comment(char *s) {
        int in_q = 0, qtype = 0;
        for (char *p = s; *p; ++p) {
                if ((*p == '"' || *p == '\'') && (p == s || p[-1] != '\\')) {
                        if (!in_q) { in_q = 1; qtype = *p; }
                        else if (qtype == *p) { in_q = 0; qtype = 0; }
                }
                if (!in_q && (*p == '#' || *p == ';')) { *p = '\0'; break; }
        }
}

static const char* wordexp_err(int rc){
        switch(rc){
                case WRDE_BADCHAR: return "bad character in expression";
                case WRDE_BADVAL:  return "undefined/bad variable";
                case WRDE_CMDSUB:  return "command substitution disabled";
                case WRDE_NOSPACE: return "memory allocation failure";
                case WRDE_SYNTAX:  return "syntax error (likely unmatched quotes)";
                default:           return "unknown wordexp error";
        }
}


/* wordexp-based expansion: expands $VARS and ~ with WRDE_NOCMD.
 * If require_single: error if result has multiple words.
 * If join_on_space: join multiple results with spaces (for STR).
 * Returns 0 on success, negative wordexp rc on failure.
 */
static int shell_expand(const char *in, char *out, size_t out_sz,
                int require_single, int join_on_space)
{
        if (!in) return -WRDE_BADVAL;

        wordexp_t we;
        int rc = wordexp(in, &we, WRDE_NOCMD);
        if (rc != 0) {
                errno = EINVAL;
                return -rc;
        }

        if (require_single) {
                if (we.we_wordc != 1) { wordfree(&we); errno = E2BIG; return -WRDE_SYNTAX; }
                size_t need = strlen(we.we_wordv[0]) + 1;
                if (need > out_sz) { wordfree(&we); errno = ENAMETOOLONG; return -WRDE_NOSPACE; }
                memcpy(out, we.we_wordv[0], need);
                wordfree(&we);
                return 0;
        }

        size_t total = 1; /* trailing NUL */
        for (size_t i = 0; i < we.we_wordc; ++i) total += strlen(we.we_wordv[i]) + (i && join_on_space ? 1 : 0);
        if (total > out_sz) { wordfree(&we); errno = ENAMETOOLONG; return -WRDE_NOSPACE; }

        out[0] = '\0';
        for (size_t i = 0; i < we.we_wordc; ++i) {
                if (i && join_on_space) strcat(out, " ");
                strcat(out, we.we_wordv[i]);
        }
        wordfree(&we);
        return 0;
}


/* Expand a path-like string:
 * - single-quoted (qflag==1): literal, no expansion
 * - otherwise: re-wrap with "..." so spaces stay one word, then expand (~, $VARS)
 * Returns 0 on success, negative wordexp rc on failure.
 */
static int expand_path_str(const char *val, unsigned char qflag,
                char *out, size_t out_sz)
{
        if (qflag == 1) { /* literal */
                size_t n = strlen(val);
                if (n + 1 > out_sz) return -WRDE_NOSPACE;
                memcpy(out, val, n + 1);
                return 0;
        }
        size_t n = strlen(val);
        char *tmp = (char*)malloc(n + 3);
        if (!tmp) return -WRDE_NOSPACE;
        tmp[0] = '"'; memcpy(tmp+1, val, n); tmp[1+n] = '"'; tmp[2+n] = '\0';
        /* require_single=1 so we enforce exactly one path; join_on_space=0 */
        int rc = shell_expand(tmp, out, out_sz, /*require_single*/1, /*join_on_space*/0);
        free(tmp);
        return rc;
}

/* Expand according to top-level quote flag:
 * qflag: 0 none, 1 single-quoted (literal), 2 double-quoted (expand but keep grouping)
 */
static int expand_with_qflag(const char *val, unsigned char qflag,
                char *out, size_t out_sz,
                int require_single, int join_on_space)
{
        if (qflag == 1) { /* single quotes: literal */
                size_t n = strlen(val);
                if (n + 1 > out_sz) { errno = ENAMETOOLONG; return -WRDE_NOSPACE; }
                memcpy(out, val, n + 1);
                return 0;
        }

        if (qflag == 2) { /* double quotes: expand but treat as one word if needed */
                size_t n = strlen(val);
                char *tmp = (char*)malloc(n + 3);
                if (!tmp) return -WRDE_NOSPACE;
                tmp[0] = '"'; memcpy(tmp+1, val, n); tmp[1+n] = '"'; tmp[2+n] = '\0';
                int rc = shell_expand(tmp, out, out_sz, require_single, join_on_space);
                free(tmp);
                return rc;
        }

        return shell_expand(val, out, out_sz, require_single, join_on_space);
}

/* Split comma-separated values with quotes and spaces.
 * Returns tokens + their quote type (0 none, 1 single, 2 double).
 * Modifies s in place.
 */
static size_t split_csv_like_ex(char *s, char **outv, unsigned char *qtype, size_t maxv) {
        size_t n = 0;
        char *p = s;
        int in_q = 0; int qt = 0;
        char *start = p;

        while (1) {
                char c = *p;
                if ((c == '"' || c == '\'') && (p == s || p[-1] != '\\')) {
                        if (!in_q) { in_q = 1; qt = c; }
                        else if (qt == c) { in_q = 0; }
                        p++; continue;
                }
                if ((c == ',' && !in_q) || c == '\0') {
                        char *end = p;
                        if (c == ',') *end = '\0';

                        char *tok = start;
                        while (*tok && isspace((unsigned char)*tok)) tok++;
                        while (end > tok && isspace((unsigned char)end[-1])) end--;
                        *end = '\0';

                        unsigned char qt_out = 0;
                        size_t len = strlen(tok);
                        if (len >= 2 && ((tok[0] == '"' && tok[len-1] == '"') ||
                                                (tok[0] == '\'' && tok[len-1] == '\''))) {
                                qt_out = (tok[0] == '"') ? 2 : 1;
                                tok[len-1] = '\0';
                                tok++;
                        }

                        if (*tok && n < maxv) { outv[n] = tok; qtype[n] = qt_out; n++; }

                        if (c == '\0') break;
                        start = p + 1;
                }
                if (c == '\0') break;
                p++;
        }
        return n;
}

/* ------------------ parsing helpers for ADDR/URL ------------------ */

static int is_valid_hostname_char(int c){
        return isalnum(c) || c == '-' || c == '.';
}

static int is_ipv4_literal(const char* s){
        int dots = 0; int seg = 0; int val = 0; int digits = 0;
        for (const char* p=s; *p; ++p){
                if (*p == '.') { if (++dots > 3) return 0; if (digits==0) return 0; seg=0; val=0; digits=0; continue; }
                if (!isdigit((unsigned char)*p)) return 0;
                val = val*10 + (*p - '0'); if (val > 255) return 0; digits++; seg++;
                if (seg > 3) return 0;
        }
        return dots == 3 && digits > 0;
}

/* Parse host:port into net_addr_t. Supports:
 * - hostname:port
 * - 1.2.3.4:port
 * - [IPv6]:port  (IPv6 MUST be bracketed)
 * Returns 0 on success, -1 on error.
 */
static int parse_hostport(const char *s, net_addr_t *out){
        if (!s || !*s) return -1;

        if (s[0] == '[') { /* IPv6 in brackets */
                const char *r = strchr(s, ']');
                if (!r) return -1;
                if (r[1] != ':') return -1;
                const char *pport = r + 2;
                if (!*pport) return -1;
                char *end = NULL; errno = 0;
                long port = strtol(pport, &end, 10);
                if (errno || *end || port < 1 || port > 65535) return -1;
                size_t hostlen = (size_t)(r - (s + 1));
                if (hostlen == 0 || hostlen >= sizeof(out->host)) return -1;
                memcpy(out->host, s + 1, hostlen);
                out->host[hostlen] = '\0';
                out->port = (int)port;
                return 0;
        }

        /* Find last ':' to split host and port (so "a:b:c" is rejected) */
        const char *colon = strrchr(s, ':');
        if (!colon) return -1;
        /* Reject additional ':' (likely IPv6 without brackets) */
        if (memchr(s, ':', (size_t)(colon - s)) != NULL) return -1;

        size_t hostlen = (size_t)(colon - s);
        if (hostlen == 0 || hostlen >= sizeof(out->host)) return -1;
        memcpy(out->host, s, hostlen);
        out->host[hostlen] = '\0';

        /* Basic host validation for hostname/IPv4 */
        int ok = 1;
        if (is_ipv4_literal(out->host)) ok = 1;
        else {
                for (size_t i=0;i<hostlen;i++) if (!is_valid_hostname_char((unsigned char)out->host[i])) { ok = 0; break; }
                if (out->host[0] == '-' || out->host[hostlen-1] == '-') ok = 0;
        }
        if (!ok) return -1;

        const char *pport = colon + 1;
        if (!*pport) return -1;
        char *end = NULL; errno = 0;
        long port = strtol(pport, &end, 10);
        if (errno || *end || port < 1 || port > 65535) return -1;
        out->port = (int)port;
        return 0;
}

/* Validate minimal http/https URL: scheme://host[:port][/path][?query][#frag]
 * Accepts hostnames, IPv4, or [IPv6].
 * Returns 0 on success, -1 on error.
 */
static int validate_http_url(const char *s){
        if (!s) return -1;
        const char *p = strstr(s, "://");
        if (!p) return -1;
        size_t scheme_len = (size_t)(p - s);
        if (scheme_len == 0) return -1;
        /* Only http or https */
        if (!((scheme_len==4 && !strncasecmp(s,"http",4)) ||
                                (scheme_len==5 && !strncasecmp(s,"https",5)))) return -1;
        p += 3; /* point to start of host */

        if (*p == '\0') return -1;

        /* Host could be [IPv6] */
        const char *host_start = p;
        const char *host_end = NULL;
        int bracket = 0;
        if (*p == '[') {
                bracket = 1;
                const char *r = strchr(p, ']');
                if (!r) return -1;
                host_end = r + 1;
                p = host_end;
        } else {
                /* host until : / ? # or end */
                while (*p && *p != ':' && *p != '/' && *p != '?' && *p != '#') p++;
                host_end = p;
        }
        if (host_end <= host_start) return -1;

        /* Optional :port */
        if (*p == ':') {
                p++;
                const char *port_start = p;
                if (!isdigit((unsigned char)*p)) return -1;
                long port = 0;
                while (isdigit((unsigned char)*p)) { port = port*10 + (*p - '0'); if (port > 65535) return -1; p++; }
                if (port < 1 || port > 65535) return -1;
        }

        /* Optional path/query/fragment */
        /* Nothing more to validate strictly; just ensure no spaces */
        for (const char *q = p; *q; ++q) if (isspace((unsigned char)*q)) return -1;

        return 0;
}

/* ------------------ schema helpers ------------------ */

static option_spec_t *find_spec(option_spec_t *spec, size_t nspec, const char *key) {
        for (size_t i = 0; i < nspec; ++i)
                if (strcmp(spec[i].key, key) == 0) return &spec[i];
        return NULL;
}

static int path_exist_check(const char *path, int flags,
        const char *filename, int lineno, const char *key)
{
        struct stat st;
        int exists = (stat(path, &st) == 0);

        if ((flags & OPTF_PATH_MUST_EXIST) && !exists) {
                fprintf(stderr, "%s:%d: path for key '%s' does not exist: '%s'\n",
                filename, lineno, key, path);
                return -1;
        }

        if ((flags & OPTF_PATH_MUSTNOT_EXIST) && exists) {
                fprintf(stderr, "%s:%d: path for key '%s' already exists: '%s'\n",
                filename, lineno, key, path);
                return -1;
        }

        if (flags & OPTF_PATH_MUST_BE_FILE) {
                if (!exists || !S_ISREG(st.st_mode)) {
                        fprintf(stderr, "%s:%d: path for key '%s' must be a regular file: '%s'\n",
                        filename, lineno, key, path);
                        return -1;
                }
        }

        if (flags & OPTF_PATH_MUST_BE_DIR) {
                if (!exists || !S_ISDIR(st.st_mode)) {
                        fprintf(stderr, "%s:%d: path for key '%s' must be a directory: '%s'\n",
                        filename, lineno, key, path);
                        return -1;
                }
        }

        return 0;
}

/* Assign parsed value. For repeated keys: scalars last-wins, lists append.
 * qflag: 0 none, 1 single-quoted, 2 double-quoted at top-level value
 */
static int assign_value(option_spec_t *os, const char *val, unsigned char qflag,
                int lineno, const char *filename)
{
        char buf[4096];

        switch (os->type) {
                case OPT_STR: {
                                      if (!os->dest || os->dest_sz == 0) goto bad_dest;
                                      int rc = expand_with_qflag(val, qflag, buf, sizeof(buf), 0, 1);
                                      if (rc != 0) { fprintf(stderr, "%s:%d: expand '%s' failed: %s\n",
                                                      filename, lineno, os->key, wordexp_err(-rc)); return -1; }
                                      size_t n = strlen(buf);
                                      if (n + 1 > os->dest_sz) { errno = ENAMETOOLONG; goto too_long; }
                                      memcpy(os->dest, buf, n + 1);
                                      return 0;
                              }
                case OPT_INT: {
                                      const char *src = val;
                                      if (qflag != 1) {
                                              int rc = expand_with_qflag(val, qflag, buf, sizeof(buf), 1, 0);
                                              if (rc != 0) { fprintf(stderr, "%s:%d: expand '%s' failed: %s\n",
                                                              filename, lineno, os->key, wordexp_err(-rc)); return -1; }
                                              src = buf;
                                      }
                                      if (!os->dest) goto bad_dest;
                                      char *end = NULL; errno = 0;
                                      long long v = strtoll(src, &end, 0);
                                      if (errno || !src[0] || (end && *end)) {
                                              fprintf(stderr, "%s:%d: invalid integer for key '%s': '%s'\n", filename, lineno, os->key, src);
                                              return -1;
                                      }
                                      if (v < os->min_i || v > os->max_i) {
                                              fprintf(stderr, "%s:%d: key '%s' out of range (%lld..%lld): %lld\n",
                                                              filename, lineno, os->key, os->min_i, os->max_i, v);
                                              return -1;
                                      }
                                      *(long long*)os->dest = v;
                                      return 0;
                              }
                case OPT_BOOL: {
                                       const char *src = val;
                                       if (qflag != 1) {
                                               int rc = expand_with_qflag(val, qflag, buf, sizeof(buf), 1, 0);
                                               if (rc != 0) { fprintf(stderr, "%s:%d: expand '%s' failed: %s\n",
                                                               filename, lineno, os->key, wordexp_err(-rc)); return -1; }
                                               src = buf;
                                       }
                                       if (!os->dest) goto bad_dest;
                                       int b;
                                       if (!parse_bool(src, &b)) {
                                               fprintf(stderr, "%s:%d: invalid bool for key '%s': '%s' (use 1/0,true/false,yes/no,on/off)\n",
                                                               filename, lineno, os->key, src);
                                               return -1;
                                       }
                                       *(int*)os->dest = b;
                                       return 0;
                               }
                case OPT_PATH: {
                                       if (!os->dest || os->dest_sz == 0) goto bad_dest;
                                       int rc = expand_path_str(val, qflag, buf, sizeof(buf));
                                       if (rc != 0) {
                                               fprintf(stderr, "%s:%d: failed to expand path for key '%s': '%s' (%s)\n",
                                                               filename, lineno, os->key, val, wordexp_err(-rc));
                                               return -1;
                                       }
                                       size_t n = strlen(buf);
                                       if (n + 1 > os->dest_sz) { errno = ENAMETOOLONG; goto too_long; }
                                       memcpy(os->dest, buf, n + 1);
                                       if (path_exist_check((char*)os->dest, os->flags, filename, lineno, os->key) != 0) return -1;
                                       return 0;
                               }
                case OPT_ADDR: {
                                       if (!os->dest) goto bad_dest;
                                       int rc = expand_with_qflag(val, qflag, buf, sizeof(buf), 1, 0);
                                       if (rc != 0) {
                                               fprintf(stderr, "%s:%d: failed to expand address for key '%s': '%s' (%s)\n",
                                                               filename, lineno, os->key, val, wordexp_err(-rc));
                                               return -1;
                                       }
                                       net_addr_t *na = (net_addr_t*)os->dest;
                                       if (parse_hostport(buf, na) != 0) {
                                               fprintf(stderr, "%s:%d: invalid address for key '%s': '%s'  (use host:port, [ipv6]:port)\n",
                                                               filename, lineno, os->key, buf);
                                               return -1;
                                       }
                                       return 0;
                               }
                case OPT_URL: {
                                      if (!os->dest || os->dest_sz == 0) goto bad_dest;
                                      int rc = expand_with_qflag(val, qflag, buf, sizeof(buf), 1, 0);
                                      if (rc != 0) {
                                              fprintf(stderr, "%s:%d: failed to expand URL for key '%s': '%s' (%s)\n",
                                                              filename, lineno, os->key, val, wordexp_err(-rc));
                                              return -1;
                                      }
                                      if (validate_http_url(buf) != 0) {
                                              fprintf(stderr, "%s:%d: invalid URL for key '%s': '%s'  (expected http[s]://host[:port]/...)\n",
                                                              filename, lineno, os->key, buf);
                                              return -1;
                                      }
                                      size_t n = strlen(buf);
                                      if (n + 1 > os->dest_sz) { errno = ENAMETOOLONG; goto too_long; }
                                      memcpy(os->dest, buf, n + 1);
                                      return 0;
                              }
                case OPT_INT_LIST: {
                                           int_list_sink_t *sink = (int_list_sink_t*)os->dest;
                                           if (!sink || !sink->vals || !sink->len) goto bad_dest;

                                           char *tmp = xstrdup(val); if (!tmp) return -1;
                                           char *parts[512]; unsigned char qt[512];
                                           size_t nparts = split_csv_like_ex(tmp, parts, qt, 512);
                                           if (nparts == 0) { free(tmp); return 0; }

                                           for (size_t i = 0; i < nparts; i++) {
                                                   if (*(sink->len) >= sink->cap) {
                                                           fprintf(stderr, "%s:%d: too many entries for '%s' (cap=%zu)\n",
                                                                           filename, lineno, os->key, sink->cap);
                                                           free(tmp); return -1;
                                                   }
                                                   const char *src = parts[i];
                                                   if (qt[i] != 1) {
                                                           int rc = expand_with_qflag(parts[i], qt[i], buf, sizeof(buf), 1, 0);
                                                           if (rc != 0) {
                                                                   fprintf(stderr, "%s:%d: failed to expand item in '%s': '%s' (%s)\n",
                                                                                   filename, lineno, os->key, parts[i], wordexp_err(-rc));
                                                                   free(tmp); return -1;
                                                           }
                                                           src = buf;
                                                   }
                                                   char *end = NULL; errno = 0;
                                                   long long v = strtoll(src, &end, 0);
                                                   if (errno || !src[0] || (end && *end)) {
                                                           fprintf(stderr, "%s:%d: invalid integer in '%s': '%s'\n",
                                                                           filename, lineno, os->key, src);
                                                           free(tmp); return -1;
                                                   }
                                                   if (v < os->min_i || v > os->max_i) {
                                                           fprintf(stderr, "%s:%d: value out of range for '%s' (%lld..%lld): %lld\n",
                                                                           filename, lineno, os->key, os->min_i, os->max_i, v);
                                                           free(tmp); return -1;
                                                   }
                                                   sink->vals[(*sink->len)++] = v;
                                           }
                                           free(tmp);
                                           return 0;
                                   }
                case OPT_STR_LIST: /* fallthrough */
                case OPT_PATH_LIST: {
                                            str_list_sink_t *sink = (str_list_sink_t*)os->dest;
                                            if (!sink || !sink->vals || !sink->len) goto bad_dest;

                                            char *tmp = xstrdup(val); if (!tmp) return -1;
                                            char *parts[512]; unsigned char qt[512];
                                            size_t nparts = split_csv_like_ex(tmp, parts, qt, 512);
                                            if (nparts == 0) { free(tmp); return 0; }

                                            for (size_t i = 0; i < nparts; i++) {
                                                    if (*(sink->len) >= sink->cap) {
                                                            fprintf(stderr, "%s:%d: too many entries for '%s' (cap=%zu)\n",
                                                                            filename, lineno, os->key, sink->cap);
                                                            free(tmp); return -1;
                                                    }
                                                    char *final = NULL;
                                                    if (os->type == OPT_PATH_LIST) {
                                                            if (qt[i] == 1) { /* literal */
                                                                    final = xstrdup(parts[i]);
                                                            } else {
                                                                    int rc = expand_path_str(parts[i], qt[i], buf, sizeof(buf));
                                                                    if (rc != 0) {
                                                                            fprintf(stderr, "%s:%d: failed to expand path item in '%s': '%s' (%s)\n",
                                                                                            filename, lineno, os->key, parts[i], wordexp_err(-rc));
                                                                            free(tmp); return -1;
                                                                    }
                                                                    if (path_exist_check(buf, os->flags, filename, lineno, os->key) != 0) {
                                                                            free(tmp); return -1;
                                                                    }
                                                                    final = xstrdup(buf);
                                                            }
                                                    } else { /* STR_LIST */
                                                            if (qt[i] == 1) {
                                                                    final = xstrdup(parts[i]);
                                                            } else {
                                                                    int rc = expand_with_qflag(parts[i], qt[i], buf, sizeof(buf), 0, 1);
                                                                    if (rc != 0) {
                                                                            fprintf(stderr, "%s:%d: failed to expand item in '%s': '%s' (%s)\n",
                                                                                            filename, lineno, os->key, parts[i], wordexp_err(-rc));
                                                                            free(tmp); return -1;
                                                                    }
                                                                    final = xstrdup(buf);
                                                            }
                                                    }
                                                    if (!final) { free(tmp); return -1; }
                                                    sink->vals[(*sink->len)++] = final;
                                            }
                                            free(tmp);
                                            return 0;
                                    }
                default:
                                    fprintf(stderr, "%s:%d: internal: unknown type for key '%s'\n", filename, lineno, os->key);
                                    return -1;
        }

too_long:
        fprintf(stderr, "%s:%d: value too long for key '%s'\n", filename, lineno, os->key);
        return -1;

bad_dest:
        fprintf(stderr, "%s:%d: internal: bad destination for key '%s'\n", filename, lineno, os->key);
        return -1;
}

/* ------------------ main loader ------------------ */

int config_load_schema(const char *filename,
                option_spec_t *spec, size_t nspec,
                int allow_unknown)
{
        for (size_t i = 0; i < nspec; ++i) spec[i].seen = 0;

        FILE *f = fopen(filename, "r");
        if (!f) {
                fprintf(stderr, "ERROR: cannot open config file %s: %s\n", filename, strerror(errno));
                return -1;
        }

        char *line = NULL;
        size_t cap = 0;
        ssize_t n;
        int rc = 0;
        int lineno = 0;

        while ((n = getline(&line, &cap, f)) != -1) {
                lineno++;
                if (n > 0 && (line[n-1] == '\n' || line[n-1] == '\r')) line[--n] = 0;

                normalize_ascii(line);
                char *p = trim(line);
                if (*p == 0 || *p == '#' || *p == ';') continue;

                char *eq = strchr(p, '=');
                if (!eq) {
                        fprintf(stderr, "%s:%d: ignoring malformed line (no '=')\n", filename, lineno);
                        continue;
                }
                *eq = 0;
                char *key = trim(p);
                char *val = trim(eq + 1);

                /* top-level quote handling; qflag: 0 none, 1 single, 2 double */
                unsigned char qflag = 0;
                if (*val == '"' || *val == '\'') {
                        char quote = *val;
                        qflag = (quote == '"') ? 2 : 1;
                        char *q = val + 1;
                        int closed = 0;
                        while (*q) {
                                if (*q == quote && q[-1] != '\\') { closed = 1; break; }
                                q++;
                        }
                        if (!closed) {
                                fprintf(stderr, "%s:%d: unterminated quoted value for key '%s'\n", filename, lineno, key);
                                rc = -1; break;
                        }
                        *q = '\0';  /* terminate AT the closing quote */
                        val++;      /* drop opening quote */
                } else {
                        strip_inline_comment(val);
                        val = trim(val);
                }

                option_spec_t *os = find_spec(spec, nspec, key);
                if (!os) {
                        if (!allow_unknown) {
                                fprintf(stderr, "%s:%d: unknown key '%s'\n", filename, lineno, key);
                                rc = -1; break;
                        }
                        continue;
                }

                if (assign_value(os, val, qflag, lineno, filename) != 0) { rc = -1; break; }
                os->seen = 1;
        }

        free(line);
        fclose(f);

        if (rc == 0) {
                for (size_t i = 0; i < nspec; ++i) {
                        if ((spec[i].flags & OPTF_REQUIRED) && !spec[i].seen) {
                                fprintf(stderr, "%s: missing required key '%s'\n", filename, spec[i].key);
                                rc = -1;
                        }
                }
        }

        return rc;
}

/* ------------------ cleanup helpers ------------------ */

void config_free_str_list(str_list_sink_t *sink) {
        if (!sink || !sink->vals || !sink->len) return;
        for (size_t i = 0; i < *sink->len; i++) {
                free(sink->vals[i]);
                sink->vals[i] = NULL;
        }
        *sink->len = 0;
}


/**
 * returns 0 if successfully expanded and found the config file
 * -1 if unable to expand the file path from the env variable
 * -2 if unable to find the file at the provided path
 */
static int expand_path(const char *in, char **out) {
        int ret = 0;
        wordexp_t we;
        struct stat st;
        int rc;

        if (!in || !*in){
                ret = -1;
                goto exit_expand_path;
        }

        rc = wordexp(in, &we, WRDE_NOCMD);
        if (rc != 0) {
                ret = -1;
                goto exit_expand_path;
        }
        if (we.we_wordc != 1) {
                wordfree(&we);
                ret = -1;
                goto exit_expand_path;
        }

        *out = strdup(we.we_wordv[0]);
        wordfree(&we);

        if (!*out){
                ret = -1;
                goto exit_expand_path;
        }

        // Check if file exists
        if (stat(*out, &st) != 0) {
                free(*out);
                *out = NULL;
                ret = -2; // File does not exist
                goto exit_expand_path;
        }

exit_expand_path:
        return ret; // Success, file exists
}
