#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <dlfcn.h>
#include <inttypes.h>
// #include <x86intrin.h>

#include <iostream>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <map>
#include <deque>
#include <unordered_map>
#include <string>
#include <iterator>
#include <atomic>

#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>

#include "interface.hpp"

#include "utils/latency_tracking/latency_tracking.hpp"

#include "utils/parse_config/config.hpp"

#ifdef ENABLE_LICENSE
#include "utils/licensing/LicenseValidation.h"
#endif

#ifdef ENABLE_EVICTION
pthread_t eviction_thread;
bool eviction_thread_created = false;

pthread_t start_stop_thread;
#endif

#ifdef ENABLE_SYSTEM_INFO
pthread_t sysinfo_tid;
#endif

#ifdef ENABLE_BG_INODE_CLEANER
pthread_t bg_cleaner_thread;
#endif

unsigned long long int nr_reads = 0;

/*
 * Implements and enables per thread constructor
 * and destructor
 */
extern thread_local per_thread_data per_th_d;

static void construct() __attribute__((constructor));
static void destruct() __attribute__((destructor));

FILE* debug_log_file = nullptr;

void initialize_debug_log() {
#ifdef DEBUG
        #ifdef DEBUG_OUTPUT_FILE
                // Use the DEBUG_FILE_PATH macro for the file path
                debug_log_file = fopen(DEBUG_FILE_PATH, "w");
                if (!debug_log_file) {
                        SPEEDYIO_FPRINTF("%s:ERROR Could not open debug log file '%s' for writing.\n", "SPEEDYIO_ERRCO_0001 %s\n", DEBUG_FILE_PATH);
                KILLME();
                }
        #else
                debug_log_file = stdout;  // Default to stdout if DEBUG_OUTPUT_FILE is not defined
        #endif
#endif
}

void close_debug_log() {
#ifdef DEBUG_OUTPUT_FILE
        if (debug_log_file) {
                fclose(debug_log_file);
        }
#endif
}

/**
 * Testing the shim library is a tricky business to say the least.
 * There are many data structures that keep the state of the system for a given
 * user program. To understand if these data structures are being updated correctly,
 * we might need to query these from inside the library and check it against the correct
 * state given the list of commands run.
 *
 * Below is an example of exposing a function from the library to the outside world.
 * This function can be called by a testing program to query DSs and get their state for checking.
 *
 * XXX: Such tests are yet to be writted. Currently all the tests are checked expecting manual
 * checking of the logs. This has to be fixed with new tests that do the checks automatically.
 */
extern "C" {
        __attribute__((visibility("default")))
        void my_preloaded_function(void){
                printf("[LD_PRELOAD] Hello from my_preloaded_function!\n");
        }
}

/**
 * Just for kix
 */
void print_speedyio_ascii(void) {
        printf(
        "   _____                     __      ________ \n"
        "  / ___/____  ___  ___  ____/ /_  __/  _/ __ \\\n"
        "  \\__ \\/ __ \\/ _ \\/ _ \\/ __  / / / // // / / /\n"
        " ___/ / /_/ /  __/  __/ /_/ / /_/ // // /_/ / \n"
        "/____/ .___/\\___/\\___/\\__,_/\\__, /___/\\____/  \n"
        "    /_/                    /____/             \n"
                );
}

struct lat_tracker handle_read_latency;
struct lat_tracker readsyscalls_latency;
struct lat_tracker get_pfd_latency;

void init_features(){

#ifdef GET_SPEEDYIO_OPTIONS

        printf("*********************************************************************************\n");
        if(get_config() != true){
                fprintf(stderr, "ERROR: Something wrong with config file %s.\n", CFG_FILE_ENV_VAR);
                KILLME();
        }

        printf("Start Stop File:%s\n", cfg->start_stop_path);
        printf("license key folder:%s\n", cfg->licensekeys_path);

        // printf("SpeedyIO server.host=%s\n", cfg->server.host);
        // printf("SpeedyIO server.port=%d\n", cfg->server.port);

        // printf("SpeedyIO API=%s\n", cfg->api_base);

        printf("devices(%zu):\n", cfg->n_devices);
        for (size_t i = 0; i < cfg->n_devices; i++){
                printf("  - %s\n", cfg->devices[i]);
        }

        printf("*********************************************************************************\n");

#ifdef ENABLE_LICENSE
        std::string base = cfg->licensekeys_path;
        std::map<std::string, std::string> licenseInfo =
                validateAndLoadLicense(base + "/key.txt",
                                        base + "/iv.txt",
                                        base + "/public.pem",
                                        base + "/license.lic",
                                        base + "/signature.txt");

        time_t expiry = string_to_time_t(licenseInfo["endDate"]);

        if(check_license_expired_target_date(expiry))
        {
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
                printf("*******SPEEDYIO LICENSE EXPIRED*******\n");
                printf("*******Contact Support*******\n");
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
                exit(1);
        }
        else{
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
                print_speedyio_ascii();
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
                printf("*******SPEEDYIO LICENSE VALID*******\n");
        }
#endif //ENABLE_LICENSE

#else //NO SPEEDYIO OPTIONS
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
                print_speedyio_ascii();
                printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
#endif //GET_SPEEDYIO_OPTIONS

#ifdef ENABLE_SYSTEM_INFO
        if(pthread_create(&sysinfo_tid, NULL, update_system_stats, NULL)) {
                SPEEDYIO_FPRINTF("%s:ERROR creating thread\n", "SPEEDYIO_ERRCO_0005\n");
        }
#endif //ENABLE_SYSTEM_INFO

#ifdef ENABLE_EVICTION
        /* Check RDTSC availability*/
        if(!is_rdtsc_available()){
                SPEEDYIO_FPRINTF("%s:ERROR RDTSC is not available on this CPU\n", "SPEEDYIO_ERRCO_0002\n");
                KILLME();
        }

        //Initialize the global heap
        init_g_heap();

#if defined(BELADY_PROOF) || defined(DISABLE_CONCURRENT_EVICTION)
        SPEEDYIO_PRINTF("%s:INFO skipping concurrent_eviction\n", "SPEEDYIO_INFOCO_0001\n");
#else
        SPEEDYIO_PRINTF("%s:INFO enabling concurrent_eviction\n", "SPEEDYIO_INFOCO_0002\n");
        if(pthread_create(&eviction_thread, NULL, concurrent_eviction, NULL)) {
                SPEEDYIO_FPRINTF("%s:ERROR creating eviction pthread\n", "SPEEDYIO_ERRCO_0003\n");
                eviction_thread_created = false;
        }else{
                eviction_thread_created = true;
        }

#ifdef ENABLE_START_STOP
        if(pthread_create(&start_stop_thread, NULL, start_stop_trigger_checking, NULL)){
                SPEEDYIO_FPRINTF("%s:ERROR in creating start_stop pthread\n", "SPEEDYIO_ERRCO_0004\n");
        }
#endif //ENABLE_START_STOP
#endif //BELADY_PROOF or DISABLE_CONCURRENT_EVICTION
#endif //ENABLE_EVICTION


#ifdef MAINTAIN_INODE
        if(!i_map_init.test_and_set()){
                i_map = init_inode_map();
                if(!i_map){
                        SPEEDYIO_FPRINTF("%s:ERROR init_inode_map failed\n", "SPEEDYIO_ERRCO_0006\n");
                        KILLME();
                }
        }

#ifdef ENABLE_BG_INODE_CLEANER
        if(pthread_create(&bg_cleaner_thread, NULL, bg_inode_cleaner, NULL)){
                cfprintf(stderr, "%s:ERROR in creating bg_cleaner pthread\n", __func__);
        }
#endif //ENABLE_BG_INODE_CLEANER

#endif //MAINTAIN_INODE

}

void construct(){

        /*
         * Link all the shim functions
         * speeds up the linked functions
         */
        link_shim_functions();
        
        // Initialize debug log printing functions
        initialize_debug_log();

        debug_printf("APP starting !\n");

        /*
         * Initializes different data structures
         * for different features
         */
        init_features();
}


void destruct(){

#ifdef ENABLE_EVICTION
        if(likely(eviction_thread_created)){
                pthread_cancel(eviction_thread);
        }
#endif //ENABLE_EVICTION

        print_latencies("read_syscalls - whitelisted files only", &readsyscalls_latency);

        print_latencies("handle_read - whitelisted files only", &handle_read_latency);

        print_latencies("get_perfd_struct_fast", &get_pfd_latency);

        print_latencies("update_pvt_heap - in handle_read", &pvt_heap_latency);

        print_latencies("g_pvt_heap - in handle_read", &g_heap_latency);

        print_latencies("heap_update_key ULONG_MAX- in evict_portions", &ulong_heap_update);

        // Close any open debug log file pointers
        close_debug_log();
        debug_printf("APP Exiting! \n");
}


static inline int path_is_dir(const char* p) {
    struct stat st;
    return (p && lstat(p, &st) == 0 && S_ISDIR(st.st_mode));
}

//OPEN SYSCALLS

void handle_open(struct file_desc file){
        struct thread_args *arg = nullptr;
        struct inode *uinode = nullptr;
        bool whitelisted_file = true;
        std::string open_flags_str;
#ifdef DEBUG
        std::string flag_str;
#endif
        //enables per thread ds
        per_th_d.touchme = true;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        std::shared_ptr<perfd_struct> __pfd = nullptr;
        std::unordered_map<int, std::weak_ptr<struct perfd_struct>>::iterator it;

        debug_printf("%s: filename:%s\n", __func__, file.filename);

        if(!is_whitelisted(file.filename)){
                debug_printf("%s: Not handling BLACKLISTED file:%s fd:%d\n",
                        __func__, file.filename, file.fd);
                whitelisted_file = false;
                goto add_per_fd_ds;
        }

#ifdef DEBUG
        if (file.flags & O_RDONLY) flag_str += "O_RDONLY ";
        if (file.flags & O_WRONLY) flag_str += "O_WRONLY ";
        if (file.flags & O_RDWR) flag_str += "O_RDWR ";
        if (file.flags & O_CREAT) flag_str += "O_CREAT ";
        if (file.flags & O_EXCL) flag_str += "O_EXCL ";
        if (file.flags & O_NOCTTY) flag_str += "O_NOCTTY ";
        if (file.flags & O_TRUNC) flag_str += "O_TRUNC ";
        if (file.flags & O_APPEND) flag_str += "O_APPEND ";
        if (file.flags & O_NONBLOCK) flag_str += "O_NONBLOCK ";
        if (file.flags & O_SYNC) flag_str += "O_SYNC ";
        if (file.flags & O_DSYNC) flag_str += "O_DSYNC ";
        if (file.flags & O_RSYNC) flag_str += "O_RSYNC ";
        if (file.flags & O_DIRECTORY) flag_str += "O_DIRECTORY ";
        if (file.flags & O_NOFOLLOW) flag_str += "O_NOFOLLOW ";
        if (file.flags & O_CLOEXEC) flag_str += "O_CLOEXEC ";

        debug_printf("%s:INFO handling whitelisted fd:%d file:%s with open_flags: %s\n", __func__, file.fd, file.filename, flag_str.c_str());
#endif
        /*
         * checking if the whitelisted file is opened with
         * expected flags.
         */
        if(!check_open_flag_sanity(file)){
                SPEEDYIO_FPRINTF("%s:ERROR open flags for whitelisted file:%s are not sane.\n", "SPEEDYIO_ERRCO_0007 %s\n", file.filename);
                goto handle_open_exit;
        }else{
                debug_printf("%s: open flags sane whitelisted fd:%d file:%s\n",
                                __func__, file.fd, file.filename);
        }

#ifdef MAINTAIN_INODE
        uinode = add_fd_to_inode(file.fd, file.flags, file.filename);
        if(!uinode){
                debug_printf("%s:WARNING Unable to add uinode fd:%d\n", __func__, file.fd);
                goto handle_open_exit;
        }

        if (file.flags & O_RDONLY) open_flags_str += "O_RDONLY ";
        if (file.flags & O_WRONLY) open_flags_str += "O_WRONLY ";
        if (file.flags & O_RDWR) open_flags_str += "O_RDWR ";
        if (file.flags & O_CREAT) open_flags_str += "O_CREAT ";
        if (file.flags & O_EXCL) open_flags_str += "O_EXCL ";
        if (file.flags & O_NOCTTY) open_flags_str += "O_NOCTTY ";
        if (file.flags & O_TRUNC) open_flags_str += "O_TRUNC ";
        if (file.flags & O_APPEND) open_flags_str += "O_APPEND ";
        if (file.flags & O_NONBLOCK) open_flags_str += "O_NONBLOCK ";
        if (file.flags & O_SYNC) open_flags_str += "O_SYNC ";
        if (file.flags & O_DSYNC) open_flags_str += "O_DSYNC ";
        if (file.flags & O_RSYNC) open_flags_str += "O_RSYNC ";
        if (file.flags & O_DIRECTORY) open_flags_str += "O_DIRECTORY ";
        if (file.flags & O_NOFOLLOW) open_flags_str += "O_NOFOLLOW ";
        if (file.flags & O_CLOEXEC) open_flags_str += "O_CLOEXEC ";

        // SPEEDYIO_PRINTF("%s:INFO opened file:%s, fd:%d, {ino:%lu, dev:%lu}, open_flags:%s\n", "SPEEDYIO_INFOCO_0004 %s %d %lu %lu %s\n", file.filename, file.fd, uinode->ino, uinode->dev_id, open_flags_str.c_str());

#endif //MAINTAIN_INODE

add_per_fd_ds:

#ifdef PER_FD_DS
        pfd = add_any_fd_to_perfd_struct(file.fd, file.flags, uinode, whitelisted_file);
        if(!pfd){
                SPEEDYIO_FPRINTF("%s:ERROR Unable to add fd:%d to per_fd_ds\n", "SPEEDYIO_ERRCO_0008 %d\n", file.fd);
                goto handle_open_exit;
        }else{
                debug_printf("%s: fd:%d allocated pfd addr:%p pfd->fd:%d refcount:%ld\n",
                                __func__, file.fd, static_cast<void*>(pfd.get()), pfd->fd, pfd.use_count());
        }

#ifdef PER_THREAD_DS
        /**
         * Since we do not free the pfd for the lifetime of the program,
         * and per_th_d.fd_map[fd] is a weak_ptr to g_fd_map[fd];
         * if a pfd exists in per_th_d.fd_map, it should have the same
         * contents as the one in g_fd_map.
         * If that is not the case, something weird is going on.
         * This is observed when some file is opened and closed before constructor is called.
         *
         * So, in order to keep the sanity of per_th_d.fd_map, we need to check uinode equivalence
         * in both the pfds. If they dont match, replace it with the one from g_fd_map and free the
         * pfd in per_th_d.fd_map.
         */
        /*update pfd only if it is not already there in this per_th_d*/
        it = per_th_d.fd_map->find(file.fd);
        if(it != per_th_d.fd_map->end()){
                __pfd = it->second.lock();
                if(!__pfd || __pfd->uinode != pfd->uinode){
                        /*This condition should not happen a lot of times in the duration of the program.*/
                        debug_fprintf(stderr, "%s:WARNING fd:%d already exists in per_th_d.fd_map. Replacing it\n",
                                        __func__, file.fd);
                        __pfd.reset();
                        it->second = pfd;
                }
        }else{
                /*if no entry*/
                debug_printf("%s: fd:%d not found in per_th_d.fd_map. Adding it now\n",
                                __func__, file.fd);
                per_th_d.fd_map->insert({file.fd, pfd});
        }
#endif //PER_THREAD_DS

#endif //PER_FD_DS

handle_open_exit:

#ifdef ENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES
        /**
         * whenever a file is opened by the program, we do FADV_RANDOM
         * because we dont want the OS to prefetch data items without our knowledge.
         * It reduces the accuracy and correctness of the heap and bitmap.
         */
        // SPEEDYIO_PRINTF("%s:INFO Calling POSIX_FADV_RANDOM for fd: %d\n", "SPEEDYIO_INFOCO_0005 %d\n", file.fd);
        if(real_posix_fadvise(file.fd, 0, 0, POSIX_FADV_RANDOM) != 0){
                SPEEDYIO_FPRINTF("%s:ERROR posix_fadvise failed for fd:%d\n", "SPEEDYIO_ERRCO_0009 %d\n", file.fd);
                KILLME();
        }

        /**
         * The OS assumes any new fd to be read sequentially.
         * After FADV_RANDOM, we set it to false for this fd.
         */
        if(uinode){
                set_fadv_on_fd_uinode(uinode, file.fd, false);
        }
// #elif defined(ENABLE_PVT_HEAP)
// #error "Book keeping pvt_heap will be incorrect if ENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES is disabled with ENABLE_PVT_HEAP."
#endif  // ENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES

        // DEBUGGING memory usage print_mem_usage_all();

        return;
}


extern "C" __attribute__((visibility("default")))
int openat(int dirfd, const char *pathname, int flags, ...){

        int fd;
        struct file_desc file;
        char filebuff[MAX_ABS_PATH_LEN];
        bool changed = false;

#ifdef ENABLE_MINCORE_DEBUG
        /**
         * mmaping a file requires read access to the file. So,
         * for files that are opened with O_WRONLY, we change it
         * to O_RDWR.
         */
        if(flags & O_WRONLY) {
                changed = true;
                flags = (flags & ~O_WRONLY) | O_RDWR;
        }
#endif //ENABLE_MINCORE_DEBUG

        if(flags & O_CREAT){
                va_list valist;
                va_start(valist, flags);
                mode_t mode = va_arg(valist, mode_t);
                va_end(valist);
                fd = real_openat(dirfd, pathname, flags, mode);
        }else{
                fd = real_openat(dirfd, pathname, flags, 0);
        }


        /**
         * Doing an extra check using path_is_dir here because we observed on
         * ARM (AWS r8g.4xlarge with RHEL9) , Cassandra/Java will do
         * open(dir, O_RDONLY) and expect a dir-fd. No O_DIRECTORY flag passed
         * hence this extra check.
         */

        // if(fd < 3 || (flags & O_DIRECTORY)){
        if (fd < 3 || (flags & O_DIRECTORY) || path_is_dir(pathname)){
                goto exit_openat;
        }

        debug_printf("%s: file:%s, fd:%d\n", __func__, pathname, fd);

        /*resolve the symbolic links and create an absolute path*/
        if(!resolve_symlink_and_get_abs_path(dirfd, pathname, filebuff, MAX_ABS_PATH_LEN)){
                SPEEDYIO_FPRINTF("%s:ERROR when calling resolve_symlink_and_get_abs_path on dirfd:%d, pathname:%s\n", "SPEEDYIO_ERRCO_0010 %d %s\n", dirfd, pathname);
                goto exit_openat;
        }else{
                debug_printf("%s: pathname:\"%s\" dirfd:%d resolved to \"%s\"\n",
                                __func__, pathname, dirfd, filebuff);
        }

        file.fd = fd;
        file.filename = pathname;
        file.filename = filebuff;
        if(changed){
                flags = (flags & ~O_RDWR) | O_WRONLY;
        }
        file.flags = flags;
        handle_open(file);

exit_openat:
        return fd;
}

extern "C" __attribute__((visibility("default")))
int open64(const char *pathname, int flags, ...){

        int fd;
        struct file_desc file;
        char filebuff[MAX_ABS_PATH_LEN];
        bool changed = false;

#ifdef ENABLE_MINCORE_DEBUG
        if(flags & O_WRONLY) {
                changed = true;
                flags = (flags & ~O_WRONLY) | O_RDWR;
        }
#endif //ENABLE_MINCORE_DEBUG

        if(flags & O_CREAT){
                va_list valist;
                va_start(valist, flags);
                mode_t mode = va_arg(valist, mode_t);
                va_end(valist);
                fd = real_open(pathname, flags, mode);
        }
        else{
                fd = real_open(pathname, flags, 0);
        }

        /**
         * Doing an extra check using path_is_dir here because we observed on
         * ARM (AWS r8g.4xlarge with RHEL9) , Cassandra/Java will do
         * open(dir, O_RDONLY) and expect a dir-fd. No O_DIRECTORY flag passed
         * hence this extra check.
         */

        // if(fd < 3 || (flags & O_DIRECTORY)){
        if (fd < 3 || (flags & O_DIRECTORY) || path_is_dir(pathname)){
                goto exit_open64;
        }

        debug_printf("%s: file:%s fd:%d\n", __func__, pathname, fd);

        /*resolve the symbolic links and create an absolute path*/
        if(!resolve_symlink_and_get_abs_path(AT_FDCWD, pathname, filebuff, MAX_ABS_PATH_LEN)){
                SPEEDYIO_FPRINTF("%s:ERROR when calling resolve_symlink_and_get_abs_path for pathname:%s\n", "SPEEDYIO_ERRCO_0011 %s\n", pathname);
                goto exit_open64;
        }else{
                debug_printf("%s: pathname:\"%s\" resolved to \"%s\"\n",
                                __func__, pathname, filebuff);
        }

        file.fd = fd;
        file.filename = filebuff;
        if(changed){
                flags = (flags & ~O_RDWR) | O_WRONLY;
        }
        file.flags = flags;
        handle_open(file);

exit_open64:
        return fd;
}

extern "C" __attribute__((visibility("default")))
int open(const char *pathname, int flags, ...){

        int fd;
        struct file_desc file;
        char filebuff[MAX_ABS_PATH_LEN];
        bool changed = false;

#ifdef ENABLE_MINCORE_DEBUG
        if(flags & O_WRONLY) {
                changed = true;
                flags = (flags & ~O_WRONLY) | O_RDWR;
        }
#endif //ENABLE_MINCORE_DEBUG

        if(flags & O_CREAT){
                va_list valist;
                va_start(valist, flags);
                mode_t mode = va_arg(valist, mode_t);
                va_end(valist);
                fd = real_open(pathname, flags, mode);
        }
        else{
                fd = real_open(pathname, flags, 0);
        }

        /**
         * Doing an extra check using path_is_dir here because we observed on
         * ARM (AWS r8g.4xlarge with RHEL9) , Cassandra/Java will do
         * open(dir, O_RDONLY) and expect a dir-fd. No O_DIRECTORY flag passed
         * hence this extra check.
         */

        // if(fd < 3 || (flags & O_DIRECTORY)){
        if (fd < 3 || (flags & O_DIRECTORY) || path_is_dir(pathname)) {
                goto exit_open;
        }

        debug_printf("%s: file:%s fd:%d\n", __func__,  pathname, fd);

        /*resolve the symbolic links and create an absolute path*/
        if(!resolve_symlink_and_get_abs_path(AT_FDCWD, pathname, filebuff, MAX_ABS_PATH_LEN)){
                SPEEDYIO_FPRINTF("%s:ERROR when calling resolve_symlink_and_get_abs_path for pathname:%s\n", "SPEEDYIO_ERRCO_0012 %s\n", pathname);
                goto exit_open;
        }else{
                debug_printf("%s: pathname:\"%s\" resolved to \"%s\"\n",
                                __func__, pathname, filebuff);
        }

        file.fd = fd;
        file.filename = filebuff;
        if(changed){
                flags = (flags & ~O_RDWR) | O_WRONLY;
        }
        file.flags = flags;
        handle_open(file);

exit_open:
        return fd;
}


#ifdef CHECK_FOR_FREAD_ERRORS
/**
 * fopen internally uses openat but the shim
 * library is not able to intercept it.
 * So we need to implement fopen handling
 */
extern "C" __attribute__((visibility("default")))
FILE *fopen(const char *pathname, const char *mode){
        FILE *fp;
        struct file_desc file;
        char filebuff[MAX_ABS_PATH_LEN];

        fp = real_fopen(pathname, mode);

        if(!fp){
                debug_printf("ERROR:%s file:%s\n", __func__, pathname);
                goto exit_fopen;
        }

        debug_printf("%s: file:%s fp:%p\n", __func__,  pathname, fp);

        if(!resolve_symlink_and_get_abs_path(AT_FDCWD, pathname, filebuff, MAX_ABS_PATH_LEN)){
                SPEEDYIO_FPRINTF("%s:ERROR when calling resolve_symlink_and_get_abs_path for pathname:%s\n", "SPEEDYIO_ERRCO_0013 %s\n", pathname);
                goto exit_fopen;
        }else{
                debug_printf("%s: pathname:\"%s\" resolved to \"%s\"\n",
                                __func__, pathname, filebuff);
        }

        if(is_whitelisted(filebuff)){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on whitelisted file:%s\n", "SPEEDYIO_NOTSUPPORTEDCO_0001 %s\n", filebuff);
                goto exit_fopen;
        }

        // file.fd = fileno(fp);
        // file.filename = pathname;
        // file.flags = 0;

        // handle_open(file);

exit_fopen:
        return fp;
}
#endif //CHECK_FOR_FREAD_ERRORS

extern "C" __attribute__((visibility("default")))
int creat(const char *pathname, mode_t mode){
        int fd;
        struct file_desc file;

        fd = real_creat(pathname, mode);

        if(fd < 0){
                SPEEDYIO_FPRINTF("%s:ERROR file:%s fd:%d\n", "SPEEDYIO_ERRCO_0014 %s %d\n", pathname, fd);
                goto exit_creat;
        }

        debug_printf("%s: file:%s fd:%d\n", __func__,  pathname, fd);

        file.fd = fd;
        file.filename = pathname;
        /*
         * This is the implementation of creat. check its man page
         * for more details.
         *
         * int creat(const char *path, mode_t mode){
         *      return open(path, O_WRONLY|O_CREAT|O_TRUNC, mode);
         * }
         */
        file.flags = O_WRONLY|O_CREAT|O_TRUNC;

        handle_open(file);

exit_creat:
        return fd;
}


//CLOSE SYSCALLS

/**
 * Does all the book keeping required at fd close.
 * Returns true if successful, else false
 */
bool handle_close(int fd){
        bool ret = true;
        int ret_remove_fd;
        bool unlinked = false;
        struct inode *uinode = nullptr;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        //enables per thread ds
        per_th_d.touchme = true;

        if(fd < 3){
                goto exit_handle_close;
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                //debug_printf("%s: did not find pfd for fd:%d\n", __func__, fd);
                //ret = false;
                goto exit_handle_close;
        }else{
                debug_printf("%s: fd:%d returned pfd addr:%p pfd->fd:%d\n",
                                __func__, fd, static_cast<void*>(pfd.get()), pfd->fd);
        }

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d != fd:%d\n", "SPEEDYIO_ERRCO_0015 %d %d\n", pfd->fd, fd);
                ret = false;
                KILLME();
                goto exit_handle_close;
        }

        /*for blacklisted files, just flip fd_open and exit*/
        if(pfd->is_blacklisted()){
                debug_printf("%s: pfd is_blacklisted for fd:%d\n", __func__, fd);
                pfd->fd_open = false;
                goto exit_handle_close;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR No uinode for whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0016 %d\n", fd);
                ret = false;
                goto exit_handle_close;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR Double close on fd:%d according to pfd\n", "SPEEDYIO_ERRCO_0017 %d\n", fd);
                /*Even though this is an error we still do further steps to keep uinode's sanity*/
        }

        // SPEEDYIO_PRINTF("%s:INFO closing fd:%d, {ino:%lu, dev:%lu}\n", "SPEEDYIO_INFOCO_0006 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);

        pfd->fd_open = false;
        /**
         * In a case where a non-regular file is opened
         * with the same fd as one previously used by a
         * (now closed) whitelisted file, handle_read
         * thinks it is a read-after-close error.
         *
         * To mitigate that without handling all kinds of files,
         * blacklisted is made true when a whitelisted fd is closed.
         * It doesnt make any difference while reusing this pfd,
         * just mitigates this false error.
         */
        pfd->blacklisted = true;
        ret_remove_fd = remove_fd_from_fdlist(uinode, fd);

        if(ret_remove_fd == 0){
                SPEEDYIO_FPRINTF("%s:ERROR unable to find fd:%d in {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0018 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                ret = false;
                goto exit_handle_close;
        }

        if(ret_remove_fd == -1){
                SPEEDYIO_FPRINTF("%s:INCORRECT_INPUT to remove_fd_from_fdlist fd:%d and {ino:%lu, dev:%lu}\n", "SPEEDYIO_OTHERCO_0001 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                ret = false;
                goto exit_handle_close;
        }

        /**
         * XXX: If this fd was the last fd for the uinode, we should
         * do something about its cache state and heap state
         * Maybe call a flush on it before closing it.
         */

        /**
         * If this fd was the last and the file was marked_unlinked
         * then set unlinked on this uinode.
         */
        unlinked = uinode->check_fdlist_and_unlink();
        // debug_printf("%s:check_fdlist_and_unlink fd:%d unlinked:%s\n",
        //                 __func__, fd, unlinked ? "true" : "false");

#ifdef ENABLE_EVICTION
        if(unlinked){
                /*perform gheap removal*/
                remove_from_g_heap(uinode);
        }
#endif

#endif

exit_handle_close:
        return ret;
}

#ifdef CHECK_FOR_FREAD_ERRORS
/**
 * if fclose is used for whitelisted file, throw error
 */
extern "C" __attribute__((visibility("default")))
int fclose(FILE *stream){
        int ret;
        int handle_close_ret;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        int fd = fileno(stream);

        debug_printf("Entering %s\n", __func__);
        ret = real_fclose(stream);
        if(ret == 0){

                pfd = get_perfd_struct_fast(fd);
                if(!pfd){
                        goto exit_fclose;
                }

                if(!pfd->is_blacklisted()){
                        SPEEDYIO_FPRINTF("%s:NOTSUPPORTED called for whitelisted fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0002 %d\n", fd);
                        KILLME();
                        goto exit_fclose;
                }

                // handle_close_ret = handle_close(fd);
                // if(!handle_close_ret){
                //         fprintf(stderr, "%s:ERROR with handle_close fd:%d\n", __func__, fd);
                // }
        }

exit_fclose:
        return ret;
}
#endif //CHECK_FOR_FREAD_ERRORS

extern "C" __attribute__((visibility("default")))
int close(int fd){
        int ret;
        bool handle_close_ret;

        debug_printf("%s: called for fd:%d\n", __func__, fd);

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        //enables per thread ds
        per_th_d.touchme = true;
        bool locked = false;
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;

        if(nr_reads < 20){
                goto serve_req;
        }

        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0019 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0020 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0021 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();

        locked = true;
#endif

serve_req:

        ret = real_close(fd);
        if(ret == 0){
                /**
                 * we handle close after real_close success because
                 * we are only using the fd for internal book keeping update
                 * we are not doing any syscalls using said fd.
                 */
                handle_close_ret = handle_close(fd);
                if(!handle_close_ret){
                        SPEEDYIO_FPRINTF("%s:ERROR with handle_close fd:%d\n", "SPEEDYIO_ERRCO_0022 %d\n", fd);
                }
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif

exit_close:
        return ret;
}


//UNLINK SYSCALLS

/**
 * handles all the unlink stuff for blacklisted and whitelisted files
 * returns true if successful, else returns false
 */
std::mutex *handle_unlink(int dirfd, const char* pathname, int unlink_flags){
        bool ret = true;
        bool unlinked = false;
        struct stat file_stat;
        struct inode *uinode = nullptr;
        struct value *val = nullptr;
        std::mutex *lock_ret = nullptr;

        debug_printf("%s: dirfd:%d, path:%s, flags:%d\n", __func__, dirfd, pathname, unlink_flags);

        /*If unlinking directory, dont do anything*/
        if(unlink_flags & AT_REMOVEDIR){
                SPEEDYIO_FPRINTF("%s: unlinking directory path:%s dirfd:%d\n", "SPEEDYIO_OTHERCO_0002 %s %d\n", pathname, dirfd);
                /**
                 * XXX: What if there are whitelisted files in the directory that is being deleted ?
                 * We havent handled that here yet.
                 */
                goto exit_handle_unlink;
        }

#if defined(MAINTAIN_INODE) && defined(PER_FD_DS)

        /**
         * Currently we ignore if unlink is called on a blacklisted path.
         * This is fine since Cassandra 3:
         * 1. Does not use 'link' systemcall (as we checked) to create a hardlink
         * to a blacklisted pathname with a whitelisted pathname.
         * 2. Does not use 'rename' syscall to rename a temp/blacklisted pathname to
         * a whitelisted pathname.
         *
         * If the above two conditions are not true, we cannot ignore blacklisted
         * pathnames. We will have to check their i_map residence using {ino, dev_id}
         * before ignoring them.
         */
        if(!is_whitelisted(pathname)){
                debug_printf("%s: ignoring blacklisted file:%s\n", __func__, pathname);
                goto exit_handle_unlink;
        }

        /**
         * unlinks on symbolic links do not unlink the target file.
         * unlinks on hard links only reduce the st_nlinks(reference count to the file)
         *
         * uinode will be maked unlinked if and only if:
         * 1. st_nlinks == 1 and
         * 2. there are no open fds in the fdlist and
         * 3. unlink has been called for the file
         *
         * Since we resolve symbolic links to their respective target files
         * during open, there could be lingering fds in that uinode if the
         * symlink file's fds were closed implictly, but that would be handled
         * as an implicit close automatically anyway.
         */

        /**
         * Fstat tells us three things:
         * 1. If the file exists - it fails otherwise.
         * 2. If the file is a regular file or not (S_ISREG)
         * 3. The nr_links to this file. (st_nlink)
         */
        if(fstatat(dirfd, pathname, &file_stat, AT_SYMLINK_NOFOLLOW) == -1) {
                debug_printf("%s:WARNING unable to fstatat dirfd:%d, path:%s error:%s\n",
                                __func__, dirfd, pathname, strerror(errno));
                goto exit_handle_unlink;
        }

        if(!S_ISREG(file_stat.st_mode)){
                /*this file is not a regular file. skip it*/
                debug_printf("%s: this file:%s is not a regular file. Ignoring it\n", __func__, pathname);
                goto exit_handle_unlink;
        }

        val = get_from_hashtable(file_stat.st_ino, file_stat.st_dev);
        if(!val){
                goto exit_handle_unlink;
        }
        uinode = (struct inode*)val->value;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR Could not find uinode for whitelisted path:%s\n", "SPEEDYIO_ERRCO_0023 %s\n", pathname);
                ret = false;
                goto exit_handle_unlink;
        }

        /*Add inode and dev check here*/

        if(uinode->ino != file_stat.st_ino || uinode->dev_id != file_stat.st_dev){
                SPEEDYIO_FPRINTF("%s:ERROR uinode->{ino:%lu, dev:%lu} != {st_ino:%lu, st_dev:%lu} for whitelisted path:%s\n", "SPEEDYIO_ERRCO_0024 %lu %lu %lu %lu %s\n", uinode->ino, uinode->dev_id, file_stat.st_ino, file_stat.st_dev, pathname);
                ret = false;
                goto exit_handle_unlink;
        }

        /*update the number of links left in the system for this inode*/
        if(!update_nr_links(uinode, file_stat.st_nlink, true)){
                SPEEDYIO_FPRINTF("%s:ERROR unable to update nr_links for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0025 %lu %lu\n", uinode->ino, uinode->dev_id);
                ret = false;
                goto exit_handle_unlink;
        }

#ifdef ENABLE_UINODE_LOCK
        uinode->uinode_lock.lock(); //deletes should not happen in the middle of an eviction event
#endif //ENABLE_UINODE_LOCK

        uinode->unlinked_lock.lock();

        if(unlikely(uinode->unlinked)){
                SPEEDYIO_FPRINTF("%s:ERROR unlinking file:%s already has uinode->unlinked==true\n", "SPEEDYIO_ERRCO_0026 %s\n", pathname);
        }

        /**
         * Since Cassandra 3 doesn't use link systemcall (and we dont do uinode
         * allocations based on it; and we also produce a warning if link is
         * being called for paths both of which are not whitelisted)
         *
         * It is safe to assume at this point that if there are multiple links
         * to a given {ino, dev_id}, both of them have paths that we consider
         * whitelisted.
         *
         * 'marked_unlinked' conveys that it is only the remaining open fds to
         * this {ino, dev_id} that is saving it from being actually deleted from
         * the filesystem.
         *
         * Therefore, we should only set marked_unlinked for a uinode if there only
         * remains one hardlink for it.
         */
        if(uinode->nr_links == 1){
                uinode->marked_unlinked = true;
        }else{
                /**
                 * Check if the pathname being unlinked right now is the same as what
                 * we have in the uinode->filename.
                 * If yes, we wont be able to use this filename later for anything.
                 * TODO: See how this can be fixed. Not high priority
                 */
                if(same_pathnames(uinode->filename, dirfd, pathname) == true){
                        cfprintf(stderr, "%s: WARNING this uinode->filename:%s "
                                "will not be available anymore for {ino:%lu, dev:%lu}. "
                                "nr_links:%lu. Skipping marked_unlinked\n",
                                __func__, uinode->filename, uinode->ino, uinode->dev_id, uinode->nr_links);

                }else{
                        cfprintf(stderr, "%s:WARNING {ino:%lu, dev:%lu} path:%s has nr_links:%lu and is being unlinked. Skipping marked_unlinked\n",
                                __func__, uinode->ino, uinode->dev_id, uinode->filename, uinode->nr_links);
                }
        }

        uinode->unlinked_lock.unlock();

        /**
         * BUG: potential race condition. We are unlocking unlinked_lock
         * before getting it again in check_fdlist_and_unlink.
         * Check and fix this.
         */

        unlinked = uinode->check_fdlist_and_unlink();

#ifdef ENABLE_EVICTION
        if(unlinked){
                /*perform gheap removal*/
                remove_from_g_heap(uinode);

#ifdef ENABLE_BG_INODE_CLEANER
                /*accounting nr_unlinks to trigger bg_inode_cleaner*/
                nr_unlinks_for_imap_cleanup->now += 1;
#endif //ENABLE_BG_INODE_CLEANER
        }
#endif

        debug_printf("%s:INFO check_fdlist_and_unlink path:%s unlinked:%s\n",
                        __func__, pathname, unlinked ? "true" : "false");

#endif //MAINTAIN_INODE && PER_FD_DS

exit_handle_unlink:
#ifdef ENABLE_UINODE_LOCK
        if(uinode && ret){
                lock_ret = &uinode->uinode_lock;
        }else{
                lock_ret = nullptr;
        }
#endif //ENABLE_UINODE_LOCK
        return lock_ret;
}

extern "C" __attribute__((visibility("default")))
int unlink(const char* pathname){
        int ret;
        std::mutex *lock_ret = nullptr;

        debug_printf("%s: path:%s\n", __func__, pathname);

        /**
         * we do unlink only after handle_unlink because the pathname will
         * be used to get inode number etc from the OS.
         */
        lock_ret = handle_unlink(AT_FDCWD, pathname, 0);
        // if(!lock_ret){
        // SPEEDYIO_FPRINTF("%s:ERROR unable to handle_unlink for path:%s\n", "SPEEDYIO_ERRCO_0027 %s\n", pathname);
        // }

        ret = real_unlink(pathname);

        if(lock_ret){
                lock_ret->unlock();
        }

exit_unlink:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int unlinkat(int dirfd, const char* pathname, int flags){
        int ret;
        std::mutex *lock_ret = nullptr;

        debug_printf("%s: path:%s\n", __func__, pathname);

        lock_ret = handle_unlink(dirfd, pathname, flags); 
        // if(!lock_ret){
        // SPEEDYIO_FPRINTF("%s:ERROR unable to handle_unlink for path:%s\n", "SPEEDYIO_ERRCO_0028 %s\n", pathname);
        // }

        ret = real_unlinkat(dirfd, pathname, flags);

        if(lock_ret){
                lock_ret->unlock();
        }
exit_unlinkat:
        return ret;
}


//DUP SYSCALLS

/*returns 0 if there is an error*/
int handle_dup(int oldfd, int newfd, int flags){
        int ret = 1;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        /**
         * XXX:TODO
         * READ THE MAN PAGE OF DUP, DUP2, DUP3 BEFORE IMPLEMENTING THIS
         * 1. Check if there exists an active pfd (not closed) for oldfd
         * 2. Check if there exists an active pfd for newfd
         * 3. Check if flags have something like CLOEXEC for whitelisted files
         * 4. Duplicate pfd from oldfd to newfd differently
         * for whitelisted and blacklisted files
         *
         * Currently this impl. only throws NOTSUPPORTED error if dup is called for
         * a whitelisted fd.
         */

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)

        pfd = get_perfd_struct_fast(oldfd);
        if(!pfd){
                goto exit_handle_dup;
        }

        if(!pfd->is_blacklisted()){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED dup/dup2/dup3 called for whitelisted fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0003 %d\n", oldfd);
                ret = 0;
                KILLME();
                goto exit_handle_dup;
        }
#endif //PER_FD_DS && MAINTAIN_INODE

exit_handle_dup:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int dup(int oldfd){
        int ret;

        debug_printf("%s:CALLED for oldfd:%d\n", __func__, oldfd);

        ret = real_dup(oldfd);
        if(ret == -1){
                goto exit_dup;
        }

        /*since no flags are given with dup, we just use 0 instead*/
        if(!handle_dup(oldfd, ret, 0)){
                SPEEDYIO_FPRINTF("%s:ERROR in handling dup oldfd:%d, newfd:%d\n", "SPEEDYIO_ERRCO_0029 %d %d\n", oldfd, ret);
                goto exit_dup;
        }

exit_dup:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int dup2(int oldfd, int newfd){
        int ret;

        debug_printf("%s:CALLED for oldfd:%d, newfd:%d\n", __func__, oldfd, newfd);

        ret = real_dup2(oldfd, newfd);
        if(ret == -1){
                goto exit_dup2;
        }

        /*since no flags are given with dup2, we just use 0 instead*/
        if(!handle_dup(oldfd, ret, 0)){
                SPEEDYIO_FPRINTF("%s:ERROR in handling dup oldfd:%d, newfd:%d\n", "SPEEDYIO_ERRCO_0030 %d %d\n", oldfd, ret);
                goto exit_dup2;
        }

exit_dup2:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int dup3(int oldfd, int newfd, int flags){
        int ret;

        debug_printf("%s:CALLED for oldfd:%d, newfd:%d, flags:%d\n", __func__, oldfd, newfd, flags);

        ret = real_dup3(oldfd, newfd, flags);
        if(ret == -1){
                goto exit_dup3;
        }

        if(!handle_dup(oldfd, ret, flags)){
                SPEEDYIO_FPRINTF("%s:ERROR in handling dup oldfd:%d, newfd:%d with flags:%d\n", "SPEEDYIO_ERRCO_0031 %d %d %d\n", oldfd, ret, flags);
                goto exit_dup3;
        }

exit_dup3:
        return ret;
}


//READ SYSCALLS

void handle_read(int fd, off_t offset, size_t size, bool offset_absent){

        struct timespec start, end;
        struct timespec get_pfd_start, get_pfd_end;
        bool clockit = true; //Do for all files.
        clock_gettime(CLOCK_MONOTONIC, &start);

        struct inode *uinode = nullptr;
        struct thread_args *arg = nullptr;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        int pid, tid;
        std::string event_string;

        //enables per thread ds
        per_th_d.touchme = true;


#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        clock_gettime(CLOCK_MONOTONIC, &get_pfd_start);

        pfd = get_perfd_struct_fast(fd);

        clock_gettime(CLOCK_MONOTONIC, &get_pfd_end);
        bin_time_to_pow2_us(get_pfd_start, get_pfd_end, &get_pfd_latency);

        if(!pfd)
                goto handle_read_exit;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0032 %d %d\n", fd, pfd->fd);
                KILLME();
                goto handle_read_exit;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto handle_read_exit;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0033 %d\n", fd);
                KILLME();
                goto handle_read_exit;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto handle_read_exit;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0034 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto handle_read_exit;
        }

#ifdef DBG_ONLY_GET_PFD
        goto handle_read_exit;
#endif //DBG_ONLY_GET_PFD

        clockit = true;

        /*offset is absent for read syscall where OS/glibc maintains it*/
        if(offset_absent){
                offset = update_fd_seek_pos(uinode, fd, size, false);
                if(offset == -1){
                        SPEEDYIO_FPRINTF("%s:ERROR while doing update_fd_seek_pos fd:%d {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0035 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                        KILLME();
                        goto handle_read_exit;
                }
        }

        // SPEEDYIO_PRINTF("%s:INFO fd:%d, {ino:%lu, dev:%lu} offset:%ld size:%ld offset_absent:%s\n", "SPEEDYIO_INFOCO_0007 %d %lu %lu %ld %ld %s\n", fd, uinode->ino, uinode->dev_id, offset, size, offset_absent ? "true" : "false");

#ifdef PRINT_READ_EVENTS
        pid = getpid(), tid = gettid();
        event_string = "READ_EVENT," + std::to_string(pid) + "," + std::to_string(tid) + "," +
        std::to_string((unsigned long)uinode->ino) + "," + std::to_string(__rdtsc()) + "," +
        std::to_string(offset) + "," + std::to_string(size) + "\n";
        // std::cout << event_string << std::endl;
        log_event_to_file(per_th_d.read_events_fd, event_string);
#endif // PRINT_READ_EVENTS

        /*check if the file is not being read beyond MAX_FILE_SIZE_BYTES*/
        if(offset+size >= MAX_FILE_SIZE_BYTES){
                SPEEDYIO_FPRINTF("%s:MISCONFIG file offset %ld >= MAX_FILE_SIZE_BYTES for fd:%d, {ino:%lu, dev:%lu}\n", "SPEEDYIO_MISCONFIGCO_0001 %ld %d %lu %lu\n", offset+size, fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto handle_read_exit;
        }

#ifdef ENABLE_PER_INODE_BITMAP

        set_range_bitmap(uinode, PG_NR_FROM_OFFSET(offset), BYTES_TO_PG(size));

#endif //ENABLE_PER_INODE_BITMAP

#ifdef ENABLE_EVICTION
        heap_update(uinode, offset, size, true); //handles both global and pvt heaps
#endif //ENABLE_EVICTION

#endif //PER_FD_DS, MAINTAIN_INODE

handle_read_exit:
        clock_gettime(CLOCK_MONOTONIC, &end);
        // if(clockit) bin_ns_to_pow2_us(timespec_diff_ns(start, end));
        if(clockit) bin_time_to_pow2_us(start, end, &handle_read_latency);

        return;
}

extern "C" __attribute__((visibility("default")))
ssize_t pread64(int fd, void *data, size_t size, off_t offset){
        ssize_t amount_read;
        struct timespec start, end;


#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        //enables per thread ds
        per_th_d.touchme = true;
        bool locked = false;
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;

        if(nr_reads < 20){
                goto serve_req;
        }

        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0036 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0037 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0038 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();

        locked = true;
#endif

serve_req:
        clock_gettime(CLOCK_MONOTONIC, &start);
        amount_read = real_pread64(fd, data, size, offset);
        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &readsyscalls_latency);

        if(amount_read > 0 && fd >= 3){
                handle_read(fd, offset, size, false);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif
        return amount_read;
}

extern "C" __attribute__((visibility("default")))
ssize_t pread(int fd, void *data, size_t size, off_t offset){

        ssize_t amount_read;
        struct timespec start, end;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        bool locked = false;
        per_th_d.touchme = true;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;
        
        if(nr_reads < 20){
                goto serve_req;
        }
        
        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0039 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0040 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0041 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();
        locked = true;
#endif

serve_req:
        clock_gettime(CLOCK_MONOTONIC, &start);
        amount_read = real_pread(fd, data, size, offset);
        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &readsyscalls_latency);

        if(amount_read > 0 && fd >= 3){
                // debug_printf("%s: fd:%d, offset:%ld, size:%ld amt_read:%ld\n",
                //                 __func__, fd, offset, size, amount_read);
                handle_read(fd, offset, size, false);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif

exit_pread:
        return amount_read;
}

extern "C" __attribute__((visibility("default")))
ssize_t read(int fd, void *data, size_t size){
        ssize_t amount_read;
        struct stat file_stat;
        struct timespec start, end;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        per_th_d.touchme = true;
        bool locked = false;

        if(fd < 3){
                goto serve_req;
        }
        
        nr_reads += 1;
        
        if(nr_reads < 20){
                goto serve_req;
        }

        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0042 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0043 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0044 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();
        locked = true;
#endif

serve_req:
        clock_gettime(CLOCK_MONOTONIC, &start);
        amount_read = real_read(fd, data, size);
        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &readsyscalls_latency);

#ifdef DEBUG
        if(amount_read < size){
                if(fstat(fd, &file_stat) == -1){
                        SPEEDYIO_FPRINTF("%s:ERROR unable to fstat\n", "SPEEDYIO_ERRCO_0045\n");
                }
                SPEEDYIO_PRINTF("%s:NOTE Inode: %lu\n", "SPEEDYIO_NOTECO_0001 %lu\n", file_stat.st_ino);
                SPEEDYIO_PRINTF("%s:NOTE Device ID: %lu\n", "SPEEDYIO_NOTECO_0002 %lu\n", file_stat.st_dev);
                SPEEDYIO_PRINTF("%s:NOTE File size: %ld bytes\n", "SPEEDYIO_NOTECO_0003 %ld\n", file_stat.st_size);
                SPEEDYIO_PRINTF("%s:NOTE Number of hard links: %lu\n", "SPEEDYIO_NOTECO_0004 %lu\n", file_stat.st_nlink);
                SPEEDYIO_PRINTF("%s:NOTE Last modified: %ld\n", "SPEEDYIO_NOTECO_0005 %ld\n", file_stat.st_mtime);
                if(S_ISREG(file_stat.st_mode)){
                        SPEEDYIO_PRINTF("%s:NOTE Type: Regular file fd:%d\n", "SPEEDYIO_NOTECO_0006 %d\n", fd);
                }else{
                        SPEEDYIO_PRINTF("%s:NOTE fd:%d NOT A REGULAR FILE\n", "SPEEDYIO_NOTECO_0007 %d\n", fd);
                }
        }
#endif

        if(amount_read > 0 && fd >= 3){
                /**
                 *
                 * We are not using amount_read instead because of the difference in its
                 * type ssize_t vs input size in size_t.
                 *
                 * XXX: Check all places where types are being used interchangably.
                 *
                 * It doesnt really affect our bookkeeping because:
                 * 1. amount_read is -ve when file cannot be read
                 * 2. amount_read is less than size if we hit end of file
                 * 3. even if we do our book keeping with size instead of amount_read,
                 * it doesnt really change much of our prefetching/eviction
                 * effectiveness because it about the end of file, one incorrect page/portion.
                 */
                handle_read(fd, 0, size, true);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif

exit_read:
        return amount_read;
}


#ifdef CHECK_FOR_FREAD_ERRORS
/**
 * Just throws an error if used for whitelisted file
 *
 * XXX: We dont mainline fread because each ftell takes ~120ns.
 * This would slow down the DB applications.
 * Since rocksdb and cassandra dont fread, we can put it for
 * later.
 */
extern "C" __attribute__((visibility("default")))
size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream){
        size_t amount_read;
        int fd;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;


        amount_read = real_fread(ptr, size, nmemb, stream);
        if(amount_read <= 0){
                goto exit_fread;
        }

        fd = fileno(stream);
        if(fd == -1){
                SPEEDYIO_FPRINTF("%s:ERROR when doing fileno\n", "SPEEDYIO_ERRCO_0046\n");
                goto exit_fread;
        }

        if(fd < 3)
                goto exit_fread;

        pfd = get_perfd_struct_fast(fd);
        if(!pfd){
                goto exit_fread;
        }

        if(!pfd->is_blacklisted()){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED called by whitelisted fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0004 %d\n", fd);
                KILLME();
                goto exit_fread;
        }

exit_fread:
        return amount_read;
}
#endif //CHECK_FOR_FREAD_ERRORS


//FSYNC

extern "C" __attribute__((visibility("default")))
int fsync(int fd){
        int ret = -1;

        ret = real_fsync(fd);

exit_fsync:
        return ret;
}


extern "C" __attribute__((visibility("default")))
int fdatasync(int fd){
        int ret = -1;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        struct inode *uinode =  nullptr;

do_real_fdatasync:
        ret = real_fdatasync(fd);

exit_fsync:
        return ret;
}


//WRITE SYSCALLS

void handle_write(int fd, off_t offset, ssize_t size, bool offset_absent){

        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        int pid, tid;
        std::string event_string;
        struct inode *uinode = nullptr;

        if(fd < 3){
                goto handle_write_exit;
        }

        //debug_printf("%s: fd:%d size of write:%d bytes\n", __func__, fd, size);

        //enables per thread ds
        per_th_d.touchme = true;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        pfd = get_perfd_struct_fast(fd);

        /* This means the file was probably not in the whitelist*/
        if(!pfd){
                goto handle_write_exit;
        }

        if(pfd->fd != fd){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d doesnt match fd:%d\n", "SPEEDYIO_ERRCO_0047 %d %d\n", pfd->fd, fd);
                goto handle_write_exit;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto handle_write_exit;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:WARNING fd:%d is closed. Skipping\n", "SPEEDYIO_WARNCO_0001 %d\n", fd);
                goto handle_write_exit;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode for this whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0048 %d\n", fd);
                goto handle_write_exit;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0049 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                goto handle_write_exit;
        }

        //debug_printf("%s: fd:%d size of write:%d bytes\n", __func__, fd, size);

        if(offset_absent){
                offset = update_fd_seek_pos(uinode, fd, size, false);
                if(offset < 0){
                        SPEEDYIO_FPRINTF("%s:ERROR update_fd_seek_pos returned error for fd:%d {ino:%lu, dev:%lu} size:%ld\n", "SPEEDYIO_ERRCO_0050 %d %lu %lu %ld\n", fd, uinode->ino, uinode->dev_id, size);
                        KILLME();
                        goto handle_write_exit;
                }
        }

        // SPEEDYIO_PRINTF("%s:INFO fd:%d, {ino:%lu, dev:%lu} offset:%ld size:%ld offset_absent:%s\n", "SPEEDYIO_INFOCO_0008 %d %lu %lu %ld %ld %s\n", fd, uinode->ino, uinode->dev_id, offset, size, offset_absent ? "true" : "false");

        /*check if the file is not being written beyond MAX_FILE_SIZE_BYTES*/
        if(offset+size >= MAX_FILE_SIZE_BYTES){
                SPEEDYIO_FPRINTF("%s:MISCONFIG file offset %ld >= MAX_FILE_SIZE_BYTES for fd:%d, {ino:%lu, dev:%lu}\n", "SPEEDYIO_MISCONFIGCO_0002 %ld %d %lu %lu\n", offset+size, fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto handle_write_exit;
        }

#ifdef PRINT_WRITE_EVENTS
        pid = getpid(), tid = gettid();
        event_string = "WRITE_EVENT," + std::to_string(pid) + "," + std::to_string(tid) + "," +
                        std::to_string((unsigned long)uinode->ino) + "," + std::to_string(__rdtsc()) + "," +
                        std::to_string(offset) + "," + std::to_string(size) + "\n";
        // std::cout << event_string << std::endl;
        log_event_to_file(per_th_d.write_events_fd, event_string);
#endif // PRINT_WRITE_EVENTS

#ifdef ENABLE_PER_INODE_BITMAP
        //update bitmap
        set_range_bitmap(uinode, PG_NR_FROM_OFFSET(offset), BYTES_TO_PG(size));
#endif //ENABLE_PER_INODE_BITMAP

#ifdef SYNC_WRITES
        sync_file_range(fd, offset, size, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER);
#endif //SYNC_WRITES

#ifdef DONT_NEED_WRITES
        real_posix_fadvise(fd, offset, size, POSIX_FADV_DONTNEED);
        goto handle_write_exit;
#endif

#if defined(ENABLE_EVICTION)// && !defined(ENABLE_FADV_ON_FDATASYNC)
        heap_update(uinode, offset, size, false);
#endif //ENABLE_EVICTION

#endif //PER_FD_DS, MAINTAIN_INODE

handle_write_exit:
        return;
}

extern "C" __attribute__((visibility("default")))
ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset){

        ssize_t amount_written = 0;
#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        bool locked = false;
        per_th_d.touchme = true;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;
        
        if(nr_reads < 20){
                goto serve_req;
        }
        
        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0051 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0052 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0053 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();
        locked = true;
#endif

serve_req:
        amount_written = real_pwrite64(fd, buf, count, offset);

        if(amount_written > 0 && fd >= 3){
                handle_write(fd, offset, amount_written, false);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif
exit_pwrite64:
        return amount_written;
}

extern "C" __attribute__((visibility("default")))
ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset){

        ssize_t amount_written = 0;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        bool locked = false;
        per_th_d.touchme = true;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;
        
        if(nr_reads < 20){
                goto serve_req;
        }
        
        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0054 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0055 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0056 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();
        locked = true;
#endif

serve_req:
        amount_written = real_pwrite(fd, buf, count, offset);

        if(amount_written > 0 && fd >= 3){
                handle_write(fd, offset, amount_written, false);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif
exit_pwrite:
        return amount_written;
}

extern "C" __attribute__((visibility("default")))
ssize_t write(int fd, const void *buf, size_t count){

        ssize_t amount_written = 0;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        struct inode *uinode =  nullptr;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        bool locked = false;
        per_th_d.touchme = true;

        if(fd < 3){
                goto serve_req;
        }

        nr_reads += 1;
        
        if(nr_reads < 20){
                goto serve_req;
        }
        
        pfd = get_perfd_struct_fast(fd);

        if(!pfd)
                goto serve_req;

        if(unlikely(pfd->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d pfd->fd:%d dont match\n", "SPEEDYIO_ERRCO_0057 %d %d\n", fd, pfd->fd);
                KILLME();
                goto serve_req;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto serve_req;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d is closed.\n", "SPEEDYIO_ERRCO_0058 %d\n", fd);
                KILLME();
                goto serve_req;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto serve_req;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0059 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto serve_req;
        }

        uinode->uinode_lock.lock();
        locked = true;
#endif

serve_req:

        amount_written = real_write(fd, buf, count);

        //debug_printf("%s:INFO fd:%d, count:%ld, amount_written:%ld\n", __func__, fd, count, amount_written);

        if(amount_written > 0 && fd >= 3){
                handle_write(fd, 0, amount_written, true);
        }

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE) && defined(ENABLE_UINODE_LOCK)
        if(locked){
                uinode->uinode_lock.unlock();
        }
#endif
exit_write:
        return amount_written;
}


#ifdef CHECK_FOR_FREAD_ERRORS
/*NOT IMPLEMENTED. Just returns an error if fwrite is used on a whitelisted file*/
extern "C" __attribute__((visibility("default")))
size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream){
        size_t ret;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;


        ret = real_fwrite(ptr, size, nmemb, stream);

        if(ret <= 0){
                goto exit_fwrite;
        }

        pfd = get_perfd_struct_fast(fileno(stream));
        if(!pfd){
                goto exit_fwrite;
        }

        if(!pfd->is_blacklisted()){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED called by whitelisted fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0005 %d\n", fileno(stream));
                KILLME();
                goto exit_fwrite;
        }

exit_fwrite:
        return ret;
}
#endif //CHECK_FOR_FREAD_ERRORS

extern "C" __attribute__((visibility("default")))
int truncate(const char *path, off_t length){
        int ret;
        struct stat file_stat;
        char filebuff[MAX_ABS_PATH_LEN];

        /*resolve the symbolic links and create an absolute path*/
        if(!resolve_symlink_and_get_abs_path(AT_FDCWD, path, filebuff, MAX_ABS_PATH_LEN)){
                SPEEDYIO_FPRINTF("%s:ERROR when calling resolve_symlink_and_get_abs_path for pathname:%s\n", "SPEEDYIO_ERRCO_0060 %s\n", path);
                goto exit_truncate;
        }else{
                debug_printf("%s: pathname:\"%s\" resolved to \"%s\"\n",
                                __func__, path, filebuff);
        }

        if(!is_whitelisted(filebuff)){
                debug_printf("%s: called on BLACKLISTED file:%s\n", __func__, filebuff);
                goto do_real_truncate;
        }

        if(fstatat(AT_FDCWD, filebuff, &file_stat, AT_SYMLINK_NOFOLLOW) == -1) {
                debug_printf("%s:WARNING unable to fstatat dirfd:%d, path:%s error:%s\n",
                                __func__, AT_FDCWD, filebuff, strerror(errno));
                goto do_real_truncate;
        }

        debug_printf("%s:INFO called for WHITELISTED file:%s for length:%ld bytes, current size:%lld bytes\n",
                __func__, filebuff, length, (long long)file_stat.st_size);

        /**
         * If the size of file is being increased, we need not do anything.
         *
         * TODO: If size of file is being reduced, we need to remove corresponding
         * elements from heap and bitmap and adjust the seek_heads for all the fds
         * if need be.
         */
        if(length < file_stat.st_size){
                /*reducing size of a whitelisted file*/
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED whitelisted file:%s is being reduced from %lld to %ld bytes\n", "SPEEDYIO_NOTSUPPORTEDCO_0006 %s %lld %ld\n", filebuff, (long long)file_stat.st_size, length);
                KILLME();
        }

do_real_truncate:
        ret = real_truncate(path, length);

        if(ret == -1){
                goto exit_truncate;
        }

exit_truncate:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int ftruncate(int fd, off_t length){
        int ret = 0;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        struct stat file_stat;
        int err;
        err = -1;

        if(fd < 3){
                goto do_real_ftruncate;
        }

        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                goto exit_ftruncate;
        }

        if(pfd->is_blacklisted()){
                debug_printf("%s: called on BLACKLISTED fd:%d\n", __func__, fd);
                goto do_real_ftruncate;
        }

        err = fstat(fd, &file_stat);
        if(unlikely(err == -1)){
                SPEEDYIO_FPRINTF("%s:ERROR when fstat(%s) called for fd:%d\n", "SPEEDYIO_ERRCO_0061 %s %d\n", strerror(errno), fd);
                goto do_real_ftruncate;
        }

        debug_printf("%s:INFO called for WHITELISTED fd:%d for length:%ld bytes, current size:%lld bytes\n",
                        __func__, fd, length, (long long)file_stat.st_size);

        /**
         * If the size of file is being increased, we need not do anything.
         *
         * TODO: If size of file is being reduced, we need to remove corresponding
         * elements from heap and bitmap and adjust the seek_heads for all the fds
         * if need be.
         */
        if(length < file_stat.st_size){
                /*reducing size of a whitelisted file*/
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED whitelisted fd:%d is being reduced from %lld to %ld bytes\n", "SPEEDYIO_NOTSUPPORTEDCO_0007 %d %lld %ld\n", fd, (long long)file_stat.st_size, length);

                KILLME();
        }

do_real_ftruncate:
        ret = real_ftruncate(fd, length);

        if(ret == -1){
                goto exit_ftruncate;
        }

exit_ftruncate:
        return ret;
}


/*SEEK FUNCTIONS*/

void handle_lseek(int fd, off_t offset, int whence, off_t seek_ret){
        struct inode *uinode = nullptr;
        struct thread_args *arg = nullptr;
        ssize_t old_offset = -1;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        //enables per thread ds
        per_th_d.touchme = true;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                goto exit_handle_lseek;
        }

        if(pfd->is_blacklisted()){
                //debug_printf("%s: fd:%d is blacklisted. Skipping\n", __func__, fd);
                goto exit_handle_lseek;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:ERROR whitelisted fd:%d is closed.\n", "SPEEDYIO_ERRCO_0062 %d\n", fd);
                goto exit_handle_lseek;
        }

        uinode = pfd->uinode;
        if(unlikely(!uinode)){
                goto exit_handle_lseek;
        }

        /*Can we assume that files unlinked will not be read ?*/
        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0063 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                goto exit_handle_lseek;
        }

        /*update the seek_head for this whitelisted fd*/
        old_offset = update_fd_seek_pos(uinode, fd, seek_ret, true);
        if(old_offset < 0){
                SPEEDYIO_FPRINTF("%s:ERROR while update_fd_seek_pos fd:%d {ino:%lu, dev:%lu} ret:%ld\n", "SPEEDYIO_ERRCO_0064 %d %lu %lu %ld\n", fd, uinode->ino, uinode->dev_id, old_offset);
                KILLME();
                goto exit_handle_lseek;
        }

        /*check if the file is not being seeked beyond MAX_FILE_SIZE_BYTES*/
        if(seek_ret >= MAX_FILE_SIZE_BYTES){
                SPEEDYIO_FPRINTF("%s:MISCONFIG seeking to:%ld >= MAX_FILE_SIZE_BYTES\n", "SPEEDYIO_MISCONFIGCO_0003 %ld\n", seek_ret);
                KILLME();
                goto exit_handle_lseek;
        }

#endif //PER_FD_DS, MAINTAIN_INODE

exit_handle_lseek:
        return;
}

extern "C" __attribute__((visibility("default")))
off64_t lseek64(int fd, off64_t offset, int whence){
        /*doing this is fine on 64 bit machines*/
        return lseek(fd, offset, whence);
}

extern "C" __attribute__((visibility("default")))
off_t lseek(int fd, off_t offset, int whence){
        off_t ret;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;

        //debug_printf("%s: fd:%d, offset:%ld, whence:%d\n", __func__, fd, offset, whence);

        ret = real_lseek(fd, offset, whence);

        if(ret == -1 || fd < 3){
                goto exit_lseek;
        }

        /*we just update anything that is done with lseek*/
        handle_lseek(fd, offset, whence, ret);

exit_lseek:
        return ret;
}

#ifdef CHECK_FOR_FREAD_ERRORS
/*NOT IMPLEMENTED. Just report an error if whitelisted file calls fseek*/
extern "C" __attribute__((visibility("default")))
int fseek(FILE *stream, long offset, int whence){
        int ret;
        int fd;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;


        debug_printf("%s: stream:%p, offset:%ld, whence:%d\n", __func__, stream, offset, whence);

        fd = fileno(stream);
        if(unlikely(fd == -1)){
                SPEEDYIO_FPRINTF("%s:ERROR when doing fileno on fd:%d\n", "SPEEDYIO_ERRCO_0065 %d\n", fd);
                goto exit_fseek;
        }

        ret = real_fseek(stream, offset, whence);
        if(ret == -1){
                goto exit_fseek;
        }

        if(whence != SEEK_SET){
                goto exit_fseek;
        }

        pfd = get_perfd_struct_fast(fd);
        if(!pfd){
                goto exit_fseek;
        }

        if(!pfd->is_blacklisted()){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0008 %d\n", fd);
                KILLME();
                goto exit_fseek;
        }

exit_fseek:
        return ret;
}

/*NOT IMPLEMENTED. Just report an error if whitelisted file calls fseeko*/
extern "C" __attribute__((visibility("default")))
int fseeko(FILE *stream, off_t offset, int whence){
        int ret;
        int fd;

        std::shared_ptr<struct perfd_struct> pfd = nullptr;


        debug_printf("%s: stream:%p, offset:%ld, whence:%d\n", __func__, stream, offset, whence);

        fd = fileno(stream);
        if(unlikely(fd == -1)){
                SPEEDYIO_FPRINTF("%s:ERROR when doing fileno on fd:%d\n", "SPEEDYIO_ERRCO_0066 %d\n", fd);
                goto exit_fseeko;
        }

        ret = real_fseeko(stream, offset, whence);
        if(ret == -1){
                goto exit_fseeko;
        }

        if(whence != SEEK_SET){
                goto exit_fseeko;
        }

        pfd = get_perfd_struct_fast(fd);
        if(!pfd){
                goto exit_fseeko;
        }

        if(!pfd->is_blacklisted()){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0009 %d\n", fd);
                KILLME();
                goto exit_fseeko;
        }

exit_fseeko:
        return ret;
}

#endif //CHECK_FOR_FREAD_ERRORS


/*LINK FUNCTIONS*/

/**
 * NOTE: This is just a boilerplate implementation; does not change anything for a given file.
 * All it does is updates the nr_links for the whitelisted file.
 * Cassandra 3 doesnt use links from what we have observed.
 *
 * TODO: Implement this later as needed.
 * Points to take care when implmenting this:
 * 1. What happens when one of the hardlinks is getting unlinked
 * 2. Consider both blacklisted and whitelisted file for the above question
 * 3. If the new hardlink is a whitelisted file, how to alloc/populate the uinode.
 * Think about the cache residence etc.
 */
void handle_link(const char *oldpath, const char *newpath){
        bool old_is_whitelisted;
        bool new_is_whitelisted;
        struct stat old_file_stat;
        struct inode *uinode = nullptr;
        struct value *val = nullptr;

        /*this is done as default since we are not handling linkat yet. will change later*/
        int dirfd = AT_FDCWD;

        debug_printf("%s: oldpath:%s, newpath:%s\n", __func__, oldpath, newpath);

        /*linking from a softlink*/
        old_is_whitelisted = is_whitelisted(oldpath);
        new_is_whitelisted = is_whitelisted(newpath);

        /*both the paths are blacklisted*/
        if(!old_is_whitelisted && !new_is_whitelisted){
                debug_printf("%s: oldpath:%s, newpath:%s are both blacklisted. Ignoring\n",
                                __func__, oldpath, newpath);
                goto exit_handle_link;
        }

        /**
         * TODO: If either of the two paths are blacklisted, that case is not handled
         * here yet.
         */
        if(old_is_whitelisted != new_is_whitelisted){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED old_path:%s new_path:%s. One of them is blacklisted.\n", "SPEEDYIO_NOTSUPPORTEDCO_0010 %s %s\n", oldpath, newpath);
        }

        /**
         * Since the old path is whitelisted, it should have a uinode.
         * Lets update the nr_links. else exit this function.
         */
        if(!old_is_whitelisted){
                goto exit_handle_link;
        }

#if defined(MAINTAIN_INODE) && defined(PER_FD_DS)

        if(fstatat(dirfd, oldpath, &old_file_stat, AT_SYMLINK_NOFOLLOW) == -1){
                debug_printf("%s:WARNING unable to fstatat dirfd:%d, path:%s error:%s\n",
                        __func__, dirfd, oldpath, strerror(errno));
                goto exit_handle_link;
        }

        /**
         * skip anything further if oldpath is a symbolic link.
         */
        if(!S_ISREG(old_file_stat.st_mode)){
                debug_printf("%s: oldpath:%s is not a regular file\n", __func__, oldpath);
                goto exit_handle_link;
        }

        val = get_from_hashtable(old_file_stat.st_ino, old_file_stat.st_dev);
        if(!val){
                goto exit_handle_link;
        }
        uinode = (struct inode*)val->value;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR Could not find uinode for whitelisted path:%s\n", "SPEEDYIO_ERRCO_0067 %s\n", oldpath);
                goto exit_handle_link;
        }

        /**
         * FACT: Hard links have the same inode number
         * We are skipping the check. Maybe we shouldn't
         */

        if(     (uinode->ino != old_file_stat.st_ino) ||
                (uinode->dev_id != old_file_stat.st_dev))
        {
                /*inode number and device id should match*/
                SPEEDYIO_FPRINTF("%s:ERROR uinode{ino:%lu, dev:%lu} != old_file_stat{ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0068 %lu %lu %lu %lu\n", uinode->ino, uinode->dev_id, old_file_stat.st_ino, old_file_stat.st_dev);
                KILLME();
                goto exit_handle_link;
        }

        if(uinode->is_deleted()){
                SPEEDYIO_FPRINTF("%s:ERROR uinode for path:%s is deleted\n", "SPEEDYIO_ERRCO_0069 %s\n", oldpath);
                KILLME();
                goto exit_handle_link;
        }

        /*update the number of hardlinks created in the system for this whitelisted inode*/
        if(!update_nr_links(uinode, old_file_stat.st_nlink, true)){
                SPEEDYIO_FPRINTF("%s:ERROR unable to update nr_links for uinode:%lu\n", "SPEEDYIO_ERRCO_0070 %lu\n", uinode->ino);
                KILLME();
                goto exit_handle_link;
        }

#endif //MAINTAIN_INODE && PER_FD_DS

exit_handle_link:
        return;
}

extern "C" __attribute__((visibility("default")))
int link(const char *oldpath, const char *newpath){
        int ret;

        ret = real_link(oldpath, newpath);

        if(ret == -1){
                debug_fprintf(stderr, "%s:ERROR recieved %s\n", __func__, strerror(errno));
                goto exit_link;
        }

        handle_link(oldpath, newpath);

exit_link:
        return ret;
}

/*This only throws an error of either paths are whitelisted.*/
extern "C" __attribute__((visibility("default")))
int linkat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags){
        int ret;

        ret = real_linkat(olddirfd, oldpath, newdirfd, newpath, flags);

        if(ret == -1){
                debug_fprintf(stderr, "%s:ERROR recieved %s\n", __func__, strerror(errno));
                goto exit_linkat;
        }

        if(is_whitelisted(oldpath) || is_whitelisted(newpath)){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED file:%s or %s\n", "SPEEDYIO_NOTSUPPORTEDCO_0011 %s %s\n", oldpath, newpath);
                KILLME();
                goto exit_linkat;
        }

exit_linkat:
        return ret;
}


/*RENAME FUNCTIONS*/
#if 0
/**
 * RENAME is not needed since Cassandra 3 doesnt use it for manipulating
 * blacklisted filenames to whitelisted filenames and;
 * it doesnt change whitelisted filenames to any other filenames
 */
int rename(const char *oldpath, const char *newpath){
        int ret;

        ret = real_rename(oldpath, newpath);

        if(ret == -1){
                debug_fprintf(stderr, "%s:ERROR recieved %s\n", __func__, strerror(errno));
                goto exit_rename;
        }

        if(is_whitelisted(oldpath) || is_whitelisted(newpath)){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED file:%s or %s\n", "SPEEDYIO_NOTSUPPORTEDCO_0012 %s %s\n", oldpath, newpath);
                KILLME();
                goto exit_rename;
        }

exit_rename:
        return ret;
}

int renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath){
        int ret;

        ret = real_renameat(olddirfd, oldpath, newdirfd, newpath);

        if(ret == -1){
                debug_fprintf(stderr, "%s:ERROR recieved %s\n", __func__, strerror(errno));
                goto exit_renameat;
        }

        if(is_whitelisted(oldpath) || is_whitelisted(newpath)){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED file:%s or %s\n", "SPEEDYIO_NOTSUPPORTEDCO_0013 %s %s\n", oldpath, newpath);
                KILLME();
                goto exit_renameat;
        }

exit_renameat:
        return ret;
}

int renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags){
        int ret;

        ret = real_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);

        if(ret == -1){
                debug_fprintf(stderr, "%s:ERROR recieved %s\n", __func__, strerror(errno));
                goto exit_renameat2;
        }

        if(is_whitelisted(oldpath) || is_whitelisted(newpath)){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED on WHITELISTED file:%s or %s\n", "SPEEDYIO_NOTSUPPORTEDCO_0014 %s %s\n", oldpath, newpath);
                KILLME();
                goto exit_renameat2;
        }

exit_renameat2:
        return ret;
}
#endif //RENAME is not needed


/**
 * Just reports error if the following are called on whitelisted files
 * 1. F_DUPFD_CLOEXEC or F_DUPFD
 * 2. F_SETFL + O_DIRECT
 * 3. F_SETFD + FD_CLOEXEC
 */
extern "C" __attribute__((visibility("default")))
int fcntl(int fd, int cmd, ...)
{
        int ret;
        uintptr_t arg_u = 0; /* always forwarded */
        int arg_i = 0; /* integer view (for logs / checks) */
        struct inode *uinode = nullptr;

        std::shared_ptr<perfd_struct> pfd = nullptr;

        bool want_dup_msg     = false;   /* F_DUPFD / F_DUPFD_CLOEXEC */
        bool want_cloexec_msg = false;   /* F_SETFD  + FD_CLOEXEC     */
        bool want_odirect_msg = false;   /* F_SETFL  + O_DIRECT       */

        /* === collect 3rd argument (if any) ============================= */
        va_list ap;
        va_start(ap, cmd);

        switch (cmd){

        /* pointer‑based cmds ------------------------------------------- */
        case F_GETLK:
        case F_SETLK:
        case F_SETLKW: {
                struct flock *flp = va_arg(ap, struct flock *);
                arg_u = reinterpret_cast<uintptr_t>(flp);
                /* no extra policy checks for locks */
                break;
        }

        /* integer‑based cmds you care about ---------------------------- */
        case F_DUPFD_CLOEXEC:
                arg_i = va_arg(ap, int);
                arg_u = static_cast<uintptr_t>(arg_i);
                want_dup_msg = true;                /* fall through */
        case F_DUPFD:
                if (!want_dup_msg) {                /* reached via fall‑through */
                        arg_i = va_arg(ap, int);
                        arg_u = static_cast<uintptr_t>(arg_i);
                }
                want_dup_msg = true;
                break;

        case F_SETFD:
                arg_i = va_arg(ap, int);
                arg_u = static_cast<uintptr_t>(arg_i);
                if (arg_i & FD_CLOEXEC)
                        want_cloexec_msg = true;
                break;

        case F_SETFL:
                arg_i = va_arg(ap, int);
                arg_u = static_cast<uintptr_t>(arg_i);
                if (arg_i & O_DIRECT)
                        want_odirect_msg = true;
                break;

        /* other integer‑arg cmds --------------------------------------- */
        case F_SETOWN:    case F_SETSIG:    case F_SETLEASE:
        case F_NOTIFY:    case F_SETPIPE_SZ:case F_ADD_SEALS:
                arg_i = va_arg(ap, int);
                arg_u = static_cast<uintptr_t>(arg_i);
                break;

        /* zero‑arg cmds ------------------------------------------------- */
        default:
                /* arg_u stays 0 */
                break;
        }

        // printf("%s:INFO before fcntl(%d, %d, 0x%" PRIxPTR ")\n",
        //         __func__, fd, cmd, arg_u);

        ret = real_fcntl(fd, cmd, arg_u);

        // printf("%s:INFO after  fcntl(%d, %d, 0x%" PRIxPTR ")\n",
        //         __func__, fd, cmd, arg_u);

        if(ret == -1)
                goto exit_fcntl;

        if(!want_dup_msg && !want_cloexec_msg && !want_odirect_msg)
                goto exit_fcntl;

        pfd = get_perfd_struct_fast(fd);
        if(!pfd)
                goto exit_fcntl;

        if(pfd->fd != fd){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d doesnt match fd:%d\n", "SPEEDYIO_ERRCO_0071 %d %d\n", pfd->fd, fd);
                KILLME();
                goto exit_fcntl;
        }

        if(pfd->is_blacklisted()){
                goto exit_fcntl;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:WARNING fd:%d is closed. Skipping\n", "SPEEDYIO_WARNCO_0002 %d\n", fd);
                goto exit_fcntl;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode for this whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0072 %d\n", fd);
                KILLME();
                goto exit_fcntl;
        }

        if(uinode->is_deleted()){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0073 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto exit_fcntl;
        }

        if(want_dup_msg){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED %s on whitelisted fd:%d (arg=%d)\n", "SPEEDYIO_NOTSUPPORTEDCO_0015 %s %d %d\n", (cmd == F_DUPFD) ? "F_DUPFD" : "F_DUPFD_CLOEXEC", fd, arg_i);
                KILLME();
        }
        /*check comments in check_open_flag_sanity on O_CLOEXEC*/
        // if (want_cloexec_msg) {
        // SPEEDYIO_FPRINTF("%s:NOTSUPPORTED F_SETFD+FD_CLOEXEC on fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0016 %d\n", fd);
        //         KILLME();
        // }
        if(want_odirect_msg){
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED F_SETFL+O_DIRECT on fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0017 %d\n", fd);
                KILLME();
        }

exit_fcntl:
        va_end(ap);
        return ret;
}

//PREFETCH SYSCALLS
extern "C" __attribute__((visibility("default")))
ssize_t readahead(int fd, off_t offset, size_t count){
        ssize_t ret;
        long first_unset_pg_bit;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        long pg_offset = PG_NR_FROM_OFFSET(offset);
        long nr_count = BYTES_TO_PG(count);
        struct inode *uinode = nullptr;

        //enables per thread ds
        per_th_d.touchme = true;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)

        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                goto real_ra;
        }

        if(pfd->fd != fd){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d doesnt match fd:%d\n", "SPEEDYIO_ERRCO_0074 %d %d\n", pfd->fd, fd);
                KILLME();
                goto real_ra;
        }

        if(pfd->is_blacklisted()){
                goto real_ra;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:WARNING fd:%d is closed. Skipping\n", "SPEEDYIO_WARNCO_0003 %d\n", fd);
                goto real_ra;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode for this whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0075 %d\n", fd);
                KILLME();
                goto real_ra;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0076 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto real_ra;
        }

        /**
         * XXX: Add PRINT_READ_EVENTS here
         */

        /*Right now we are skipping readahead to any whitelisted file*/

        // SPEEDYIO_PRINTF("%s:INFO Skipping for WHITELISTED fd:%d {ino:%lu, dev:%lu} offset:%ld count:%ld\n", "SPEEDYIO_INFOCO_0009 %d %lu %lu %ld %ld\n", fd, uinode->ino, uinode->dev_id, pg_offset, nr_count);
        ret = 0;
        goto exit_readahead;

#ifdef ENABLE_PER_INODE_BITMAP

#ifdef CHECK_BITMAP_RA
        /*
         * This checks if the requested pages are in the cache or not
         * The request is altered to only pages that are not in the cache
         * Saves on syscall and locking overheads inside the kernel
         */
        first_unset_pg_bit = first_unset_bit(uinode, pg_offset, nr_count);

        if(unlikely(first_unset_pg_bit == -2))
                goto real_ra;

        if(first_unset_pg_bit > -1){
                nr_count -= (first_unset_pg_bit - pg_offset);
                pg_offset = first_unset_pg_bit;
        }
        else{
                /*No unset pages in the given range. Skip readahead*/
                goto exit_readahead;
        }
#endif //CHECK_BITMAP_RA

        SPEEDYIO_FPRINTF("%s:ERROR readahead should not update the bitmap\n", "SPEEDYIO_ERRCO_0077\n");
        KILLME();
        //Update Bitmap
        set_range_bitmap(uinode, pg_offset, nr_count);
#endif //ENABLE_PER_INODE_BITMAP

#ifdef ENABLE_EVICTION
        /**
         * XXX: Here we need to update the count based on the current linux version
         * eg: linux 4 allows only upto 128 KB to be prefetched at once
         * Linux 5 allows more than 128 KB (around 2 MB)
         * a count > this allowed value will be truncated by the OS.
         */
        heap_update(uinode, offset, count, true);
#endif //ENABLE_EVICTION

#endif //PER_FD_DS and MAINTAIN_INODE

real_ra:
        ret = real_readahead(fd, pg_offset << PAGE_SHIFT, nr_count << PAGE_SHIFT);

exit_readahead:
        return ret;
}

/**
 * it returns true if real_fadvise should be called
 * else returns false.
 */
bool handle_fadvise(int fd, off_t offset, off_t len, int advice){
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        bool ret = true;
        struct inode *uinode = nullptr;
        //enables per thread ds
        per_th_d.touchme = true;

#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                goto exit_handle_fadvise;
        }

        if(pfd->fd != fd){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d doesnt match fd:%d\n", "SPEEDYIO_ERRCO_0078 %d %d\n", pfd->fd, fd);
                KILLME();
                goto exit_handle_fadvise;
        }

        if(pfd->is_blacklisted()){
                goto exit_handle_fadvise;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:WARNING fd:%d is closed. Skipping\n", "SPEEDYIO_WARNCO_0004 %d\n", fd);
                goto exit_handle_fadvise;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode for this whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0079 %d\n", fd);
                KILLME();
                goto exit_handle_fadvise;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0080 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                goto exit_handle_fadvise;
        }

        if(advice == POSIX_FADV_WILLNEED){
                //SPEEDYIO_FPRINTF("%s:INFO skipping POSIX_FADV_WILLNEED called on whitelisted fd:%d\n", "SPEEDYIO_INFOCO_0010 %d\n", fd);
                ret = false;
                goto exit_handle_fadvise;
                // ret = true;
                // goto update_uinode;
        }

        if(advice == POSIX_FADV_NOREUSE){
                ret = false;
                SPEEDYIO_FPRINTF("%s:NOTSUPPORTED POSIX_FADV_NOREUSE called on whitelisted fd:%d\n", "SPEEDYIO_NOTSUPPORTEDCO_0018 %d\n", fd);
                //KILLME();
                goto exit_handle_fadvise;
        }

        if(advice == POSIX_FADV_NORMAL || advice == POSIX_FADV_SEQUENTIAL || advice == POSIX_FADV_RANDOM){
                debug_fprintf(stderr, "%s:INFO skipping POSIX_FADV_NORMAL/POSIX_FADV_SEQUENTIAL/POSIX_FADV_RANDOM called on whitelisted fd:%d\n",
                                __func__, fd);
                ret = false;
                goto exit_handle_fadvise;
        }

        if(advice == POSIX_FADV_DONTNEED){

                // fprintf(stderr, "%s:INFO DONTNEED for file:%s, off:%lu, size:%lu\n", __func__, uinode->filename, offset, len);

#ifdef ENABLE_FADV_DONT_NEED
                ret = true;

#ifdef ENABLE_SEQ_ON_DONTNEED
                real_posix_fadvise64(fd, offset+len, 0, POSIX_FADV_SEQUENTIAL);
                set_fadv_on_fd_uinode(uinode, fd, true);
#endif //ENABLE_SEQ_ON_DONTNEED

                goto update_uinode_dontneed;
#else //dont do POSIX_FADV_DONTNEED
                ret = false;
                goto exit_handle_fadvise;
#endif //ENABLE_FADV_DONT_NEED

        }

        cfprintf(stderr, "%s:ERROR advice %d should not update heap\n", __func__, advice);
        KILLME();

update_uinode_dontneed:
        /**
         * update the heap according to what has been evicted
         */
        // heap_dont_need_update(uinode, fd, offset, len); //FOR TESTING ONLY
        goto exit_handle_fadvise;

update_uinode:
#ifdef ENABLE_EVICTION
        /**
         * XXX: Here we need to update the count based on the current linux version
         * eg: linux 4 allows only upto 128 KB to be prefetched at once
         * Linux 5 allows more than 128 KB (around 2 MB)
         * a count > this allowed value will be truncated by the OS.
         *
         * BUG: if offset == 0 and len == 0 This will lead to (very large)
         * incorrect last_portion_nr in heap_update.
         */
        heap_update(uinode, offset, len, false);
#endif //ENABLE_EVICTION

#endif //PER_FD_DS and MAINTAIN_INODE

exit_handle_fadvise:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int posix_fadvise(int fd, off_t offset, off_t len, int advice){
        int ret = -1;

        if(handle_fadvise(fd, offset, len, advice) == false){
                ret = 0;
                errno = 0;
                goto skip_posix_fadvise;
        }

real_fadvise:
        ret = real_posix_fadvise(fd, offset, len, advice);
skip_posix_fadvise:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int posix_fadvise64(int fd, off_t offset, off_t len, int advice){
        int ret = -1;

        if(handle_fadvise(fd, offset, len, advice) == false){
                ret = 0;
                errno = 0;
                goto skip_posix_fadvise64;
        }

real_fadvise64:
        ret = real_posix_fadvise64(fd, offset, len, advice);
skip_posix_fadvise64:
        return ret;
}

extern "C" __attribute__((visibility("default")))
int fadvise64(int fd, off_t offset, off_t len, int advice){
        int ret = -1;

        if(handle_fadvise(fd, offset, len, advice) == false){
                ret = 0;
                errno = 0;
                goto skip_fadvise64;
        }

real_fadvise64:
        ret = real_fadvise64(fd, offset, len, advice);
skip_fadvise64:
        return ret;
}


int handle_mmap(size_t length, int prot, int flags, int fd, off_t offset){
        int ret = true;
        std::string prot_str;
        std::string flags_str;
        std::shared_ptr<struct perfd_struct> pfd = nullptr;
        ssize_t len;
        char path[PATH_MAX];
        char filename[PATH_MAX];
        struct inode *uinode = nullptr;

        if(fd < 3){
                goto exit_handle_mmap;
        }

        // snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
        // len = readlink(path, filename, sizeof(filename) - 1);
        // if (len != -1) {
        //         filename[len] = '\0'; // Null-terminate the string
        // } else {
        //         snprintf(filename, sizeof(filename), "unknown");
        // }


        // cprintf("%s:INFO called for fd:%d filename:%s\n", __func__, fd, filename);


        /*If a whitelisted file is being mmaped, throw error*/
#if defined(PER_FD_DS) && defined(MAINTAIN_INODE)
        pfd = get_perfd_struct_fast(fd);

        if(!pfd){
                goto exit_handle_mmap;
        }

        if(pfd->fd != fd){
                SPEEDYIO_FPRINTF("%s:ERROR pfd->fd:%d doesnt match fd:%d\n", "SPEEDYIO_ERRCO_0082 %d %d\n", pfd->fd, fd);
                KILLME();
                ret = true;
                goto exit_handle_mmap;
        }

        if(pfd->is_blacklisted()){
                // cfprintf(stderr, "%s:INFO fd:%d is blacklisted\n", __func__, fd);
                ret = true;
                goto exit_handle_mmap;
        }

        if(pfd->is_closed()){
                SPEEDYIO_FPRINTF("%s:WARNING fd:%d is closed. Skipping\n", "SPEEDYIO_WARNCO_0005 %d\n", fd);
                ret = true;
                goto exit_handle_mmap;
        }

        uinode = pfd->uinode;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode for this whitelisted fd:%d\n", "SPEEDYIO_ERRCO_0083 %d\n", fd);
                KILLME();
                ret = true;
                goto exit_handle_mmap;
        }

        if(unlikely(uinode->is_deleted())){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d {ino:%lu, dev:%lu} is deleted. Skipping\n", "SPEEDYIO_ERRCO_0084 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                KILLME();
                ret = true;
                goto exit_handle_mmap;
        }

        if (prot & PROT_READ) prot_str += "PROT_READ ";
        if (prot & PROT_WRITE) prot_str += "PROT_WRITE ";
        if (prot & PROT_EXEC) prot_str += "PROT_EXEC ";
        if (prot & PROT_NONE) prot_str += "PROT_NONE ";

        if (flags & MAP_SHARED) flags_str += "MAP_SHARED ";
        if (flags & MAP_PRIVATE) flags_str += "MAP_PRIVATE ";
        if (flags & MAP_FIXED) flags_str += "MAP_FIXED ";
        if (flags & MAP_ANONYMOUS) flags_str += "MAP_ANONYMOUS ";
        if (flags & MAP_POPULATE) flags_str += "MAP_POPULATE ";
        if (flags & MAP_NONBLOCK) flags_str += "MAP_NONBLOCK ";


        SPEEDYIO_FPRINTF("%s:NOTSUPPORTED mmap called on whitelisted fd:%d with prot: %s, flags: %s, length: %zu, offset: %ld, and file: %s\n", "SPEEDYIO_NOTSUPPORTEDCO_0019 %d %s %s %zu %ld %s\n", fd, prot_str.c_str(), flags_str.c_str(), length, offset, uinode->filename);
        // cprintf("%s:NOTSUPPORTED mmap called on whitelisted fd:%d with prot: %s, flags: %s, length: %zu, offset: %ld, and file: %s\n",
        //                 __func__, fd, prot_str.c_str(), flags_str.c_str(), length, offset, uinode->filename);
        // KILLME();

#endif //PER_FD_DS && MAINTAIN_INODE

exit_handle_mmap:
        return ret;
}


extern "C" __attribute__((visibility("default")))
void *mmap64(void *addr, size_t length, int prot, int flags, int fd, off_t offset){
        void *ret = nullptr;

        if(fd < 3){
                goto do_mmap64;
        }

        // cprintf("%s:INFO called for fd:%d\n", __func__, fd);

        handle_mmap(length, prot, flags, fd, offset);

do_mmap64:
        ret = real_mmap(addr, length, prot, flags, fd, offset);
exit_mmap64:
        return ret;
}


extern "C" __attribute__((visibility("default")))
void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset){
        void *ret = nullptr;

        if(fd < 3){
                goto do_mmap;
        }

        // cprintf("%s:INFO called for fd:%d\n", __func__, fd);

        handle_mmap(length, prot, flags, fd, offset);

do_mmap:
        ret = real_mmap(addr, length, prot, flags, fd, offset);

exit_mmap:
        return ret;
}



/**
 * To understand how well are different eviction algorithms are fairing,we have written
 * a utility that runs a trace file against Belady's algorithm in a cache simulator.
 * The idea is that if cache hits increase in this cache simulator, we should see it
 * translate to real performance improvements barring other overhead in the library.
 * look at Makefile BELADY.
 * The following functions are directly exposed.
 *
 * The work flow is as follows:
 * 1. At the beginning, all the inodes are populated using populate_inodes. No events other
 * than read, writes and eviction are expected (eg. unlink, open, close etc.). This populates
 * all the inodes in ino->uinode hashtable (i_map).
 *
 * 2. mock_read/mock_write update the internal data structures(heap_update) for the given
 * {ino, dev_id}. Currently, mock_read handles both read and write since writes are not
 * supported to be treated differently from reads. TODO: This should change later.
 *
 * 3. When the cache simulator's cache is full, it would call mock_eviction to get eviction
 * decision from the current eviction policy. It would then remove those elements from the cache.
 *
 * The cache state maintained in the cache simulator is not visible to the eviction library.
 * It decides victim elements based on the (approx.) cache state maintained by these eviction
 * policies (using heap etc).
 *
 * To check the current eviction algorithms against original flat LRU implementation, we have
 * also implemented a flat LRU (ifdef ENABLE_ONE_LRU)just for Belady's proof. XXX: This is not
 * available for real workloads yet.
 *
 * NOTE: The trace is always expected to be run from a single thread. All the events from a
 * multithreaded trace must be ordered by their respective rdtsc values. ie. all the read/write
 * events are replayed by a single thread in order of their rdtsc values, not withstanding their
 * corresponding thread id or process id.
 */

#ifdef BELADY_PROOF

extern "C" {

/**
 * This function expects that all the {ino, dev_id} pairs are unique.
 */
__attribute__((visibility("default")))
int populate_inodes(struct mock_all_inodes *inode_list){
        int i;
        if(!inode_list || inode_list->nr_inodes <= 0){
                SPEEDYIO_FPRINTF("%s:ERROR inode_list is NULL or nr_inodes <=0\n", "SPEEDYIO_ERRCO_0085\n");
                goto exit_populate_inodes;
        }
        // printf("%s: nr_inodes:%d\n", __func__, inode_list->nr_inodes);

        for(i = 0; i < inode_list->nr_inodes; i++){
                // printf("%s: {ino:%lu, dev:%lu}\n", __func__, inode_list->inodes[i].ino, inode_list->inodes[i].dev_id);
                mock_populate_inode_ds(inode_list->inodes[i].ino, inode_list->inodes[i].dev_id);
        }

exit_populate_inodes:
        return 1;
}

/**
 * This currently handles both read and write events.
 *
 * Event timestamps from the trace are passed aswell because:
 *
 * 1. This ensures repeatability of a mock run. ie. we see the
 * exact same result for a given trace if run multiple times.
 *
 * 2. The trace runs so fast that the rdtsc time difference is
 * very small between two consecutive events. This might become
 * a problem when subtracting from first_rdtsc or more complex
 * functions based on it (eg. LRU_COMPLEX).
 *
 * 3. Different rdtsc values are maintained by different cores
 * on a single CPU die. This becomes all the more complicated
 * for multiple numa nodes.
 */
__attribute__((visibility("default")))
int *mock_read(struct mock_read_event *event){

        struct inode *uinode = nullptr;
        if(!event){
                SPEEDYIO_FPRINTF("%s:ERROR nullptr event!\n", "SPEEDYIO_ERRCO_0086\n");
                goto exit_mock_read;
        }

        //printf("%s: {ino:%lu, dev:%lu}, offset:%ld, size:%ld\n", __func__, event->ino, event->dev_id, event->offset, event->size);

        uinode = get_uinode_from_hashtable(event->ino, event->dev_id);
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0087 %lu %lu\n", event->ino, event->dev_id);
                KILLME();
                goto exit_mock_read;
        }

#ifdef ENABLE_EVICTION
        heap_update(uinode, event->offset, event->size, true, event->timestamp);
#endif //ENABLE_EVICTION

exit_mock_read:
        return nullptr;
}

/**
 * eviction event is triggered by the cache simulator. for each event,
 * mock_eviction returns exactly one portion of file to evict. If that is
 * not enough, cache simulator can trigger another eviction event.
 *
 * If offset == 0 and size == 0, this should be interpreted as eviction of the whole file.
 */
__attribute__((visibility("default")))
struct mock_eviction_item *mock_eviction(void){
        struct mock_eviction_item *event = nullptr;

#if defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP)
        /*kept sz_to_claim_kb=1 such that the evict_portions function runs exactly once*/
        event = evict_portions(1);
#elif defined(ENABLE_EVICTION) && defined(ENABLE_ONE_LRU)
        event = evict_from_one_lru(1);
#elif defined(ENABLE_EVICTION)
        event = evict_file();
#endif //ENABLE_EVICTION

        if(!event){
                SPEEDYIO_FPRINTF("%s:INFO no eviction event\n", "SPEEDYIO_INFOCO_0015\n");
                KILLME();
        }

        // printf("%s:INFO eviction event: {ino:%lu, dev:%lu}, offset:%ld, size:%ld\n", __func__,
        //         event->ino, event->dev_id, event->offset, event->size);

        return event;
}

} //extern "C"

#endif //BELADY_PROOF
