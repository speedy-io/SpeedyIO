#ifndef _PREFETCH_EVICT_HPP
#define _PREFETCH_EVICT_HPP

#include "inode.hpp"
#include "utils/shim/shim.hpp"
#include "utils/events_logger/events_logger.hpp"
#include "utils/latency_tracking/latency_tracking.hpp"

extern struct lat_tracker pvt_heap_latency;
extern struct lat_tracker g_heap_latency;
extern struct lat_tracker ulong_heap_update;


struct perfd_struct{

        ino_t ino; //inode number
        dev_t dev_id; //device ID

        int fd;
        int open_flags;

        struct inode *uinode;

        /*
         * Indicates that the pfd_struct represents
         * a blacklisted file.
         * check function is_whitelisted for more details.
         *
         * We dont use an atomic variable for blacklisted and open
         * since those impose significant overheads;and we don't
         * free the contents the uinode even if the corresponding
         * file is unlinked. So, in case of a race condition, we
         * would be doing one unnecessary operation only instead
         * of getting a segfault.
         */
        bool blacklisted;

        /*
         * If this fd is open, this open is set to be true
         * else it is false.
         */
        bool fd_open;

        bool is_blacklisted(){
                return blacklisted;
        }

        bool is_closed(){
                return !fd_open;
        }

        perfd_struct(){
                ino = 0UL;
                dev_id = 0UL;
                fd = 0;
                open_flags = 0;
                uinode = nullptr;
                blacklisted = false;
                fd_open = true;
        }

        ~perfd_struct(){
                //debug_fprintf(stderr, "%s: Destroying pfd with fd:%d\n", __func__, fd);
                ino = 0UL;
                dev_id = 0UL;
                fd = 0;
                open_flags = 0;
                uinode = nullptr;
                blacklisted = false;
                fd_open = false;
        }
};

/**
 * This is used only for ONE_LRU.
 * which is enabled only for BELADY_PROOF
 */
struct lru_entry{
        struct inode *uinode;
        off_t portion_nr;
        
        lru_entry(){
                uinode = nullptr;
                portion_nr = -1;
        }
};

void init_g_fd_map();

std::shared_ptr<struct perfd_struct> add_fd_to_perfd_struct(int, struct inode *);
std::shared_ptr<struct perfd_struct> add_any_fd_to_perfd_struct(int, int, struct inode *, bool);
std::shared_ptr<struct perfd_struct> get_perfd_data(int fd);
std::shared_ptr<struct perfd_struct> get_perfd_data_nolock(int fd);
std::shared_ptr<struct perfd_struct> get_perfd_struct_fast(int fd);


void delete_fd(int, bool);

/*
 * Operations on global LRU
 */
void update_g_lru(struct inode*);

/*
 * Operations on global heap
 */
void init_g_heap();
void remove_from_g_heap(struct inode* uinode);

#ifdef BELADY_PROOF
void update_g_heap(struct inode* uinode, uint64_t timestamp);
#else
void update_g_heap(struct inode* uinode);
#endif

void* concurrent_eviction(void *arg);

/*
 * Operations on pvt heap
 */
void init_pvt_heap(struct inode* uinode);
bool clear_pvt_heap(struct inode* uinode);

#ifdef BELADY_PROOF
unsigned long long int update_pvt_heap(struct inode* uinode, off_t offset, size_t size, bool from_read, uint64_t timestamp);
#else
unsigned long long int update_pvt_heap(struct inode* uinode, off_t offset, size_t size, bool from_read);
#endif

void destroy_pvt_heap(struct Heap *pvt_heap);
unsigned long long int get_min_key(struct inode* uinode);

void heap_dont_need_update(struct inode* uinode, int fd, off_t offset, size_t size);

#ifdef BELADY_PROOF
void heap_update(struct inode* uinode, off_t offset, size_t size, bool from_read, uint64_t timestamp);
#else
void heap_update(struct inode* uinode, off_t offset, size_t size, bool from_read);
#endif //BELADY_PROOF

#ifdef BELADY_PROOF
/**
 * Eviction functions for one_lru, pvt_lru and global lru respectively
 */
struct mock_eviction_item *evict_from_one_lru(long sz_to_claim_kb);
struct mock_eviction_item *evict_portions(long sz_to_claim_kb);
struct mock_eviction_item *evict_file(void);

/*DEBUGGING*/
void print_full_gheap();

#endif //BELADY_PROOF

#endif
