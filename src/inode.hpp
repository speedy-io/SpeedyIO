#ifndef _INODE_HPP
#define _INODE_HPP

#include <string.h>
// #include <x86intrin.h>

#include <atomic>
#include <mutex>
#include <vector>

#include "utils/ticks.h"
#include "utils/hashtable/hashtable.h"
#include "utils/hashtable/hashtable_private.h"
#include "utils/util.hpp"
#include "utils/bitmap/bitmap.h"
#include "utils/r_w_lock/readers_writers_lock.hpp"
#include "utils/vector/auto_expand_vector.hpp"
#include "utils/trigger/trigger.hpp"

/**
 * total_nr_unlinks is used to trigger iter_i_map_and_put_unused
 */
extern struct trigger *nr_unlinks_for_imap_cleanup;

extern struct hashtable *i_map;
extern std::atomic_flag i_map_init;

struct key {
        ino_t  ino;
        dev_t  dev_id;
};

struct value {
        void *value;
};

struct hashtable *init_inode_map(void);
struct value *get_from_hashtable(ino_t ino, dev_t dev_id);
struct inode *get_uinode_from_hashtable(ino_t ino, dev_t dev_id);
void *bg_inode_cleaner(void *arg);

void alloc_bitmap(struct inode *);
void destroy_bitmap(struct inode *);
void set_range_bitmap(struct inode *, unsigned long start_bit, unsigned long num_bits);
void clear_range_bitmap(struct inode *, unsigned long start_bit, unsigned long num_bits);
bool clear_full_bitmap(struct inode *uinode);
long first_set_bit(struct inode *, unsigned long start_bit, unsigned long num_bits);
long first_unset_bit(struct inode *, unsigned long start_bit, unsigned long num_bits);
bool bits_are_set(struct inode *, unsigned long start_pos, unsigned long num_bits);

#ifdef ENABLE_MINCORE_DEBUG
// Mincore functions
void allocate_mmap(inode* uinode);
void free_mmap(inode* uinode);
std::vector<bool> check_mincore(inode* uinode);
void update_mmap_fd(inode* uinode);
void print_mincore_array(const std::vector<bool>& page_residency_arr, off_t filesize);
#endif // ENABLE_MINCORE_DEBUG

/*Operations on inode*/
bool sanitize_uinode(struct inode *uinode);
bool update_nr_links(struct inode *uinode, nlink_t nlink, bool take_unlinked_lock);
bool add_fd_to_fdlist(struct inode *uinode, int fd, int open_flags, off_t seek_head);
int remove_fd_from_fdlist(struct inode *uinode, int fd);
bool clear_uinode_fdlist(struct inode *uinode);
ssize_t update_fd_seek_pos(struct inode *uinode, int fd, off_t bytes, bool set_to);
bool reset_all_fd_seek_pos(struct inode *uinode);
int get_open_flags_from_uinode(struct inode *uinode, int fd);
bool get_fadv_from_uinode(struct inode *uinode);
int set_fadv_on_fd_uinode(struct inode *uinode, int fd, bool is_seq);
struct inode *add_fd_to_inode(int fd, int open_flags, const char *filename);


/*Functions for pvt lru*/

void update_pvt_lru(struct inode*, off_t, size_t);

/*functions for pvt heap*/
void _dest_pvt_heap(struct inode *uinode);


/**
 * Mock functions
 */
int mock_populate_inode_ds(ino_t ino, dev_t dev_id);


/*contains the fd and its corresponding seek head for a file*/
struct fd_info{
        int fd;
        /**
         * The following system calls update the seek_head
         * read, fread, write, fwrite, lseek
         * pread and pwrite dont change the seek_head
         */
        off_t seek_head;
        int open_flags;

        /**
         * FADV_SEQUENTIAL, FADV_NORMAL tells the OS to prefetch for each
         * reads on this file based on its predictor.
         *
         * Note: fadvise is a per fd construct. ie. for a uinode with two
         * fds, fd1 and fd2, if one of the above advise have been called on
         * fd1, it will not be subjected to reads from fd2.
         */
        bool fadv_seq;

        void reset(){
                fd = 0;
                seek_head = 0;
                open_flags = 0;
                fadv_seq = true;
        }

        fd_info(){
                fd = 0;
                seek_head = 0;
                open_flags = 0;
                fadv_seq = true;
        }
};

struct inode{
        ino_t ino; //inode number
        dev_t dev_id; //device ID

        char filename[PATH_MAX]; //filename of the inode

        /**
         * Different locks defined in the struct inode
         * are held while performing operations like:
         * read, write, close, unlink and eviction
         *
         * Sometimes, multiple threads working on the same uinode
         * may interleave these operations especially with the eviction thread
         * such that the cache state represented by the heap and bitmap
         * diverge from the actual cache state of the file in the OS.
         * This was checked with mincore debugging.
         *
         * eg. If a read is happening on a portion of file which was elected to be
         * evicted; interleaving would diverge the cache state.
         *
         * To prevent interleaving of these operations, we have introduced
         * the uinode_lock. It is taken before doing anything in the above
         * operations and released only after all the work is done.
         *
         * Thought with bigger portion sizes, the cache divergence problem should be less
         * significant; right now its not clear if this lock improves the performance of the library.
         * Hence it is not enabled by default. ENABLE_UINODE_LOCK enables it.
         *
         * XXX: the implementation of holding the lock is messy in read and write syscalls.
         * To be cleaned.
         */
        std::mutex uinode_lock;

        struct fd_info fdlist[MAX_FD_PER_INODE];
        int fdlist_index; //fd_list index. max fdlist_index = (MAX_FD_PER_INODE - 1)
        std::mutex fdlist_lock; //lock for update to the fdlist

        //Enabled with ENABLE_PER_INODE_BITMAP
        bit_array_t *cache_state;
        ReaderWriterLock cache_rwlock;
        //TODO: Add interval tree for bitmap

        //PVT_HEAP
        /*
         * Eviction Book Keeping for each page range in this file.
         * Size of the page range is defined by PVT_HEAP_PG_ORDER.
         */
        struct Heap* file_heap;

        //stores ids to heap nodes for each file portion
        AutoExpandVector<int> *file_heap_node_ids;
        std::mutex file_heap_lock;

        int heap_id; //this is the unique id for the heap node in global heap
        bool one_operation_done; //false if no read/write operations done; else true

        unsigned long nr_accesses; //number of accesses
        unsigned long long int last_access_tstamp; //time stamp of last access for EVICTION_COMPLEX

        struct trigger *gheap_trigger; //trigger gheap update based on number of read/write syscalls - replacement of nr_accesses

        /*
        * nr_evictions used for EVICTION_COMPLEX.
        * It is not being used currently.
        */
        unsigned int nr_evictions; //number of times this inode has been evicted

        /**
         * The linux kernel keeps an unlinked file alive till all its
         * open fds are closed implicitly or explicitly; it also keeps
         * them alive if there are hardlinks to that file(st_nlink).
         *
         * Meaning if you have open fds to a file that is unlinked,
         * these fds will still be able to read and write to the file perfectly fine.
         * Also, the file be accessible by any of its hardlinks without any issues.
         *
         * Only when all the open fds are closed and all the hardlinks are unlinked,
         * is when the file is finally removed from the filesystem and its
         * corresponding page cache is freed.
         *
         * unlinked == true means the file has truly been unlinked in all way shape or form
         * marked_unlinked == true means that unlink has been called for atleast one of the
         * hardlinks.
         * nr_links stores the number of links made to this inode. we get this info whenever
         * an open, link or unlink is called on this inode from the fstat syscall (st_nlinks).
         *
         * Hardlinks could also be created or unlinked outside the purview of the library.
         * So, before finally making unlinked = true, we check:
         * 1. if marked_unlinked == true
         * 2. nr_links == 1 - only one link remains for this inode.
         * 3. fdlist_index < 0 - this means there are no open fd for this inode
         *
         * We dont use nr of marked_unlinked instead of just a bool because it doesnt give us
         * any more information. a file unlinked outside the purview of the library will not be
         * recorded; so nr_links acts as a good ground truth from the OS.
         *
         * We don't free the contents the uinode even if the corresponding file is unlinked.
         * So, in case of a race condition, we would be doing one unnecessary operation 
         * only instead of getting a segfault.
         * XXX: This will be done later with a background cleanup thread.
         */
        bool unlinked; //actually deleted the inode
        bool marked_unlinked; //unlink called for this inode, not actually deleted yet
        nlink_t nr_links; //keeps track of the number of links for this inode (using st_nlink)
        std::mutex unlinked_lock;

#ifdef ENABLE_MINCORE_DEBUG
        void* mmap_addr; // Pointer to mmap address
        int mmap_fd;
#endif // ENABLE_MINCORE_DEBUG

        /**
         * Do we really need a lock here ?
         * This poses a significant performance loss for each read/write etc.
         *
         * We can skip the lock for the following reasons:
         * 1. The flag unlinked does not switch multiple times in its lifetime.
         * 2. If there is a race condition where is_deleted() returns false while
         * being changed to true; that is just one extra operation on the uinode.
         * 3. Functions who need tight is_deleted checks take unlinked_lock themselves.
         */
        bool is_deleted(){
                bool ret;
                //unlinked_lock.lock();
                ret = unlinked;
                //unlinked_lock.unlock();
                return ret;
        }

        /**
        * The uinode should be unlinked if and only if:
        * 1. It has been marked_unlinked earlier for atleast one hardlink
        * 2. It has no fds in the fdlist
        * 3. only one link to the inode left in the system
        *
        * Note: The library currently does not detect whitelisted files or its
        * hardlinks being unlinked outside its purview.
        */
        bool check_fdlist_and_unlink(){
                bool ret = false;
                unlinked_lock.lock();

                if(!unlinked && marked_unlinked){
                        fdlist_lock.lock();

                        if(fdlist_index < 0 && nr_links == 1){
                                unlinked = true;
                                ret = true;
                        }
                        fdlist_lock.unlock();
                }

                unlinked_lock.unlock();

                if(ret){
                        debug_printf("%s: called on {ino:%lu, dev:%lu} UNLINKED\n", __func__, ino, dev_id);
                }
                return ret;
        }

        inode(){
                ino = 0UL;
                dev_id = 0UL;

                memset(filename, 0, PATH_MAX);

                fdlist_index = -1; // This means there are no fds in fdlist
                cache_state = nullptr;
                memset(fdlist, 0, MAX_FD_PER_INODE*sizeof(struct fd_info));

                unlinked = false;
                marked_unlinked = false;
                nr_links = 0;

#ifdef ENABLE_EVICTION
                /*since smallest global heap_id can be 0*/
                heap_id = -1;
                one_operation_done = false;

                /**
                 * nr_accesses are updated with ENABLE_EVICTION on
                 */
                nr_accesses = 1;
                last_access_tstamp = ticks_now();
                nr_evictions = 0;

                gheap_trigger = new trigger;
                sanitize_struct_trigger(gheap_trigger);
                gheap_trigger->step = G_HEAP_FREQ;

                //private heap initialization
                file_heap_node_ids = nullptr;
                file_heap = nullptr;

#endif //ENABLE_EVICTION

#ifdef ENABLE_MINCORE_DEBUG
                mmap_addr = nullptr; // Initialize mmap address to nullptr
                mmap_fd = -1; // Initialize mmap fd to -1
#endif // ENABLE_MINCORE_DEBUG
        }

        ~inode(){

                /*clearing the fdlist*/
                fdlist_index = -1;
                memset(fdlist, 0, MAX_FD_PER_INODE*sizeof(struct fd_info));

#ifdef ENABLE_PER_INODE_BITMAP
                /*cache_state destroy*/
                destroy_bitmap(this);
#endif //ENABLE_PER_INODE_BITMAP

                heap_id = -1;
                one_operation_done = false;
                nr_accesses = 0;
                last_access_tstamp = 0;
                nr_evictions = 0;
#if defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP)
                _dest_pvt_heap(this);
#endif //ENABLE_EVICTION && ENABLE_PVT_HEAP

                /**
                 * we are not cleaning ENABLE_MINCORE_DEBUG
                 * variables because they are not 
                 * used during production runs for now.
                 * Will have to identify if the mmap_fd is
                 * live and the addr is valid etc.
                 */

                unlinked = true;
                marked_unlinked = true;
                nr_links = 0;

                /**
                 * identifying variables are being reset at the end so that
                 * the inode can be identified if error occurs during destruction
                 */
                memset(filename, 0, PATH_MAX);
                ino = 0UL;
                dev_id = 0UL;
        }
};

#endif
