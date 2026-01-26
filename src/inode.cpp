#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
// #include <malloc.h>

#include <iostream>
#include <mutex>
#include <new>
#include <vector>
#include <stdexcept>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "prefetch_evict.hpp"
#include "inode.hpp"
#include "utils/shim/shim.hpp"


struct trigger *nr_unlinks_for_imap_cleanup = nullptr;

//lock for updates to i_map XXX:CHECK IF READERS WRITERS LOCK REQUIRED
std::mutex i_map_lock;
struct hashtable *i_map;
std::atomic_flag i_map_init;

/*Required initialization for hashtable*/
DEFINE_HASHTABLE_INSERT(insert_some, struct key, struct value);
DEFINE_HASHTABLE_SEARCH(search_some, struct key, struct value);
DEFINE_HASHTABLE_REMOVE(remove_some, struct key, struct value);


/**
 * 1. It reduces a 64-bit value into 32 bits while trying to
 * preserve as much entropy as possible.
 * 2. XOR is cheap and “folds” the high bits into the low bits,
 * so changes in either half affect the result.
 * 3. This avoids just discarding the high bits, which would lose
 * uniqueness if the high bits vary more than the low bits.
 */
static inline uint32_t fold64to32(uint64_t v) {
        return (uint32_t)v ^ (uint32_t)(v >> 32);
}

static inline unsigned int hashfromkey(void *ky)
{
        struct key *k = (struct key *)ky;

        /* --- dev_t -> 32-bit fold --- */
        uint32_t dev_fold;
        #if defined(__SIZEOF_DEV_T__) && (__SIZEOF_DEV_T__ > 4)
        {
                uint64_t d = (uint64_t)k->dev_id;
                dev_fold = fold64to32(d);
        }
        #else
                dev_fold = (uint32_t)k->dev_id;
        #endif

        /* --- ino_t -> 32-bit fold --- */
        uint32_t ino_fold;
        #if defined(__SIZEOF_INO_T__) && (__SIZEOF_INO_T__ > 4)
        {
                uint64_t i = (uint64_t)k->ino;    /* ino_t may be 64-bit */
                ino_fold = fold64to32(i);
        }
        #else
                ino_fold = (uint32_t)k->ino;          /* 32-bit (or smaller) ino_t */
        #endif

        /* --- pack into 64 bits and Murmur finalizer --- */
        uint64_t x = ((uint64_t)ino_fold << 32) | dev_fold;

        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;

        return (unsigned int)x;  /* 32-bit table expects 32-bit hash */
}

static int equalkeys(void *k1, void *k2){
        struct key *key1 = (struct key *)k1;
        struct key *key2 = (struct key *)k2;

        return (key1->ino == key2->ino) && (key1->dev_id == key2->dev_id);
}

struct hashtable *init_inode_map(void){
        return create_hashtable(MAX_IMAP_FILES, hashfromkey, equalkeys);
}


/**
 * All operations on the i_map hashtable
 *
 * NOTE: i_map_lock management has to be handled by the caller
 */

/*
 * takes inode number and a pointer to the uinode 
 * inserts this uinode to the i_map
 * returns true if successful, false otherwise
 */
int insert_to_hashtable(ino_t ino, dev_t dev_id, void *val){

        int ret = false;
        struct key *key = (struct key*)malloc(sizeof(struct key));
        struct value *v = (struct value *)malloc(sizeof(struct value));

        if(!key || !v){
                SPEEDYIO_FPRINTF("%s:ERROR unable to allocate memory\n", "SPEEDYIO_ERRCO_0089\n");
                KILLME();
                free(key);
                free(v);
                goto insert_to_hashtable_exit;
        }
        key->ino = ino;
        key->dev_id = dev_id;
        v->value = val;

        if(!insert_some(i_map, key, v)){
                goto insert_to_hashtable_exit;
        }

        ret = true;

insert_to_hashtable_exit:
        return ret;
}

/*
 * takes the inode number and returns the struct value
 * returns NULL if some error occured
 */
struct value *get_from_hashtable(ino_t ino, dev_t dev_id){

        struct value *val= NULL;
        struct key key;
        key.ino = ino;
        key.dev_id = dev_id;

        val = search_some(i_map, &key);

get_from_hashtable_exit:
        return val;
}


/*
 * takes the inode number and removes its value from i_map
 * returns struct value *
 * it is the callers job to free struct value and its contents
 */
struct value *remove_from_hashtable(ino_t ino, dev_t dev_id){
        struct key key;
        struct value *val = nullptr;

        key.ino = ino;
        key.dev_id = dev_id;

        val = remove_some(i_map, &key);

remove_from_hashtable_exit:
        return val;
}


/**
 * Cleans uinodes which are not being used by anyone
 */
void iter_i_map_and_put_unused(void){
        int i = 0;
        int nr_uinodes_put = 0;
        int nr_iterated = 0;
        struct entry *entry = nullptr;
        struct value *val = nullptr;
        struct key *key = nullptr;
        struct inode *uinode = nullptr;
        struct inode *val_uinode = nullptr;

        // cprintf("%s:INFO Starting\n", __func__);

continue_iterating:
        i_map_lock.lock();

        for (; i < i_map->tablelength; i++){
                entry = i_map->table[i];
                while (entry){
                        key = nullptr;
                        val = nullptr;
                        uinode = nullptr;

                        nr_iterated += 1;
                        key = (struct key *)entry->k;
                        if(!key){
                                cfprintf(stderr, "%s:ERROR key is nullptr\n", __func__);
                                goto get_next_entry;
                        }

                        val = (struct value *)entry->v;
                        if(!val){
                                cfprintf(stderr, "%s:ERROR for key->{ino:%lu, dev_id:%lu}, value is nullptr\n",
                                                __func__, key->ino, key->dev_id);
                                goto get_next_entry;
                        }

                        uinode = (struct inode *)val->value;
                        if(!uinode){
                                cfprintf(stderr, "%s:ERROR for key->{ino:%lu, dev_id:%lu}, uinode is nullptr\n",
                                                __func__, key->ino, key->dev_id);
                                goto get_next_entry;
                        }

                        // printf("key:{ino:%lu, dev:%lu} file:%s\n", key->ino, key->dev_id, uinode->filename);
                        if(uinode->unlinked_lock.try_lock()){
                                if(uinode->is_deleted()){
                                        /**
                                         * Since this uinode is_deleted.
                                         * We can assume the following here since we are holding
                                         * the uinode->unlinked_lock:
                                         * 1. It has been completely unlinked, ie. not in the middle of it.
                                         * 2. It has not been reused.
                                         * 3. It is not being victimized by the evictor thread.
                                         *
                                         * Do some sanity checks before freeing the uinode.
                                         */

                                        /**
                                         * nr_links should be == 1 because the final caller to
                                         * unlink(filename) and close(fd) doesnt update the nr_links to 0.
                                         */
                                        if(uinode->nr_links > 1){
                                                cfprintf(stderr, "%s:UNUSUAL {ino:%lu, dev_id:%lu} is deleted and nr_links:%lu.. Skipping\n",
                                                        __func__, uinode->ino, uinode->dev_id, uinode->nr_links);

                                                uinode->unlinked_lock.unlock();
                                                goto get_next_entry;
                                        }

                                        /**
                                         * There should be no fds in the fdlist_index.
                                         * Else the uinode shouldnt have been is_deleted()
                                         * in the first place.
                                         */
                                        if(uinode->fdlist_index >= 0){
                                                cfprintf(stderr, "%s:UNUSUAL {ino:%lu, dev_id:%lu} is deleted and fdlist_index:%d.. Skipping\n",
                                                        __func__, uinode->ino, uinode->dev_id, uinode->fdlist_index);

                                                uinode->unlinked_lock.unlock();
                                                goto get_next_entry;
                                        }
                                        goto put_uinode;
                                }else{
                                        /**
                                         * It is a live uinode, not to be put.
                                         */
                                        uinode->unlinked_lock.unlock();
                                        goto get_next_entry;
                                }
                        }

                        //Ignore this entry if unable to take unlinked_lock
get_next_entry:
                        entry = entry->next;
                }
        }

        //Done with all the elements of the i_map.
        i_map_lock.unlock();
        goto exit;

put_uinode:
        /**
         * At this point we are holding the following locks:
         * 1. i_map_lock - imap
         * 2. uinode->unlinked_lock - this uinode
         *
         * Do the following:
         * 0. Remove this uinode from the i_map, so no one sees this inode anymore
         * 1. release i_map_lock, so others can work on it
         * 2. Cleanup this uinode
         * 3. Increment out nr_uinodes_cleaned counter
         * 4. Go back to iterating through the imap
         */

        val = remove_from_hashtable(uinode->ino, uinode->dev_id);
        i_map_lock.unlock();

        val_uinode = (struct inode*)val->value;
        if(!val_uinode){
                cfprintf(stderr, "%s:ERROR got uinode nullptr from return_from_hashtable input: {ino:%lu, dev_id:%lu}\n",
                                __func__, uinode->ino, uinode->dev_id);
                KILLME();
        }
        if(uinode != val_uinode
                || uinode->ino != val_uinode->ino
                || uinode->dev_id != val_uinode->dev_id
        ){
                cfprintf(stderr, "%s:ERROR different uinode: remove_from_hashtable input:{ino:%lu, dev_id:%lu}, returned:{ino:%lu, dev_id:%lu}\n",
                                __func__, uinode->ino, uinode->dev_id, val_uinode->ino, val_uinode->dev_id);
                KILLME();
        }

        //fprintf(stderr, "%s:INFO freeing uinode for file:%s ino:%d\n", __func__, uinode->filename, uinode->ino);

        delete val_uinode;
        free(val);

        // printf("%s:INFO deleted ino:%lu\n", __func__, val_uinode->ino);

        nr_uinodes_put += 1;

        val_uinode = nullptr;
        uinode = nullptr;
        val = nullptr;


        /**
         * Here we will miss if there are multiple entries in the same bucket
         * in i_map, but that is fine; we can get that in the next sweep
         */
        i += 1;
        goto continue_iterating;

exit:
        if(nr_iterated > 0)
        {
                cprintf("%s:INFO nr_uinodes_put:%d out of nr_iterated:%d\n", __func__, nr_uinodes_put, nr_iterated);
        }else{
                cprintf("%s: exiting\n", __func__);
        }

        /**
         * A lot of times the allocator chooses not to munmap memory
         * that has been freed here in order to help with future
         * allocations. To test if my frees were actually working
         * I tried using malloc_trim to reduce memory hoarding.
         * It is a best effort call, so don't expect all the unused mem
         * to just be freed using this. But it is an indicator.
         *
         * malloc_info(0, stdout); tells how much memory is being
         * hoarded by malloc for future allocations. That is a good
         * indicator that this is working.
         */
        // if(nr_uinodes_put > 0){
        //         malloc_trim(0);
        // }

        return;
}


/**
 * Thread that invokes cleanup of unused inodes
 */
void *bg_inode_cleaner(void *arg){

        struct timespec start, end;
        long seconds, nanoseconds;
        double elapsed_ms;

        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

        nr_unlinks_for_imap_cleanup = new trigger;
        nr_unlinks_for_imap_cleanup->step = CLEANUP_AFTER_NR_UNLINKS;

        while(true){
sleep:
                /*stops here if thread killed*/
                pthread_testcancel();

                sleep(BG_CLEANUP_SLEEP);

                if(!trigger_check(nr_unlinks_for_imap_cleanup)){
                        // cprintf("%s:INFO current nr_unlinked:%ld last:%ld\n", __func__,
                        //         nr_unlinks_for_imap_cleanup->now, nr_unlinks_for_imap_cleanup->last);
                        goto sleep;
                }

                // clock_gettime(CLOCK_MONOTONIC, &start);

                iter_i_map_and_put_unused();

                // clock_gettime(CLOCK_MONOTONIC, &end);

                // seconds = end.tv_sec - start.tv_sec;
                // nanoseconds = end.tv_nsec - start.tv_nsec;
                // elapsed_ms = (seconds * 1000.0) + (nanoseconds / 1.0e6);

                // cprintf("%s:INFO Time taken in iter_i_map_and_put_unused: %.3f ms\n", __func__, elapsed_ms);

        }
}


/**
 * uses get_from_hashtable to return the struct inode associated with ino and dev_id
 */
struct inode *get_uinode_from_hashtable(ino_t ino, dev_t dev_id){
        struct value *uinode_exists = nullptr;
        struct inode *uinode = nullptr;

        if(ino < 1 || dev_id < 0){
                SPEEDYIO_FPRINTF("%s:ERROR ino:%lu or dev_id:%lu is invalid\n", "SPEEDYIO_ERRCO_0091 %lu %lu\n", ino, dev_id);
                goto exit_get_uinode_from_hashtable;
        }

        i_map_lock.lock();

        uinode_exists = get_from_hashtable(ino, dev_id);
        if(uinode_exists){
                uinode = (struct inode*)uinode_exists->value;
                if(unlikely(!uinode)){
                        SPEEDYIO_FPRINTF("%s:ERROR inode entry exists but no valid inode {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0092 %lu %lu\n", ino, dev_id);
                        KILLME();
                        uinode = nullptr;
                        goto unlock_and_exit_get_uinode_from_hashtable;
                }
                if(uinode->ino != ino || uinode->dev_id != dev_id){
                        SPEEDYIO_FPRINTF("%s:ERROR inode doesn't match {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0093 %lu %lu\n", ino, dev_id);
                        KILLME();
                        uinode = nullptr;
                        goto unlock_and_exit_get_uinode_from_hashtable;
                }
        }

unlock_and_exit_get_uinode_from_hashtable:
        i_map_lock.unlock();

exit_get_uinode_from_hashtable:
        return uinode;
}


/*All Operations on the cache state*/

void alloc_bitmap(struct inode *uinode){
        if(likely(uinode)){
                uinode->cache_rwlock.lock_write();

                uinode->cache_state = BitArrayCreate(NR_BITMAP_BITS);
                if(unlikely(!uinode->cache_state)){
                        SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for bitmap\n", "SPEEDYIO_ERRCO_0094\n");
                        KILLME();
                        goto alloc_bitmap_unlock_exit;
                }
                BitArrayClearAll(uinode->cache_state);
                debug_printf("%s: Allocating cache state to {ino:%lu, dev:%lu} with %lu bits\n",
                                __func__, uinode->ino, uinode->dev_id, NR_BITMAP_BITS);
        }
        else{
                goto alloc_bitmap_exit;
        }
alloc_bitmap_unlock_exit:
        uinode->cache_rwlock.unlock_write();
alloc_bitmap_exit:
        return;
}

/*frees the cache state of a given uinode*/
void destroy_bitmap(struct inode *uinode){

        if(unlikely(!uinode)){
                goto destroy_bitmap_exit;
        }

        uinode->cache_rwlock.lock_write();
        if(likely(uinode->cache_state)){
                BitArrayDestroy(uinode->cache_state);
        }
        uinode->cache_state = nullptr;
        uinode->cache_rwlock.unlock_write();

        debug_printf("%s: Destroying cache state for uinode %lu\n", __func__, uinode->ino);

destroy_bitmap_exit:
        return;
}

void set_range_bitmap(struct inode *uinode, unsigned long start_bit, unsigned long num_bits){

        if(!uinode){
                goto set_range_bitmap_exit;
        }

        /*
         * XXX: Here we are using a readers lock instead of a writers lock eventhough
         * set range is writing to the bitmap
         * but we are using a readers lock because
         * 1. If two threads are setting/clearing a non overlapping range(different ull in the bitmap)
         *      - There will be no conflicts hence its okay
         * 2. if two threads are setting/clearing an overlapping range (same ull in the bitmap)
         *      - An approximate result - may show either update
         *
         *TODO:Range Locks for bitmaps if this approximation is not working
         */
        uinode->cache_rwlock.lock_read();
        if(likely(uinode->cache_state)){
                BitArraySetRange(uinode->cache_state, start_bit, num_bits);
        }
        uinode->cache_rwlock.unlock_read();

set_range_bitmap_exit:
        return;
}

void clear_range_bitmap(struct inode *uinode, unsigned long start_bit, unsigned long num_bits){
        if(!uinode){
                goto clear_range_bitmap_exit;
        }

        uinode->cache_rwlock.lock_read();
        if(likely(uinode->cache_state)){
                BitArrayClearRange(uinode->cache_state, start_bit, num_bits);
        }
        uinode->cache_rwlock.unlock_read();

clear_range_bitmap_exit:
        return;
}


//clears the whole cache_state for a given uinode
//returns true if successful else false
bool clear_full_bitmap(struct inode *uinode){
        bool ret = true;

        if(unlikely(!uinode || !uinode->cache_state)){
                SPEEDYIO_FPRINTF("%s:ERROR uinode==NULL or uinode->cache_state==NULL", "SPEEDYIO_ERRCO_0095");
                ret = false;
                KILLME();
                goto exit_clear_full_bitmap;
        }

        uinode->cache_rwlock.lock_write();
        BitArrayClearAll(uinode->cache_state);
        uinode->cache_rwlock.unlock_write();

        debug_printf("%s: done clearing cache_state for {ino:%lu, dev:%lu}\n", __func__, uinode->ino, uinode->dev_id);
exit_clear_full_bitmap:
        return ret;
}

/*
 * Returns first set bit for the given range of bits
 * if returned -2 -> !uinode or num_bits == 0
 * if returned -1 -> no bits were set in the given range
 * if returned a positive number -> that was the first bit set in the given bit range
 */
long first_set_bit(struct inode *uinode, unsigned long start_bit, unsigned long num_bits){
        long ret = -2;

        if(unlikely(!uinode || num_bits == 0)){
                goto first_set_bit_exit;
        }

        uinode->cache_rwlock.lock_read();
        if(likely(uinode->cache_state)){
                ret = BitArrayGetFirstSetBit(uinode->cache_state, start_bit, num_bits);
        }
        uinode->cache_rwlock.unlock_read();

first_set_bit_exit:
        return ret;
}


/*
 * Returns first unset bit for the given range of bits
 * if returned -2 -> !uinode or num_bits == 0
 * if returned -1 -> no bits were unset in the given range
 * if returned a positive number -> that was the first unset bit in the given bit range
 */
long first_unset_bit(struct inode *uinode, unsigned long start_bit, unsigned long num_bits){
        long ret = -2;

        if(unlikely(!uinode || num_bits == 0)){
                goto first_unset_bit_exit;
        }

        uinode->cache_rwlock.lock_read();
        if(likely(uinode->cache_state)){
                ret = BitArrayGetFirstUnsetBit(uinode->cache_state, start_bit, num_bits);
        }
        uinode->cache_rwlock.unlock_read();

first_unset_bit_exit:
        return ret;
}


bool bits_are_set(struct inode *uinode, unsigned long start_pos, unsigned long num_bits){
        bool ret = false;
        if(unlikely(!uinode || num_bits == 0)){
                goto bits_are_set_exit;
        }

        uinode->cache_rwlock.lock_read();
        if(likely(uinode->cache_state)){
                ret = BitArrayIsSet(uinode->cache_state, start_pos, num_bits);
        }
        uinode->cache_rwlock.unlock_read();

bits_are_set_exit:
        return ret;
}


/*All operations on uinode*/

/**
 * Clears out all contents of a given struct inode
 * returns true if everything was okay. else false
 *
 * NOTE: This function doesnt take any locks by itself.
 * Whatever locks need taking, need to be taken by the
 * caller.
 * Exceptions: clear_pvt_heap, clear_full_bitmap, clear_uinode_fdlist
 * take their own locks.
 */
bool sanitize_uinode(struct inode *uinode){
        bool ret = true;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr\n", "SPEEDYIO_ERRCO_0097\n");
                ret = false;
                KILLME();
                goto exit_sanitize_uinode;
        }

        /*clearing fdlist*/
        if(!clear_uinode_fdlist(uinode)){
                goto exit_sanitize_uinode;
        }

        /*clearing bitmap*/
#ifdef ENABLE_PER_INODE_BITMAP
        if(unlikely(!clear_full_bitmap(uinode))){
                SPEEDYIO_FPRINTF("%s:ERROR clear_full_bitmap did not work for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0098 %lu %lu\n", uinode->ino, uinode->dev_id);
                ret = false;
                KILLME();
                goto exit_sanitize_uinode;
        }
#endif //ENABLE_PER_INODE_BITMAP

#ifdef ENABLE_EVICTION
        uinode->heap_id = -1;
        uinode->one_operation_done = false;
        uinode->nr_accesses = 0;
        uinode->last_access_tstamp = ticks_now();
        uinode->nr_evictions = 0;

        sanitize_struct_trigger(uinode->gheap_trigger);
        uinode->gheap_trigger->step = G_HEAP_FREQ;

#ifdef ENABLE_PVT_HEAP
        if(!clear_pvt_heap(uinode)){
                SPEEDYIO_FPRINTF("%s:ERROR clear_pvt_heap did not work for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0099 %lu %lu\n", uinode->ino, uinode->dev_id);
                ret = false;
                KILLME();
                goto exit_sanitize_uinode;
        }
#endif //ENABLE_PVT_HEAP
#endif //ENABLE_EVICTION

#ifdef ENABLE_MINCORE_DEBUG
        uinode->mmap_addr = nullptr; // Initialize mmap address to nullptr
        uinode->mmap_fd = -1; // Initialize mmap fd to -1
#endif // ENABLE_MINCORE_DEBUG

        uinode->ino = 0UL;
        uinode->dev_id = 0UL;
        memset(uinode->filename, 0, PATH_MAX);
        uinode->unlinked = false;
        uinode->marked_unlinked = false;
        uinode->nr_links = 0;

exit_sanitize_uinode:
        return ret;
}

/**
 * Updates the number of links from fstat family functions.
 *
 * Set take_unlink_lock = false if and only if the caller already
 * holds it. else always set it to true.
 */
bool update_nr_links(struct inode *uinode, nlink_t nr_links, bool take_unlinked_lock){
        bool ret = true;

        if(!uinode || nr_links < 1 || uinode->is_deleted()){
                SPEEDYIO_FPRINTF("%s:ERROR no uinode or it is deleted or nr_links < 1\n", "SPEEDYIO_ERRCO_0100\n");
                ret = false;
                KILLME();
                goto exit_update_nr_links;
        }

        if(take_unlinked_lock){
                uinode->unlinked_lock.lock();
        }

        uinode->nr_links = nr_links;

        if(take_unlinked_lock){
                uinode->unlinked_lock.unlock();
        }
exit_update_nr_links:
        return ret;
}


/**
 * Checks if fd is already in fdlist for this uinode
 * add new if it doesn't exist.
 * returns true if done successfully, else false.
 */
bool add_fd_to_fdlist(struct inode *uinode, int fd, int open_flags, off_t seek_head){
        bool ret = true;
        int i;

        if(fd < 3 || seek_head < 0){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d or seek_head:%ld input is insane\n", "SPEEDYIO_ERRCO_0101 %d %ld\n", fd, seek_head);
                ret = false;
                KILLME();
                goto exit_add_fd_to_fdlist;
        }

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0102\n");
                ret = false;
                KILLME();
                goto exit_add_fd_to_fdlist;
        }

        uinode->fdlist_lock.lock();

        /*check if fd is in already*/
        for(i=0; i <= uinode->fdlist_index; i++){
                if(uinode->fdlist[i].fd == fd){
                        /**
                         * Duplicate open for the same file returning the same fd.
                         * Since two threads with the same fd share the same seek_head
                         * We should not change anything here.
                         */
                        SPEEDYIO_FPRINTF("%s:WARNING same fd:%d being added to {ino:%lu, dev:%lu} again\n", "SPEEDYIO_WARNCO_0006 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                        goto unlock_exit;
                }
        }

        /*This fd is new to the fdlist*/

        uinode->fdlist_index += 1;

        //Check if fdlist_index is not overflowing MAX_FD_PER_INODE
        if(uinode->fdlist_index >= MAX_FD_PER_INODE){
                SPEEDYIO_FPRINTF("%s:MISCONFIG increase MAX_FD_PER_INODE:%d fdlist_index:%d for {ino:%lu, dev:%lu} Aborting !!!\n", "SPEEDYIO_MISCONFIGCO_0004 %d %d %lu %lu\n", MAX_FD_PER_INODE, uinode->fdlist_index, uinode->ino, uinode->dev_id);
                uinode->fdlist_index -= 1;

                KILLME();
                ret = false;
                goto unlock_exit;
        }

        /*all checks done, adding new fd to fdlist*/
        uinode->fdlist[uinode->fdlist_index].fd = fd;
        uinode->fdlist[uinode->fdlist_index].open_flags = open_flags;
        uinode->fdlist[uinode->fdlist_index].seek_head = seek_head;
        uinode->fdlist[uinode->fdlist_index].fadv_seq = true; //OS assumes a new fd to be sequential
unlock_exit:
        uinode->fdlist_lock.unlock();

exit_add_fd_to_fdlist:
        return ret;
}

/**
 * removes an fd from uinode's fd list.
 * returns -1 if failed, 1 if successful, 0 if could not find fd in fdlist
 */
int remove_fd_from_fdlist(struct inode *uinode, int fd){
        int ret = 0;
        int i;

        if(fd < 3){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d input is insane\n", "SPEEDYIO_ERRCO_0103 %d\n", fd);
                ret = -1;
                goto exit;
        }

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0104\n");
                ret = -1;
                goto exit;
        }

        uinode->fdlist_lock.lock();

        if(uinode->fdlist_index < 0){
                SPEEDYIO_FPRINTF("%s:ERROR fdlist_index:%d for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0105 %d %lu %lu\n", uinode->fdlist_index, uinode->ino, uinode->dev_id);
                ret = 0;
                goto unlock_and_exit;
        }

        for(i=0; i <= uinode->fdlist_index; i++){
                if(uinode->fdlist[i].fd == fd){
                        /*found this fd.*/
                        if(i == uinode->fdlist_index){
                                uinode->fdlist[i].fd = -1;
                                uinode->fdlist[i].seek_head = -1;
                                uinode->fdlist[i].open_flags = -1;
                                uinode->fdlist[i].fadv_seq = -1;
                        }else{
                                uinode->fdlist[i].fd = uinode->fdlist[uinode->fdlist_index].fd;
                                uinode->fdlist[i].open_flags = uinode->fdlist[uinode->fdlist_index].open_flags;
                                uinode->fdlist[i].seek_head = uinode->fdlist[uinode->fdlist_index].seek_head;
                                uinode->fdlist[i].fadv_seq = uinode->fdlist[uinode->fdlist_index].fadv_seq;
                        }
                        ret = 1;
                        uinode->fdlist_index -= 1;
                        break;
                }
                ret = 0;
        }

unlock_and_exit:
        uinode->fdlist_lock.unlock();

#ifdef ENABLE_MINCORE_DEBUG
        if (ret == 1) {
                update_mmap_fd(uinode);
        }
#endif // ENABLE_MINCORE_DEBUG

exit:
        return ret;
}


bool clear_uinode_fdlist(struct inode *uinode){
        bool ret = true;

        uinode->fdlist_lock.lock();
        uinode->fdlist_index = -1;
        if(likely(uinode->fdlist)){
                memset(uinode->fdlist, 0, MAX_FD_PER_INODE*sizeof(struct fd_info));
        }else{
                SPEEDYIO_FPRINTF("%s:ERROR {ino:%lu, dev:%lu} fdlist is nullptr\n", "SPEEDYIO_ERRCO_0096 %lu %lu\n", uinode->ino, uinode->dev_id);
                ret = false;
                KILLME();
                goto exit_clear_uinode_fdlist;
        }

exit_clear_uinode_fdlist:
        uinode->fdlist_lock.unlock();
        return ret;
}

/**
 * assuming there has been a read or write, update the seek head of that fd
 * in this uinode. returns the old offset.
 *
 * if set_to == true -> new seek_head = bytes.
 * else seek_head += bytes
 *
 * returned value will be -1 if error.
 * 
 * Note: In 64 bit machines the following is the implementation:
 * 1. off_t - long long int (64 bit)
 * 2. size_t - unsigned long long int (64 bit)
 * 3. ssize_t - long long int (64 bit)
 * So conversion between ssize_t and off_t is okay
 * Conversion of a off_t to size_t is okay
 * Conversion of size_t to off_t is okay only if it < 2^63 - 1
 * else, the value will overflow 
 */
ssize_t update_fd_seek_pos(struct inode *uinode, int fd, off_t bytes, bool set_to){

        ssize_t ret = -1;
#ifdef DEBUG_SEEK_POS
        ssize_t curr_pos;
        off_t seek_pos;
#endif
        int i;

        if(fd < 3){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d input is insane\n", "SPEEDYIO_ERRCO_0106 %d\n", fd);
                goto exit_update_fd_seek_pos;
        }

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr\n", "SPEEDYIO_ERRCO_0107\n");
                goto exit_update_fd_seek_pos;
        }

        uinode->fdlist_lock.lock();
        for(i = 0; i <= uinode->fdlist_index; i++){
                if(uinode->fdlist[i].fd == fd){
                        ret = uinode->fdlist[i].seek_head;

                        if(set_to){
                                uinode->fdlist[i].seek_head = bytes;
                        }else{
                                /*forward read/write. add bytes.*/
                                uinode->fdlist[i].seek_head += bytes;
                        }
#ifdef DEBUG_SEEK_POS
                                curr_pos = uinode->fdlist[i].seek_head;
                                seek_pos = real_lseek(fd, 0, SEEK_CUR);
                                if(curr_pos != seek_pos){
                                        SPEEDYIO_FPRINTF("%s:ERROR fd:%d curr_pos:%ld whereas ground truth:%lu\n", "SPEEDYIO_ERRCO_0108 %d %ld %lu\n", fd, curr_pos, seek_pos);
                                        KILLME();
                                }
#endif //DEBUG_SEEK_POS
                }
        }
        uinode->fdlist_lock.unlock();

        if(unlikely(ret == -1)){
                SPEEDYIO_FPRINTF("%s:ERROR unable to find fd:%d in {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0109 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                goto exit_update_fd_seek_pos;
        }

        if(unlikely(ret < 0)){
                ret = -1;
                SPEEDYIO_FPRINTF("%s:ERROR seek_head has overflown for fd:%d {ino:%lu, dev:%lu} seek_head:%ld\n", "SPEEDYIO_ERRCO_0110 %d %lu %lu %ld\n", fd, uinode->ino, uinode->dev_id,ret);
                goto exit_update_fd_seek_pos;
        }

exit_update_fd_seek_pos:
        return ret;
}

/*Resets the seek_heads*/
bool reset_all_fd_seek_pos(struct inode *uinode){
        bool ret = true;
        int i;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0111\n");
                ret = false;
                goto exit_reset_all_fd_seek_pos;
        }

        uinode->fdlist_lock.lock();
        for(i = 0; i <= uinode->fdlist_index; i++){
                uinode->fdlist[i].seek_head = 0;
        }
        uinode->fdlist_lock.unlock();

        debug_printf("%s: {ino:%lu, dev:%lu} reset %d fd's seek_heads to 0\n",
                        __func__, uinode->ino, uinode->dev_id, i+1);

exit_reset_all_fd_seek_pos:
        return ret;
}


/**
 * finds the fd in this uinode's fdlist and returns the open flags
 * returns -1 if fd was not found in fdlist
 */
int get_open_flags_from_uinode(struct inode *uinode, int fd){
        int ret = -1;
        bool err = true;
        int i;

        if(fd < 3){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d input is insane\n", "SPEEDYIO_ERRCO_0112 %d\n", fd);
                goto exit_get_open_flags_from_uinode;
        }

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0113\n");
                goto exit_get_open_flags_from_uinode;
        }

        uinode->fdlist_lock.lock();
        for(i = 0; i <= uinode->fdlist_index; i++){
                if(uinode->fdlist[i].fd == fd){
                        ret = uinode->fdlist[i].open_flags;
                        err = false;
                        break;
                }
        }
        uinode->fdlist_lock.unlock();

        if(err){
                SPEEDYIO_FPRINTF("%s:ERROR could not find fd:%d in {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0114 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                ret = -1;
        }

exit_get_open_flags_from_uinode:
        return ret;
}


/**
 * Returns true if any of the valid fds on this uinode have fadv_seq = true
 *
 * NOTE: FADV_SEQ/FADV_NORMAL is a per-fd construct inside the OS ie. if
 * this uinode has fd1 and fd2. If FADV_SEQ is called for fd1 and FADV_RANDOM
 * for fd2, OS will consider prefetching only for reads done via fd1.
 * Also, these advises are also relevant only for the offset and size passed with
 * it.
 *
 * Here we are still returning a coarse grained result to reduce accounting complexity.
 * Moreover:
 * 1. Discounting some prefetched page from pvt_heap will be a big problem since those
 * pages will essentially never be evicted. (OS cleanup operations never get triggered)
 * 2. Some extra portions in pvt_heap don't significantly increase memory usage.
 * 3. Attempting eviction on pages not in memory doesnt increase OS overheads.
 */
bool get_fadv_from_uinode(struct inode *uinode){
        bool ret = false;
        int i;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0113\n");
                goto exit_get_fadv_from_uinode;
        }

        uinode->fdlist_lock.lock();
        for(i = 0; i <= uinode->fdlist_index; i++){
                        ret |= uinode->fdlist[i].fadv_seq;
        }
        uinode->fdlist_lock.unlock();

        if(ret != false && ret != true){
                fprintf(stderr, "%s:ERROR ret was neither true nor false uinode{ino:%lu, dev_id:%lu}. check fdlist\n",
                                __func__, uinode->ino, uinode->dev_id);
                ret = false;
        }

exit_get_fadv_from_uinode:
        return ret;
}


/**
 * returns -1 if fd was not found in fdlist
 */
int set_fadv_on_fd_uinode(struct inode *uinode, int fd, bool is_seq){
        int ret = -1;
        bool err = true;
        int i;

        if(fd < 3){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d input is insane\n", "SPEEDYIO_ERRCO_0112 %d\n", fd);
                goto exit_set_fadv_on_fd_uinode;
        }

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid\n", "SPEEDYIO_ERRCO_0113\n");
                goto exit_set_fadv_on_fd_uinode;
        }

        uinode->fdlist_lock.lock();
        for(i = 0; i <= uinode->fdlist_index; i++){
                if(uinode->fdlist[i].fd == fd){
                        uinode->fdlist[i].fadv_seq = is_seq;
                        err = false;
                        break;
                }
        }
        uinode->fdlist_lock.unlock();

        if(err){
                cfprintf(stderr, "%s:ERROR could not find fd:%d in {ino:%lu, dev:%lu}\n",
                                __func__, fd, uinode->ino, uinode->dev_id);
                ret = -1;
        }

exit_set_fadv_on_fd_uinode:
        return ret;
}


/**
 * Adds a new uinode to i_map and/or updates an existing uinode with new fd
 */
struct inode *add_fd_to_inode(int fd, int open_flags, const char *filename){
        ino_t ino;
        dev_t dev_id;
        int err;
        struct stat file_stat;
        struct value *uinode_exists = nullptr;
        struct inode *uinode = nullptr;
        struct inode *ret = nullptr;
        bool allocated_new_uinode = false;
        off_t seek_head = 0;
        err = -1;
        int nr_unlinked_lock_retries = 0;

        /*We don't handle stdout and stderr*/
        if(unlikely(fd < 3)){
                goto exit_add_fd_to_inode;
        }

        err = fstat(fd, &file_stat);
        if(unlikely(err == -1)){
                SPEEDYIO_FPRINTF("%s:ERROR when fstat(%s) called for fd:%d\n", "SPEEDYIO_ERRCO_0115 %s %d\n", strerror(errno), fd);
                goto exit_add_fd_to_inode;
        }

        if(!S_ISREG(file_stat.st_mode)){
                /*this file is not a regular file. skip it*/
                debug_printf("%s: this file is not a regular file. Ignoring it\n", __func__);
                goto exit_add_fd_to_inode;
        }

        /**
         * TODO: Here we should add another check to
         * ignore files whose dev_id is either equal to
         * that of /proc or /sys
         */

        ino = file_stat.st_ino;
        dev_id = file_stat.st_dev;

        debug_fprintf(stderr, "%s:INFO filename:%s, fd:%d, ino:%lu, dev_id:%lu\n",
                        __func__, filename, fd, ino, dev_id);

        /*seek_head will be equal to filesize if open_flag has O_APPEND*/
        if(open_flags & O_APPEND){
                seek_head = file_stat.st_size;
                debug_printf("%s:O_APPEND for fd:%d seek_head:%ld\n", __func__, fd, seek_head);
        }
        /*sometimes open syscall has both O_APPEND and O_TRUNC. O_TRUNC preceeds*/
        if(open_flags & O_TRUNC){
                seek_head = 0;
        }

        /**
         * XXX: Optimize this very large i_map_lock critical section.
         *
         * This is to prevent a potential race condition leading to duplicate entries
         * in the i_map since many threads could be opening the same file {ino, dev_id}
         * for the first time together.
         */
        i_map_lock.lock();

        uinode_exists = get_from_hashtable(ino, dev_id);
        if(!uinode_exists){
alloc_new_uinode:
                /*Allocating a new uinode*/
                debug_printf("%s: Allocating new struct uinode for {ino:%lu, dev:%lu}\n", __func__, ino, dev_id);
                try{
                        uinode = new struct inode;
                        allocated_new_uinode = true;
                }catch(std::bad_alloc& e){
                        SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for inode: %s\n", "SPEEDYIO_ERRCO_0116 %s\n", e.what());
                        uinode = nullptr;
                        goto exit_add_fd_to_inode;
                }

                /**
                 * Since this is a newly allocated uinode and has not been
                 * added to the hashtable, no one knows about it; so this
                 * lock doesnt do anything. It is just to keep locking
                 * consistent with older uinodes received from other paths
                 * of this function.
                 */
                uinode->unlinked_lock.lock();

                goto populate_full_uinode;
        }

        uinode = (struct inode*)uinode_exists->value;
        if(unlikely(!uinode)){
                SPEEDYIO_FPRINTF("%s:ERROR inode entry exists but no valid uinode {ino:%lu, dev:%lu} for fd:%d\n", "SPEEDYIO_ERRCO_0117 %lu %lu %d\n", ino, dev_id, fd);
                goto exit_add_fd_to_inode;
        }

retry_lock:
        if(!uinode->unlinked_lock.try_lock()){

                if(!uinode->is_deleted()){
                        /**
                         * Since not deleted, maybe:
                         * 1. The nr_links is being updated
                         * 2. The file is being victimized for eviction
                         * 3. Is being unlinked [check_fdlist_and_unlink]
                         *
                         * Just retry lock. It should reveal whats up with the
                         * uinode. If failed MAX_LOCK_RETRIES times, log it.
                         */
                        nr_unlinked_lock_retries += 1;

                        if(nr_unlinked_lock_retries > MAX_LOCK_RETRIES){
                                cfprintf(stderr, "%s:UNUSUAL THIS SHOULD NOT HAPPEN unable to take unlinked_lock "
                                        "on undeleted uinode->{ino:%lu, dev_id:%lu}, "
                                        "passed{ino:%lu, dev_id:%lu}. fd:%d, filename:%s retried %d times\n",
                                        __func__, uinode->ino, uinode->dev_id, ino, dev_id, fd, filename, nr_unlinked_lock_retries);
                                KILLME();
                        }
                        goto retry_lock;
                }else{
                        /**
                         * Unable to take unlinked_lock and it is unlinked.
                         * Which can mean that it is being put [iter_i_map_and_put_unused]
                         *
                         * Ideally this state CANNOT be reached since iter_i_map_and_put_unused
                         * takes i_map.lock() before starting to scan and the uinode to be put
                         * is first removed from the i_map before i_map.unlock().
                         *
                         * For EXT4, inode numbers are not repeated frequently
                         * but for XFS, they are reused aggressively. I have observed
                         * inode number reuse after just 2 deletions.
                         *
                         * So, at this point in this function, we can assume that this uinode
                         * has been chosen to get freed. So, we log it.
                         *
                         * If such a condition can occur, we should allocate a new uinode since
                         * the older one will be freed shortly. But that is not supported right now.
                         */
                        cfprintf(stderr, "%s:UNUSUAL THIS SHOULD NEVER HAPPEN unable to take unlinked_lock on deleted uinode{ino:%lu, dev_id:%lu}. Allocating a new one instead.\n",
                                        __func__, uinode->ino, uinode->dev_id);
                        KILLME();

                        /**
                         * XXX: This goto's behaviour is not tested.
                         */
                        // goto alloc_new_uinode;
                }
        }

        /**
         * A uinode already exists in the hashtable with the same {ino, dev_id}
         * and we have acquired the unlinked_lock so no one can manipulate its
         * unlinked boolean.
         */
        allocated_new_uinode = false;

        if(uinode->is_deleted()){
                /**
                 * The OS is reusing the {ino, dev_id}. Let us sanitize and reuse its uinode.
                 */
                debug_printf("%s:INFO inode for fd:%d {ino:%lu, dev:%lu} is_deleted(). sanitizing it\n", __func__, fd, ino, dev_id);
                sanitize_uinode(uinode);
                goto populate_full_uinode;
        }else{
                /**
                 * Valid, undeleted uinode. update the uinode
                 */
                debug_printf("%s:Valid {ino:%lu, dev:%lu} exists in hashtable, adding fd:%d\n", __func__, ino, fd, dev_id);

                if(uinode->ino != ino || uinode->dev_id != dev_id){
                        SPEEDYIO_FPRINTF("%s:ERROR uinode{ino:%lu, dev:%lu} provided{ino:%lu, dev:%lu} don't match.\n", "SPEEDYIO_ERRCO_0118 %lu %lu %lu %lu\n", uinode->ino, uinode->dev_id, ino, dev_id);
                        KILLME();
                }

                if(open_flags & O_TRUNC){
                        /**
                         * if this existing uinode is being opened with
                         * O_TRUNC, we need to reset the bitmaps and pvt_heap
                         */

                        debug_printf("%s:O_TRUNC for {ino:%lu, dev:%lu}, fd:%d\n",
                                        __func__, uinode->ino, uinode->dev_id, fd);

#ifdef ENABLE_PER_INODE_BITMAP
                        //clear bitmap
                        if(!clear_full_bitmap(uinode)){
                                SPEEDYIO_FPRINTF("%s:ERROR unable to clear_full_bitmap on existing {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0119 %lu %lu\n", uinode->ino, uinode->dev_id);
                                KILLME();
                        }
#endif //ENABLE_PER_INODE_BITMAP


#if defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP)
                        //clear pvt heap. global heap can remain as is
                        if(!clear_pvt_heap(uinode)){
                                SPEEDYIO_FPRINTF("%s:ERROR unable to clear_pvt_heap on existing {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0120 %lu %lu\n", uinode->ino, uinode->dev_id);
                                KILLME();
                        }
#endif //ENABLE_EVICTION && ENABLE_PVT_HEAP

                        //all existing fds related to this uinode will seek back to 0
                        if(!reset_all_fd_seek_pos(uinode)){
                                SPEEDYIO_FPRINTF("%s:ERROR unable to reset_all_fd_seek_pos on existing {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0121 %lu %lu\n", uinode->ino, uinode->dev_id);
                                KILLME();
                        }
                }
                goto update_uinode;
        }

populate_full_uinode:
        uinode->ino = ino;
        uinode->dev_id = dev_id;

        /*adding filename in uinode*/
        strncpy(uinode->filename, filename, PATH_MAX-1);
        uinode->filename[PATH_MAX-1] = '\0';

        /*if this is a new uinode, we need to allocate heap and bitmap*/
        if(allocated_new_uinode){
#ifdef ENABLE_PER_INODE_BITMAP
                alloc_bitmap(uinode);
#endif //ENABLE_PER_INODE_BITMAP

#if defined(ENABLE_EVICTION) && (defined(ENABLE_PVT_HEAP) || (defined(ENABLE_ONE_LRU) && defined(BELADY_PROOF)))
                init_pvt_heap(uinode);
#endif //ENABLE_EVICTION && ENABLE_PVT_HEAP or (ENABLE_ONE_LRU && BELADY_PROOF)
        }

update_uinode:
        /*Add this fd to this uinode's fdlist*/
        if(!add_fd_to_fdlist(uinode, fd, open_flags, seek_head)){
                SPEEDYIO_FPRINTF("%s:ERROR add_fd_to_fdlist fd:%d to {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0122 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                uinode = nullptr;
                KILLME();
                goto exit_add_fd_to_inode;
        }
        /*update the number of links for this uinode*/
        if(!update_nr_links(uinode, file_stat.st_nlink, false)){
                SPEEDYIO_FPRINTF("%s:ERROR unable to add nr_links to {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0123 %lu %lu\n", uinode->ino, uinode->dev_id);
                uinode = nullptr;
                KILLME();
                goto exit_add_fd_to_inode;
        }
#ifdef ENABLE_MINCORE_DEBUG
        update_mmap_fd(uinode);
#endif // ENABLE_MINCORE_DEBUG
        if(allocated_new_uinode){
                /**
                 * TODO: Should check if {ino, dev_id} is already in the hashtable
                 * to avoid duplicates.
                 */
                if(!insert_to_hashtable(ino, dev_id, (void*)uinode)){
                        SPEEDYIO_FPRINTF("%s:ERROR unable to insert {ino:%lu, dev:%lu} to hashtable\n", "SPEEDYIO_ERRCO_0124 %lu %lu\n", ino, dev_id);
                        KILLME();
                }
        }
        uinode->unlinked_lock.unlock();
        i_map_lock.unlock();

exit_add_fd_to_inode:
        ret = uinode;
        return ret;
}


/*pvt heap functions */

/**
 * This is mostly never called because for a read workload
 * the file is never deleted. so the inode is never destroyed.
 * For write workload; inodes will be deleted.
 * XXX:To be tested for that.
 *
 */
void _dest_pvt_heap(struct inode *uinode){
        if(unlikely(!uinode)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid uinode passed\n", "SPEEDYIO_ERRCO_0125\n");
                goto exit_dest_pvt_heap;
        }

        uinode->file_heap_lock.lock();
        if(likely(uinode->file_heap)){
                destroy_pvt_heap(uinode->file_heap);
                uinode->file_heap = nullptr;
        }
        if(likely(uinode->file_heap_node_ids)){
                uinode->file_heap_node_ids->clear();
                uinode->file_heap_node_ids->shrink_to_fit();
                delete uinode->file_heap_node_ids;
                uinode->file_heap_node_ids = nullptr;
        }
        uinode->file_heap_lock.unlock();
exit_dest_pvt_heap:
        return;
}


#ifdef ENABLE_MINCORE_DEBUG
void allocate_mmap(struct inode* uinode) {
        if (uinode == nullptr) {
                throw std::invalid_argument("Null inode pointer passed to allocate_mmap");
        }
        if (uinode->mmap_addr != nullptr) {
                throw std::runtime_error("mmap_addr is already valid");
        }
        if (uinode->fdlist_index < 0) {
                throw std::runtime_error("No valid fd available for mmap");
        }

        uinode->mmap_fd = uinode->fdlist[0].fd; // Use the first valid fd
        size_t mmap_length = NR_BITMAP_BITS * PAGESIZE;
        //printf("mmap_fd = %d, mmap_length = %zu\n", uinode->mmap_fd, mmap_length);
        int prot_flags = 0;
        if (uinode->fdlist[0].open_flags & O_RDONLY) {
                prot_flags |= PROT_READ;
        }
        if (uinode->fdlist[0].open_flags & O_WRONLY) {
                prot_flags |= PROT_WRITE;
        }
        if (uinode->fdlist[0].open_flags & O_RDWR) {
                prot_flags |= PROT_READ | PROT_WRITE;
        }

        uinode->mmap_addr = real_mmap(nullptr, mmap_length, prot_flags, MAP_SHARED, uinode->mmap_fd, 0);
        if (uinode->mmap_addr == MAP_FAILED) {
                uinode->mmap_addr = nullptr;
                std::string error_message = "mmap failed: ";
                error_message += strerror(errno);
                error_message += " | open_flags: ";
                if (uinode->fdlist[0].open_flags & O_RDONLY) error_message += "O_RDONLY ";
                if (uinode->fdlist[0].open_flags & O_WRONLY) error_message += "O_WRONLY ";
                if (uinode->fdlist[0].open_flags & O_RDWR) error_message += "O_RDWR ";
                if (uinode->fdlist[0].open_flags & O_CREAT) error_message += "O_CREAT ";
                if (uinode->fdlist[0].open_flags & O_EXCL) error_message += "O_EXCL ";
                if (uinode->fdlist[0].open_flags & O_NOCTTY) error_message += "O_NOCTTY ";
                if (uinode->fdlist[0].open_flags & O_TRUNC) error_message += "O_TRUNC ";
                if (uinode->fdlist[0].open_flags & O_APPEND) error_message += "O_APPEND ";
                if (uinode->fdlist[0].open_flags & O_NONBLOCK) error_message += "O_NONBLOCK ";
                if (uinode->fdlist[0].open_flags & O_SYNC) error_message += "O_SYNC ";
                if (uinode->fdlist[0].open_flags & O_ASYNC) error_message += "O_ASYNC ";
                if (uinode->fdlist[0].open_flags & O_DIRECT) error_message += "O_DIRECT ";
                if (uinode->fdlist[0].open_flags & O_LARGEFILE) error_message += "O_LARGEFILE ";
                if (uinode->fdlist[0].open_flags & O_DIRECTORY) error_message += "O_DIRECTORY ";
                if (uinode->fdlist[0].open_flags & O_CLOEXEC) error_message += "O_CLOEXEC ";
                error_message += "| prot_flags: ";
                if (prot_flags & PROT_READ) error_message += "PROT_READ ";
                if (prot_flags & PROT_WRITE) error_message += "PROT_WRITE ";
                throw std::runtime_error(error_message);
        }

}

void free_mmap(struct inode* uinode) {
    if (uinode == nullptr) {
        throw std::invalid_argument("Null inode pointer passed to free_mmap");
    }
    if (uinode->mmap_addr != nullptr) {
        size_t mmap_length = NR_BITMAP_BITS * PAGESIZE;
        munmap(uinode->mmap_addr, mmap_length);
        uinode->mmap_addr = nullptr;
        uinode->mmap_fd = -1;
    }
}

std::vector<bool> check_mincore(struct inode* uinode) {
    if (uinode == nullptr) {
        throw std::invalid_argument("Null inode pointer passed to check_mincore");
    }
    if (uinode->mmap_addr == nullptr) {
        throw std::runtime_error("check_mincore(): mmap not allocated");
    }
    size_t num_pages = NR_BITMAP_BITS;
    std::vector<bool> page_residency(num_pages, false);
    std::vector<unsigned char> mincore_array(num_pages);
    if (mincore(uinode->mmap_addr, num_pages * PAGESIZE, mincore_array.data()) != 0) {
        throw std::runtime_error("mincore failed");
    }
    for (size_t i = 0; i < num_pages; ++i) {
        page_residency[i] = mincore_array[i] & 1;
    }
    return page_residency;
}

void update_mmap_fd(struct inode* uinode) {

    if (uinode == nullptr) {
        throw std::invalid_argument("Null inode pointer passed to update_mmap_fd");
    }
    if(uinode->fdlist_index < 0) {
        free_mmap(uinode);
        return;
        //throw std::runtime_error("No valid fd available for mmap");
    }

    bool fd_still_valid = false;
    if (uinode->mmap_fd != -1) {
        for (int i = 0; i <= uinode->fdlist_index; ++i) {
            if (uinode->fdlist[i].fd == uinode->mmap_fd) {
                fd_still_valid = true;
                break;
            }
        }
    }

    if (!fd_still_valid) {
        free_mmap(uinode);
        if (uinode->fdlist_index >= 0) {
            allocate_mmap(uinode);
        }
    }
}

void print_mincore_array(const std::vector<bool>& page_residency_arr, off_t filesize) {
    const char* red = "\033[31m";
    const char* green = "\033[32m";
    const char* yellow = "\033[33m";
    const char* reset = "\033[0m";

    size_t num_pages = (filesize + PAGESIZE - 1) / PAGESIZE;

    // Print mincore status: 1 (green) if resident, 0 (red) if not
    /*
    for (size_t i = 0; i < num_pages; ++i) {
        bool in_memory = page_residency_arr[i];
        std::cout << (in_memory ? green : red) << (in_memory ? '1' : '0') << reset;
    }
    std::cout << std::endl;
    */

    // Print the number of entries in i_map
    size_t i_map_entries = hashtable_count(i_map);
    std::cout << yellow << "Number of entries in i_map: " << i_map_entries << reset << std::endl;


    size_t count = 1;
    bool current_value = page_residency_arr[0];
    for (size_t i = 1; i < num_pages; ++i) {
        if (page_residency_arr[i] == current_value) {
            ++count;
        } else {
            std::cout << (current_value ? green : red) << (current_value ? '1' : '0') << "(" << count << ")" << reset << " ";
            current_value = page_residency_arr[i];
            count = 1;
        }
    }
    // Print the last run
    std::cout << (current_value ? green : red) << (current_value ? '1' : '0') << "(" << count << ")" << reset << std::endl;


    size_t total_ones = 0, total_zeroes = 0;
    for (size_t i = 0; i < num_pages; ++i) {
        if (page_residency_arr[i]) {
            ++total_ones;
        } else {
            ++total_zeroes;
        }
    }
    std::cout << green << "Total ones: " << total_ones << reset << red << " Total zeroes: " << total_zeroes << reset << std::endl;

    // Check for any true values after num_pages (in case the filesize is wrong)
    for (size_t i = num_pages; i < page_residency_arr.size(); ++i) {
        if (page_residency_arr[i]) {
            throw std::runtime_error("Unexpected true value in page_residency_arr after num_pages");
        }
    }

    // Print underline: yellow ^ every 4 pages, otherwise -
    //     for (size_t i = 0; i < page_residency_arr.size(); i++) {
    //         if (i % 4 == 3) {
    //             std::cout << yellow << '^' << reset;
    //         } else {
    //             std::cout << '-';
    //         }
    //     }
    //     std::cout << std::endl << std::endl;
}

#endif // ENABLE_MINCORE_DEBUG


#ifdef BELADY_PROOF
/*Functions to support BELADY_PROOF*/

/**
 * This is a mock function to populate a given inode to all the
 * data structures. Will not add duplicates
 *
 * Assumptions:
 * 1. Should not see duplicate {ino, dev_id} pairs.
 */
int mock_populate_inode_ds(ino_t ino, dev_t dev_id){
        struct inode *uinode = nullptr;
        struct value *uinode_exists = nullptr;

        if(ino < 1 || dev_id < 0){
                SPEEDYIO_FPRINTF("%s:ERROR ino:%lu or dev_id:%lu is invalid\n", "SPEEDYIO_ERRCO_0126 %lu %lu\n", ino, dev_id);
                KILLME();
                goto exit_mock_populate_inode_ds;
        }

        // printf("%s: populating {ino:%lu, dev:%lu}\n", __func__, ino, dev_id);

        /**
         * DS to populate:
         * 1. i_map - <inode, dev_id> to struct inode mapping (will be used extensively)
         * 2. uinode - struct inode for typical inode operations
         * 2. fd_map not required. since we will get inode number for each operation
         */


        i_map_lock.lock();

        uinode_exists = get_from_hashtable(ino, dev_id);
        if(uinode_exists){
                SPEEDYIO_FPRINTF("%s:ERROR uinode already exists for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0127 %lu %lu\n", ino, dev_id);
                KILLME();
                i_map_lock.unlock();
                goto exit_mock_populate_inode_ds;
        }

        //No existing uinode found. Allocate and populate a new uinode

        try{
                uinode = new struct inode;
        }catch(std::bad_alloc& e){
                SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for inode: %s\n", "SPEEDYIO_ERRCO_0128 %s\n", e.what());
                KILLME();
                uinode = nullptr;
                i_map_lock.unlock();
                goto exit_mock_populate_inode_ds;
        }

        uinode->ino = ino;
        uinode->dev_id = dev_id;

#ifdef ENABLE_PER_INODE_BITMAP
        alloc_bitmap(uinode);
#endif //ENABLE_PER_INODE_BITMAP

#if defined(ENABLE_EVICTION) && (defined(ENABLE_PVT_HEAP) || (defined(ENABLE_ONE_LRU) && defined(BELADY_PROOF)))
        init_pvt_heap(uinode);
#endif //ENABLE_EVICTION && ENABLE_PVT_HEAP or (ENABLE_ONE_LRU && BELADY_PROOF)

        /**
         * Not populating the following here:
         * 1. add_fd_to_fdlist
         * 2. update_nr_links
         * 3. update_mmap_fd
         *
         * Do these if required
         */

        if(!insert_to_hashtable(ino, dev_id, (void*)uinode)){
                SPEEDYIO_FPRINTF("%s:ERROR unable to insert {ino:%lu, dev:%lu} to hashtable\n", "SPEEDYIO_ERRCO_0129 %lu %lu\n", ino, dev_id);
                KILLME();
        }

        // printf("%s: number of hashtable_entries:%zu\n", __func__, hashtable_count(i_map));

        i_map_lock.unlock();

exit_mock_populate_inode_ds:
        return -1;
}

#endif //BELADY_PROOF
