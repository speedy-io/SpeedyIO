#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
// #include <x86intrin.h>
#include <limits.h>

#include <sys/stat.h>
#include <sys/syscall.h>

#include <cstdlib>
#include <unordered_map>
#include <vector>

#include "prefetch_evict.hpp"
#include "per_thread_ds.hpp"
#include "utils/r_w_lock/readers_writers_lock.hpp"
#include "utils/heaps/binary_heap/heap.hpp"
#include "utils/system_info/system_info.hpp"
#include "utils/start_stop/start_stop_speedyio.hpp"

#include <iostream>
#include <set>
#include <algorithm>
#include <map>
#include <memory>

#ifdef ENABLE_MINCORE_DEBUG
long long int nr_pvt_heap_calls = 0;
#endif //ENABLE_MINCORE_DEBUG

/*
 * Implements and enables per thread constructor
 * and destructor
 */
thread_local per_thread_data per_th_d;

/**
 * When a program running with a dynamically linked library is forked
 * or calls exec family of commands, all the dlls are replicated in a
 * completely new address space for that new process. This means that
 * all the book keeping data structures we allocate and use here are
 * completely re-allocated. now since fds are copied to forked processes
 * this means that there could be a situation where a file was opened by
 * a process X ie. documented and saved in g_fd_map of X and then fork()
 * or exec was called, creating a process Y; ie. the same fds is now
 * valid in the Y but a new g_fd_map and other datastructures are allocated.
 * Now when Y reads this fd, handle_read will not be able to map it to its
 * uinode since there doesnt exit anything in this new address space.
 *
 * TODO: The correct solution to this problem is allocating a shared_memory
 * with its own custom allocator and then allocating all the book keeping
 * data structures in said shared memory. Now any fork or exec will point
 * to data from the shared memory removing any duplication etc.
 *
 * Fortunately, RocksDB and cassandra only calls exec on itself at the
 * very beginning of its start. So we can put this off for a bit.
 *
 */
std::unordered_map<int, std::shared_ptr<struct perfd_struct>> *g_fd_map = nullptr;
std::atomic_flag g_fd_map_init;
ReaderWriterLock g_fd_map_rwlock;


/*
 * first_rdtsc is used to subtract from __rdtsc() before saving it.
 * XXX: This is just a stop gap solution. Has to be refined later.
 * The correct solution is to use double or unsigned longs as keys in the heap
 */
unsigned long long int first_rdtsc = 0;


/*
 * This implements the heap of files that need to be evicted in that order
 */
struct Heap *g_file_heap = nullptr;
std::atomic_flag g_file_heap_init;
std::mutex g_heap_lock; //for updates to the global heap


struct lat_tracker pvt_heap_latency;
struct lat_tracker g_heap_latency;
struct lat_tracker ulong_heap_update;

/*
 * This function reserves MAX_IMAP_FILES*2 for g_fd_map.
 * It was implemented because with gcc 11 + centos 8;
 * g_fd_map.insert({XXX}) gave the following error.
 * "During startup program terminated with signal SIGFPE, Arithmetic exception"
 * Worked fine on gcc11 + centos 7 without reserve.
 * init_g_fd_map is called in per_thread_ds to make sure that the first thread
 * is able to reserve the unordered_map before any insertions happen.
 */
void init_g_fd_map(){
        if(!g_fd_map_init.test_and_set()){
                g_fd_map = new std::unordered_map<int, std::shared_ptr<struct perfd_struct>>();
                if(!g_fd_map){
                        SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for g_fd_map\n", "SPEEDYIO_ERRCO_0151\n");
                        KILLME();
                }
                g_fd_map->reserve(MAX_IMAP_FILES*2);
                SPEEDYIO_FPRINTF("%s: initialized g_fd_map\n", "SPEEDYIO_OTHERCO_0003\n");
        }
}


/**
 * adds any fd to g_fd_map. returns a valid pfd if successful, else nullptr
 * this can be called for both for blacklisted and whitelisted fd
 */
std::shared_ptr<struct perfd_struct> add_any_fd_to_perfd_struct(
                int fd, int open_flags, struct inode *uinode, bool file_is_whitelisted)
{

        std::shared_ptr<struct perfd_struct> pfd;

#ifdef DEBUG
        const char *filetype[] = {"blacklisted", "whitelisted"};
#endif
        bool existing_pfd = false;

        if(unlikely(fd < 3)){
                SPEEDYIO_FPRINTF("%s:ERROR fd:%d input is insane\n", "SPEEDYIO_ERRCO_0152 %d\n", fd);
                goto exit;
        }

        /*for blacklisted files, uinode will not be considered*/
        if(file_is_whitelisted && !uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is not valid for whitelisted file fd:%d\n", "SPEEDYIO_ERRCO_0153 %d\n", fd);
                goto exit;
        }

        pfd = get_perfd_data(fd);

        /**
         * NOTE: we never delete a pfd no matter the file is unlinked or closed
         * Just check sanity and update the pfd with the current data
         */
         if(pfd){
                debug_printf("%s: pfd found for %s file fd:%d\n", __func__, filetype[file_is_whitelisted], fd);
                existing_pfd = true;

                if(pfd->is_blacklisted() || pfd->is_closed()){
                        /**
                         * Scenarios:
                         * 0. this fd is being reopened for the same blacklisted file.
                         * 1. this fd was previously for a blacklisted file and now it has been closed
                         * 2. this fd was previously for a blacklisted file and now it has been implictly closed
                         * 3. this fd was previously for a whitelisted file but was closed earlier. Now this fd
                         * is being reused for some other whitelisted file.
                         *
                         * So we just update the pfd with this new data
                         */
                        debug_printf("%s: pfd is blacklisted or closed for %s file fd:%d going to update_pfd_data\n",
                                        __func__, filetype[file_is_whitelisted], fd);
                        goto update_pfd_data;
                }

                if(pfd->uinode){
                        debug_printf("%s: pfd has uinode for %s file fd:%d\n",
                                        __func__, filetype[file_is_whitelisted], fd);

                        if(pfd->uinode->is_deleted()){
                                /**
                                * file in this uinode has been unlinked
                                * just update pfd data
                                */
                               SPEEDYIO_FPRINTF("%s:UNUSUAL {ino:%lu, dev:%lu} is deleted but not marked closed in pfd fd:%d\n", "SPEEDYIO_UNUSCO_0001 %lu %lu %d\n", pfd->uinode->ino, pfd->uinode->dev_id, fd);
                                goto update_pfd_data;
                        }

                        /**
                         * at this point the pfd is supposed to be currently pointing to a
                         * whitelisted open file.
                         *
                         * this fd has been reused means the fd has been implicitly closed earlier
                        */
                        if(file_is_whitelisted && (uinode == pfd->uinode)){
                                /**
                                 * duplicate fd for the same whitelisted file.
                                 * Don't do anything
                                */
                               debug_printf("%s: pfd->uinode and passed uinode match for %s file fd:%d\n",
                                        __func__, filetype[file_is_whitelisted], fd);
                               goto exit;
                        }

                        /**
                         * At this point we can be sure of the following things:
                         * 1. pfd currently belongs to a whitelisted file
                         * 2. The closed flag on pfd is false, ie. it is not explicitly closed
                         * 3. The whitelisted uinode is not marked deleted
                         * 4. The passed uinode and the pfd->uinode don't match.
                         *
                         * This can only mean that the previous whitelisted fd was implicitly closed.
                         */
                        if(remove_fd_from_fdlist(pfd->uinode, fd) == -1){
                                SPEEDYIO_FPRINTF("%s:ERROR while removing fd:%d from {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0154 %d %lu %lu\n", fd, pfd->uinode->ino, pfd->uinode->dev_id);
                        }

                        /**
                        * If this fd was the last and the file was marked_unlinked
                        * then set unlinked on this uinode.
                        */
                        if(pfd->uinode->check_fdlist_and_unlink())
                        {
#ifdef ENABLE_EVICTION
                                remove_from_g_heap(pfd->uinode);
#endif
                        }
                        goto update_pfd_data;
                }

                SPEEDYIO_FPRINTF("%s:UNUSUAL fd:%d is not blacklisted or closed and doesnt have uinode\n", "SPEEDYIO_UNUSCO_0002 %d\n", fd);
                goto update_pfd_data;
        }
        else{
                existing_pfd = false;
                debug_printf("%s: No pfd found for %s file fd:%d. allocating a new one\n",
                                __func__, filetype[file_is_whitelisted], fd);

                /*no pfd. allocate a brand new one*/

                try{
                        pfd = std::make_shared<struct perfd_struct>();
                }catch (const std::bad_alloc& e){
                        SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for perfd_struct: %s\n", "SPEEDYIO_ERRCO_0155 %s\n", e.what());
                        pfd = nullptr;
                        goto exit;
                }catch (const std::exception& e){
                        SPEEDYIO_FPRINTF("%s:ERROR Unable to allocate memory for perfd_struct: %s\n", "SPEEDYIO_ERRCO_0156 %s\n", e.what());
                        pfd = nullptr;
                        goto exit;
                }
        }

update_pfd_data:
        pfd->fd = fd;
        pfd->open_flags = open_flags;
        pfd->fd_open = true;

        if(file_is_whitelisted){
                pfd->ino = uinode->ino;
                pfd->dev_id = uinode->dev_id;
                pfd->uinode = uinode;
                pfd->blacklisted = false;
        }else{
                //update blacklisted file in pfd
                pfd->uinode = nullptr;
                pfd->blacklisted = true;
        }

add_to_fdmap:
        if(!existing_pfd){
                /**
                 * Since for a given fd, we only allocate one pfd for the lifetime of the
                 * application, there shouldnt be any updates for already inserted fds.
                 * pfds are not freed for the lifetime of the application even if files are
                 * closed or unlinked. This is done to make sure that the weak_ptr link between
                 * per_thread_ds.fd_map to this fd's pfd shared_ptr is not broken.
                 */
                g_fd_map_rwlock.lock_write();
                auto result = g_fd_map->insert({fd, pfd});
                g_fd_map_rwlock.unlock_write();
                if(!result.second){
                        SPEEDYIO_FPRINTF("%s:ERROR fd:%d already exists in g_fd_map. Unable to insert {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0157 %d %lu %lu\n", fd, uinode->ino, uinode->dev_id);
                        KILLME();
                }else{
                        if(file_is_whitelisted){
                                debug_printf("%s: successfully added whitelisted fd:%d {ino:%lu, dev:%lu} to g_fd_map:%p\n",
                                        __func__, fd, uinode->ino, uinode->dev_id, (void*)g_fd_map);
                        }else{
                                debug_printf("%s: successfully added blacklisted fd:%d to g_fd_map:%p\n",
                                        __func__, fd, (void*)g_fd_map);
                        }
                }
        }

exit:
        return pfd;
}


std::shared_ptr<struct perfd_struct> get_perfd_data(int fd)
{
        std::shared_ptr<struct perfd_struct> a;

        g_fd_map_rwlock.lock_read();

        auto it = g_fd_map->find(fd);
        if(it != g_fd_map->end()){
                a = it->second;
        }

get_perfd_data_exit:
        g_fd_map_rwlock.unlock_read();
        return a;
}


/*
 * Returns perfd_struct from per_thread_ds or from perfd_ds
 * XXX: This function will slow for non-whitelisted fds
 * incurs overhead to check to per_thread fd_map and then global fd_map
 * But that is okay for now; We will improve this later
 */
std::shared_ptr<struct perfd_struct> get_perfd_struct_fast(int fd)
{
        std::shared_ptr<struct perfd_struct> ret;
        std::unordered_map<int, std::weak_ptr<struct perfd_struct>>::iterator it;

        if(fd < 3){
                ret = nullptr;
                goto exit_get_perfd_struct_fast;
        }

#ifdef PER_THREAD_DS
        /*
         * we had used (*per_th_d.fd_map)[fd] here
         * earlier but that hurt performance. hence
         * using find.
         */
        it = per_th_d.fd_map->find(fd);
        if(it != per_th_d.fd_map->end()){
                ret = it->second.lock(); //weak_ptr -> shared_ptr
        }

        /**
         * Maybe the per_th_d doesnt have this fd's pfd updated.
         * Get it from g_fd_map
         */
        // if(!ret || ret->fd != fd){
        if(!ret || ret->fd != fd
                || (!ret->is_blacklisted() && !ret->uinode)
                || (!ret->is_blacklisted() && ret->uinode && (ret->uinode->ino != ret->ino || ret->uinode->dev_id != ret->dev_id)))
        {
                ret = get_perfd_data(fd);
                (*per_th_d.fd_map)[fd] = ret;
        }

#else // no PER_THREAD_DS
        ret = get_perfd_data(fd);
#endif //PER_THREAD_DS

        /*Check if this can be done*/
// #ifdef ENABLE_SMART_PTR
//         goto exit_get_perfd_struct_fast;
// #endif //ENABLE_SMART_PTR

        /*Additional checks to ensure sanity*/

        if(!ret){
                ret = nullptr;
                goto exit_get_perfd_struct_fast;
        }

        if(unlikely(ret->fd != fd)){
                SPEEDYIO_FPRINTF("%s:ERROR asked fd:%d, got PFD with fd:%d. NULLifying output\n", "SPEEDYIO_ERRCO_0158 %d %d\n", fd, ret->fd);
                ret = nullptr;
                KILLME();
                goto exit_get_perfd_struct_fast;
        }

        /*whitelisted file should always have a valid uinode*/
        if(!ret->is_blacklisted() && !ret->uinode){
                SPEEDYIO_FPRINTF("%s:ERROR whitelisted fd:%d doesn't have a uinode. Nullifying output\n", "SPEEDYIO_ERRCO_0159 %d\n", fd);
                ret = nullptr;
                KILLME();
                goto exit_get_perfd_struct_fast;
        }

        if(!ret->is_blacklisted() && (ret->uinode->ino != ret->ino || ret->uinode->dev_id != ret->dev_id))
        {
                SPEEDYIO_FPRINTF("%s:ERROR whitelisted fd:%d, pfd{ino:%lu, dev:%lu} & uinode{ino:%lu, dev:%lu} dont match\n", "SPEEDYIO_ERRCO_0160 %d %lu %lu %lu %lu\n", fd, ret->ino, ret->dev_id, ret->uinode->ino, ret->uinode->dev_id);
                ret = nullptr;
                KILLME();
                goto exit_get_perfd_struct_fast;
        }

exit_get_perfd_struct_fast:
        return ret;
}

/*Global Heap Functions*/

/*
 * This allocates heap with nr_elements and correct comparator
 */
struct Heap* __heap_init(size_t nr_elements, std::string name){
        struct Heap *heap = nullptr;

        heap = heap_init(nr_elements, name.c_str());
        if(unlikely(!heap)){
                SPEEDYIO_FPRINTF("%s:ERROR heap_init failed\n", "SPEEDYIO_ERRCO_0161\n");
                goto exit_heap_init;
        }
exit_heap_init:
        return heap;
}

/*
 * This initializes the global file heap.
 * Each element in it is a file.
 */
void init_g_heap(){
        if(!g_file_heap_init.test_and_set()){
                debug_printf("%s: done\n", __func__);

                std::string heap_name = std::string("gh");
                g_file_heap = __heap_init(MAX_IMAP_FILES, heap_name);

#ifndef DISABLE_FIRST_RDTSC
                first_rdtsc = ticks_now();
#endif //DISABLE_FIRST_RDTSC

        }
        else {
                debug_printf("%s: g_file_heap already initialized\n", __func__);
        }
}

void remove_from_g_heap(struct inode* uinode){

        if(unlikely(!uinode)){
                goto exit_remove_from_g_heap;
        }

        // SPEEDYIO_PRINTF("%s:INFO inode:%lu heap_id:%d\n", "SPEEDYIO_INFOCO_0016 %lu %d\n", uinode->ino, uinode->heap_id);

        /*We ideally want only deleted uinodes to be removed from gheap*/
        if(!uinode->is_deleted()){
                debug_fprintf(stderr, "%s:WARNING removing an undeleted {ino:%lu, dev:%lu} from g_heap\n",
                                __func__, uinode->ino, uinode->dev_id);
        }

        if(uinode->heap_id < 0){
                if(unlikely(uinode->one_operation_done)){
                        /**
                         * print an error only if some read/write operations have been done on
                         * this uinode but still the heap_id < 0
                         */
                        SPEEDYIO_FPRINTF("%s:ERROR {ino:%lu, dev:%lu} inode's heap_id:%d is not valid\n", "SPEEDYIO_ERRCO_0162 %lu %lu %d\n", uinode->ino, uinode->dev_id, uinode->heap_id);
                }
                goto exit_remove_from_g_heap;
        }

        g_heap_lock.lock();
        heap_delete_key_by_id(g_file_heap, uinode->heap_id);
        uinode->heap_id = -1;
        g_heap_lock.unlock();

exit_remove_from_g_heap:
        return;
}

/*
 * There are currently three eviction policies in this code:
 * 1. EVICTION_LRU
 *  It uses a min heap to determine the file to be evicted in g_heap
 *  using time of last access as key. It uses the same as key for each
 *  portion in the file to determine eviction order from pvt_heap
 * 
 * 2. EVICTION_FREQ
 *  It employs a min heap to determine the files and its portions to be
 *  evicted using g_heap and pvt heap with freq of access as the key.
 *  When a file/portion is evicted, ADD_TO_KEY_REDUCE_PRIORITY is added
 *  to the key to keep the last freq of access.
 * 
 * 3. EVICTION_COMPLEX
 *  It employs the following formula as key in min heap
 * 
 *  key =  nr_accesses * MAX(1, (EVICTION_GAMMA * nr_evictions))
 *         -----------------------------------------------------
 *                     (TIME_DECAY * access_time_diff)
 * 
 *  This is only implemented for global heap. will give error when pvt heap is
 *  enabled.
 */

/*
 * Calculates the new priority value for the given uinode
 * if new_node is true, initial priority values are returned.
 * priority values are calculated based on the chosen EVICTION policy
 */
unsigned long long int get_g_priority_val(struct inode *uinode, bool new_node){
        unsigned long long int priority_val;
        unsigned long long int old_priority_val;
        unsigned long long int curr_access_time;
        unsigned long long int time_diff;

        if(!uinode){
                priority_val = 0;
                goto exit_get_priority_val;
        }

        curr_access_time = ticks_now();

#ifdef EVICTION_LRU
        //Requires Min Heap
        priority_val =  curr_access_time - first_rdtsc;
#elif EVICTION_COMPLEX
        /*EVICTION_COMPLEX is not completely implemented. rudementary for global heaps only
         * DO NOT USE without checking all the code.
         */
        //Requires Min Heap
        //The required uinode instrumentation happens even without ENVICTION_COMPLEX enabled
        time_diff = curr_access_time - uinode->last_access_tstamp;

        priority_val = uinode->nr_accesses * MAX(1, (EVICTION_GAMMA * (uinode->nr_evictions)));
        priority_val /= (TIME_DECAY*time_diff);
#elif EVICTION_FREQ
        //Requires Min Heap
        if(unlikely(new_node)){
                priority_val = 1.0;
        }else{
                old_priority_val = heap_get_key_by_id(g_file_heap, uinode->heap_id);

                if(old_priority_val > ADD_TO_KEY_REDUCE_PRIORITY){
                        priority_val = (old_priority_val - ADD_TO_KEY_REDUCE_PRIORITY) + 1;
                }else if(unlikely(old_priority_val == ADD_TO_KEY_REDUCE_PRIORITY)){
                        /*
                         * This means that this heap added frequencies to ADD_TO_KEY_REDUCE_PRIORITY
                         * Increase the ADD_TO_KEY_REDUCE_PRIORITY in this case else frequencies
                         * will be lost at eviction/re-access
                         */
                        SPEEDYIO_FPRINTF("%s:ERROR key {ino:%lu, dev:%lu}, increase ADD_TO_KEY_REDUCE_PRIORITY\n", "SPEEDYIO_ERRCO_0163 %lu %lu\n", uinode->ino, uinode->dev_id);
                        priority_val = old_priority_val + 1;
                }else{
                        priority_val = old_priority_val + 1;
                }
        }

#elif defined(ENABLE_EVICTION) && !defined(EVICTION_FREQ) && !defined(EVICTION_LRU) && !defined(EVICTION_COMPLEX)//EVICTION_LRU
        #error "No EVICTION priority chosen"
#endif //EVICTION_LRU

        uinode->last_access_tstamp = curr_access_time;

        /*
        printf("%s: {ino:%lu, dev:%lu}, nr_accesses:%ld, time_diff:%llu, nr_evictions:%d, p_val:%f\n",
                __func__, uinode->ino, uinode->dev_id, uinode->nr_accesses, time_diff, uinode->nr_evictions,
                priority_val);
        */

exit_get_priority_val:
        return priority_val;
}

/*only called when ENABLE_PVT_HEAP disabled*/
#ifdef BELADY_PROOF
void update_g_heap(struct inode* uinode, uint64_t timestamp)
#else
void update_g_heap(struct inode* uinode)
#endif
{
        // This is called when only gheap is enabled
        unsigned long long time;
        int heap_id;
        unsigned long long int new_priority = 0;

        if(unlikely(!uinode)){
                goto update_g_heap_exit;
        }

        g_heap_lock.lock();

        uinode->nr_accesses += 1;

        //No heap node for this uinode yet.
        if(uinode->heap_id < 0){
                //New uinode
#if defined(EVICTION_LRU) && defined(BELADY_PROOF)
                new_priority = timestamp;
#elif !defined(BELADY_PROOF)
                new_priority = get_g_priority_val(uinode, true);
#else
#error "BELADY_PROOF is only available with EVICTION_LRU."
#endif //EVICTION_LRU and BELADY_PROOF

                if(unlikely(new_priority) == 0){
                        debug_fprintf(stderr, "%s:UNUSUAL new priority is 0. Should not happen\n", __func__);
                        goto unlock_and_exit;
                }
                uinode->heap_id = heap_insert(g_file_heap, new_priority, (void*)uinode);
        }
        /*
         * This if condition will be true if the number of accesses to the file is a multiple of G_HEAP_FREQ
         * This is done purely for book keeping performance
         * For G_HEAP_FREQ values other than 1, modulo operation is done
         * For G_HEAP_FREQ = 1, the case is handled specially as anything modulo 1 is 0
         * It checks whether uinode->nr_accesses is divisible by G_HEAP_FREQ -
         * if G_HEAP_FREQ is 0 or negative, it uses 1 instead to avoid a
         * division/modulo by zero (which would crash the program).
        */
        else if (uinode->nr_accesses % ((G_HEAP_FREQ > 0) ? G_HEAP_FREQ : 1) == 0) {
                /*
                 * Periodic updates of the global heap are done purely
                 * for book keeping performance.
                 * Without periodic updates, we have observed large
                 * performance drops just for updating these
                 */

#if defined(EVICTION_LRU) && defined(BELADY_PROOF)
                new_priority = timestamp;
#elif !defined(BELADY_PROOF)
                new_priority = get_g_priority_val(uinode, false);
#else
#error "BELADY_PROOF is only available with EVICTION_LRU."
#endif //EVICTION_LRU and BELADY_PROOF

                if(unlikely(new_priority) == 0){
                        debug_fprintf(stderr, "%s:UNUSUAL new priority is 0. Should not happen\n", __func__);
                        goto unlock_and_exit;
                }
                heap_update_key(g_file_heap, uinode->heap_id, new_priority);
        }

unlock_and_exit:
        g_heap_lock.unlock();

update_g_heap_exit:
        return;
}


#ifdef BELADY_PROOF
/**
 * This is the implementation of a flat LRU where each element is a portion of a uinode.
 * The size of portion is defined by PVT_HEAP_PG_ORDER. Only implemented for BELADY_PROOF for now.
 */
void update_one_heap(struct inode* uinode, off_t offset, size_t size, uint64_t timestamp)
{
        off_t portion_nr;
        size_t portion_order = PAGE_SHIFT + PVT_HEAP_PG_ORDER;
        size_t portion_sz = 1UL << portion_order;
        unsigned long long int portion_key;

        /*DEBUG print flag*/
        bool print = false;

        off_t first_portion_nr = PORTION_NR_FROM_OFFSET(offset, portion_order);
        off_t last_portion_nr  = PORTION_NR_FROM_OFFSET(offset + size - 1, portion_order);
        struct lru_entry *entry = nullptr;

        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr\n", "SPEEDYIO_ERRCO_0164\n");
                goto update_one_heap_exit;
        }

        g_heap_lock.lock();

        uinode->nr_accesses += 1;
        for (portion_nr = first_portion_nr; portion_nr <= last_portion_nr; portion_nr++) {
                entry = nullptr;

                if((*uinode->file_heap_node_ids)[portion_nr] == -1){
                        struct lru_entry *entry = (struct lru_entry*)malloc(sizeof(struct lru_entry));
                        if(!entry){
                                SPEEDYIO_FPRINTF("%s:ERROR malloc failed for lru_entry\n", "SPEEDYIO_ERRCO_0165\n");
                                g_heap_lock.unlock();
                                KILLME();
                                goto update_one_heap_exit;
                        }

                        entry->uinode = uinode;
                        entry->portion_nr = portion_nr;

#if defined(EVICTION_LRU)
                        portion_key = timestamp;
#else
#error "Only EVICTION_LRU implemented with ENABLE_ONE_LRU"
#endif //EVICTION_LRU

                        (*uinode->file_heap_node_ids)[portion_nr] = heap_insert(g_file_heap, portion_key, (void*)entry);
                        if((*uinode->file_heap_node_ids)[portion_nr] == -1){
                                SPEEDYIO_FPRINTF("%s:ERROR heap_insert failed {ino:%lu, dev:%lu}, portion_nr:%ld\n", "SPEEDYIO_ERRCO_0166 %lu %lu %ld\n", uinode->ino, uinode->dev_id, portion_nr);
                                g_heap_lock.unlock();
                                KILLME();
                                goto update_one_heap_exit;
                        }
                        // if(print)
                        // {
                        //         printf("%s: insert {ino:%lu, dev:%lu}, portion_nr:%ld, portion_key:%llu\n", __func__, uinode->ino, uinode->dev_id, portion_nr, portion_key);
                        // }
                        //printf("%s:ADDED {ino:%lu, dev:%lu}, portion_nr:%ld, id:%d\n", __func__, uinode->ino, uinode->dev_id, portion_nr, (*uinode->file_heap_node_ids)[portion_nr]);
                }
                else{

#if defined(EVICTION_LRU)
                        portion_key = timestamp;
#else
#error "Only EVICTION_LRU implemented with ENABLE_ONE_LRU"
#endif //EVICTION_LRU

                        heap_update_key(g_file_heap, (*uinode->file_heap_node_ids)[portion_nr], portion_key);

                        // if(print){
                        //         printf("%s: update {ino:%lu, dev:%lu}, portion_nr:%ld, portion_key:%llu\n", __func__, uinode->ino, uinode->dev_id, portion_nr, portion_key);
                        // }
                        //printf("%s:UPDATED {ino:%lu, dev:%lu}, portion_nr:%ld, id:%d\n", __func__, uinode->ino, uinode->dev_id, portion_nr, (*uinode->file_heap_node_ids)[portion_nr]);
                }
        }

        g_heap_lock.unlock();

update_one_heap_exit:
        return;
}
#endif //BELADY_PROOF


#if 0
/**
 * If the user application is allowed to evicts pages, use this function
 * to update that in the uinode->file_heap and g_file_heap.
 */
void heap_dont_need_update(struct inode* uinode, int fd, off_t offset, size_t size){
        size_t portion_order = PAGE_SHIFT + PVT_HEAP_PG_ORDER;
        off_t portion_nr;
        size_t portion_sz = 1UL << portion_order;
        off_t first_portion_nr;
        off_t last_portion_nr;
        unsigned long long int new_pvt_heap_min;
        struct stat file_stat;

        if(offset < 0){
                cfprintf(stderr, "%s:ERROR offset is negative\n", __func__);
                goto exit_heap_dont_need_update;
        }
        if(!uinode || uinode->is_deleted()){
                cfprintf(stderr, "%s:ERROR uinode is nullptr\n", __func__);
                goto exit_heap_dont_need_update;
        }
        if(uinode->is_deleted()){
                cfprintf(stderr, "%s:ERROR uinode {ino:%lu, dev_id:%lu} is_deleted()\n", __func__, uinode->ino, uinode->dev_id);
                goto exit_heap_dont_need_update;
        }

        if(!uinode->one_operation_done){
                /**
                 * no read/write operation has been on this uinode yet.
                 * Dont need to do any of the below book keeping work.
                 */
                goto exit_heap_dont_need_update;
        }

        if(size == 0){
                /**
                 * Get the size of this file if supplied size is 0
                 */
                if(fstat(fd, &file_stat) == -1){
                        cfprintf(stderr, "%s:ERROR unable to fstat\n", __func__);
                        goto exit_heap_dont_need_update;
                }
                size = file_stat.st_size;
        }

        first_portion_nr = PORTION_NR_FROM_OFFSET(offset, portion_order);
        last_portion_nr  = PORTION_NR_FROM_OFFSET(offset + size - 1, portion_order);

#ifdef ENABLE_PVT_HEAP
        for(portion_nr = first_portion_nr; portion_nr <= last_portion_nr; portion_nr++){
                uinode->file_heap_lock.lock();

                /*Update pvt heap element if and only if there exists a portion id*/
                if((*uinode->file_heap_node_ids)[portion_nr] >= 0){
#ifdef EVICTION_LRU
                        heap_update_key(uinode->file_heap, (*uinode->file_heap_node_ids)[portion_nr], ULONG_MAX);
#elif defined(ENABLE_EVICTION)
#error "ONLY EVICTION_LRU supported for pvt heap"
#endif
                }
                uinode->file_heap_lock.unlock();
        }

        /**
         * Updating the new pvt heap's min to g_file_heap
         */
        uinode->file_heap_lock.lock();
        new_pvt_heap_min = heap_read_min(uinode->file_heap)->key;
        uinode->file_heap_lock.unlock();

        g_heap_lock.lock();
        heap_update_key(g_file_heap, uinode->heap_id, new_pvt_heap_min);
        g_heap_lock.unlock();

#elif (defined(ENABLE_EVICTION))
        /**
         * This means only gheap enabled. This is not supported.
         */
#error "heap_dont_need_update only supported with ENABLE_PVT_HEAP"
#endif //ENABLE_PVT_HEAP

exit_heap_dont_need_update:
        return;
}
#endif //TESTING ONLY

/*updates both pvt and global heaps as required*/
#ifdef BELADY_PROOF
void heap_update(struct inode* uinode, off_t offset, size_t size, bool from_read, uint64_t timestamp)
#else
void heap_update(struct inode* uinode, off_t offset, size_t size, bool from_read)
#endif //BELADY_PROOF
{
        unsigned long long key = 0;
        long long int heap_key;
        unsigned long long int new_pvt_heap_min;

        struct timespec start, end;

#ifdef ENABLE_PVT_HEAP
        /*
         * we haven't observed a large performance drop when updating pvt heaps
         * for each file read. Hence not reducing its frequency.
         */

#ifdef BELADY_PROOF
        new_pvt_heap_min = update_pvt_heap(uinode, offset, size, from_read, timestamp);
#else //Not BELADY_PROOF, general usage

        clock_gettime(CLOCK_MONOTONIC, &start);

        new_pvt_heap_min = update_pvt_heap(uinode, offset, size, from_read);

        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &pvt_heap_latency);

#ifdef DBG_ONLY_UPDATE_PVT_HEAP
        goto skip_everything;
#endif //DBG_ONLY_UPDATE_PVT_HEAP

#endif //BELADY_PROOF

        /*updating this uinode's position in the global heap*/

        clock_gettime(CLOCK_MONOTONIC, &start);

        // g_heap_lock.lock();
        if(!g_heap_lock.try_lock()){
                goto skip_gheap_update;
        }

        // uinode->nr_accesses += 1;

        /*This sets the key for uinode in gheap = min key in its pvt heap*/
        if(uinode->heap_id < 0){

#ifdef EVICTION_FREQ
                key = get_min_key(uinode);
                if(key < 1){
                        SPEEDYIO_FPRINTF("%s:UNUSUAL min_key is less than 1. This should not happen\n", "SPEEDYIO_UNUSCO_0003\n");
                }
#elif EVICTION_LRU

#ifdef SET_PVT_MIN_IN_GHEAP
    key = new_pvt_heap_min;
#elif defined(BELADY_PROOF)
    key = timestamp;
#else //Neither BELADY_PROOF nor SET_PVT_MIN_IN_GHEAP
    key = ticks_now() - first_rdtsc;
#endif //SET_PVT_MIN_IN_GHEAP

#endif //EVICTION_FREQ && EVICTION_LRU

                /*New uinode. insert for the first time*/
                uinode->one_operation_done = true;
                uinode->heap_id = heap_insert(g_file_heap, key, (void*)uinode);
                // SPEEDYIO_PRINTF("%s: heap_insert for {ino:%lu, dev:%lu}, heap_id:%d\n", "SPEEDYIO_OTHERCO_0004 %lu %lu %d\n", uinode->ino, uinode->dev_id, uinode->heap_id);
        }
#ifdef EVICTION_FREQ
        else if(heap_get_key_by_id(g_file_heap, uinode->heap_id) > ADD_TO_KEY_REDUCE_PRIORITY){
                key = get_min_key(uinode);
                if(key < 1){
                        SPEEDYIO_FPRINTF("%s:UNUSUAL key is less than 1. This should not happen\n", "SPEEDYIO_UNUSCO_0004\n");
                }
                heap_update_key(g_file_heap, uinode->heap_id, key);
        }
#endif //EVICTION_FREQ

#ifdef GHEAP_TRIGGER
        else if( (heap_get_key_by_id(g_file_heap, uinode->heap_id) == ULONG_MAX)  || trigger_check(uinode->gheap_trigger) || !from_read)
#else
        else if( (heap_get_key_by_id(g_file_heap, uinode->heap_id) == ULONG_MAX)  || ((uinode->nr_accesses % G_HEAP_FREQ) == 0))
#endif //GHEAP_TRIGGER
        {

#ifdef EVICTION_FREQ
                key = get_min_key(uinode);
                if(key < 1){
                        SPEEDYIO_FPRINTF("%s:UNUSUAL key is less than 1. This should not happen\n", "SPEEDYIO_UNUSCO_0005\n");
                }
#elif EVICTION_LRU

#if defined(BELADY_PROOF)
        key = timestamp;
#else
        key = new_pvt_heap_min;

        /**
         * Cassandra, during compactions, usually opens a file in append only mode,
         * writes to it; then opens it in read only and reads it.
         * We have observed during heavy read-write load on cassandra nodes, that sometimes
         * a just written file is chosen for eviction before it has even been read. Now this
         * is incorrect because the new file written is probably going to be read and older
         * corresponding files will be deleted by cassandra. So to make sure this doesnt happen
         * if a file is being written, it will be placed very low in the global heap.
         * The first time it is read, that is when it will be updated to its current time stamp
         * and evicted if need be.
         */
        if(!from_read){
                key = ULONG_MAX - 1;
        }
#endif //BELADY_PROOF

#endif //EVICTION_FREQ && EVICTION_LRU
                // SPEEDYIO_PRINTF("%s: heap_update_key for {ino:%lu, dev:%lu}, heap_id:%d, key:%lu\n", "SPEEDYIO_OTHERCO_0005 %lu %lu %d\n",
                //                 uinode->ino, uinode->dev_id, uinode->heap_id, key);
                heap_update_key(g_file_heap, uinode->heap_id, key);
        }

        g_heap_lock.unlock();

skip_gheap_update:

        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &g_heap_latency);

skip_everything:

#elif defined(ENABLE_ONE_LRU) && defined(BELADY_PROOF)
        // printf("%s update_one_heap being called\n", __func__);
        update_one_heap(uinode, offset, size, timestamp);

#else //ENABLE_PVT_HEAP only global heap enabled

#ifdef BELADY_PROOF
        update_g_heap(uinode, timestamp);
#else
        //only global heap enabled
        update_g_heap(uinode);
#endif //BELADY_PROOF

#endif //ENABLE_PVT_HEAP
        return;
}


/*
 * Private Heap implementation
 */
void init_pvt_heap(struct inode* uinode){
        unsigned long nr_page_range;
        if(!uinode){
                SPEEDYIO_FPRINTF("%s:ERROR invalid uinode found\n", "SPEEDYIO_ERRCO_0167\n");
                goto exit_init_pvt_heap;
        }

        if(uinode->file_heap != nullptr || uinode->file_heap_node_ids != nullptr){
                SPEEDYIO_FPRINTF("%s:UNUSUAL fileheap for {ino:%lu, dev:%lu} already allocated. Dual init attempted\n", "SPEEDYIO_UNUSCO_0006 %lu %lu\n", uinode->ino, uinode->dev_id);
                goto exit_init_pvt_heap;
        }else{
                debug_printf("%s: fileheap for {ino:%lu, dev:%lu} being allocated\n", __func__, uinode->ino, uinode->dev_id);
                uinode->file_heap_lock.lock();
#ifndef ENABLE_ONE_LRU
                std::string heap_name = std::string("ph_") + std::to_string(uinode->ino);
                uinode->file_heap = __heap_init(NR_PVT_HEAP_ELEMENTS, heap_name);
                if(unlikely(!uinode->file_heap)){
                        SPEEDYIO_FPRINTF("%s:ERROR heap_init failed\n", "SPEEDYIO_ERRCO_0168\n");
                        uinode->file_heap_lock.unlock();
                        goto exit_init_pvt_heap;
                }
#endif //ENABLE_ONE_LRU

                /*
                 * file_heap_node_ids keeps for each portion_nr in the file
                 * the corresponding heap id in the pvt heap.
                 * It has now been allocated using an auto expanding vector with nice properties:
                 *
                 * 1. Can be dereferenced etc almost like an array.
                 * Little to no perf penalty on access/updates
                 * 2. Auto resizes based on the index that is being used right now.
                 * (Doubles the size so resizes dont happen often)
                 * 3. Maintains a default value passed at declaration time.
                 * We need this because the code requires unused file_heap_node_ids indices to have -1.
                 * 4. Is implemented as a template, so any variable type can use it. Here we are using it for int.
                 */

                /*all untouched file heap node ids should be -1 since heap node ids start from 0*/
                uinode->file_heap_node_ids = new AutoExpandVector<int>(MIN_NR_FILE_HEAP_NODES, -1);
                uinode->file_heap_lock.unlock();
        }
exit_init_pvt_heap:
        return;
}

/**
 * clears the pvt heap of a given uinode
 * returns true if successful, else false
 * The capacity of the heap remains same as before
 */
bool clear_pvt_heap(struct inode* uinode){
        bool ret = true;

        if(!uinode){
                ret = false;
                SPEEDYIO_FPRINTF("%s:ERROR no uinode\n", "SPEEDYIO_ERRCO_0169\n");
                goto exit_clear_pvt_heap;
        }

        if(!uinode->file_heap){
                ret = false;
                SPEEDYIO_FPRINTF("%s:ERROR no file_heap for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0170 %lu %lu\n", uinode->ino, uinode->dev_id);
                goto exit_clear_pvt_heap;
        }

        if(!uinode->file_heap_node_ids){
                ret = false;
                SPEEDYIO_FPRINTF("%s:ERROR no file_heap_node_ids for {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0171 %lu %lu\n", uinode->ino, uinode->dev_id);
                goto exit_clear_pvt_heap;
        }

        uinode->file_heap_lock.lock();
        heap_clear(uinode->file_heap);

        uinode->file_heap_node_ids->clear();
        uinode->file_heap_node_ids->shrink_to_fit();

        uinode->file_heap_lock.unlock();

exit_clear_pvt_heap:
        return ret;
}

/*returns the current min key from this uinode's heap*/
#ifdef BELADY_PROOF
unsigned long long int update_pvt_heap(struct inode* uinode, off_t offset, size_t size, bool from_read, uint64_t timestamp)
#else
unsigned long long int update_pvt_heap(struct inode* uinode, off_t offset, size_t size, bool from_read)
#endif
{
        int pid = getpid(), tid = gettid();

        off_t portion_nr;
        unsigned long long int portion_key;
        unsigned long long int current_min;
        size_t portion_order = PAGE_SHIFT + PVT_HEAP_PG_ORDER;
        size_t portion_sz = 1UL << portion_order;
        off_t first_portion_nr = 0;
        off_t last_portion_nr = 0;

        // SPEEDYIO_PRINTF("%s:INFO {ino:%lu, dev:%lu}, offset:%ld, size:%ld portion_sz:%ld first_portion_nr:%ld last_portion_nr:%ld from_read:%d\n", "SPEEDYIO_INFOCO_0017 %lu %lu %ld %ld %ld %ld %ld %d\n", uinode->ino, uinode->dev_id, offset, size, portion_sz, first_portion_nr, last_portion_nr, from_read);

        if (unlikely(portion_nr < 0)) {
                SPEEDYIO_PRINTF("%s:NOTE negative first_portion_nr detected for {ino:%lu, dev:%lu}, first_portion_nr:%ld, portion_sz:%ld, portion_order:%ld\n", "SPEEDYIO_NOTECO_0008 %lu %lu %ld %ld %ld\n", uinode->ino, uinode->dev_id, first_portion_nr, portion_sz, portion_order);
        }

// #ifdef ENABLE_MINCORE_DEBUG
//         nr_pvt_heap_calls += 1;
//         struct stat file_stat;
//         long long int mincore_in_mem_count = 0 , heap_in_mem_count = 0, heap_out_of_mem_count = 0;
//         std::vector<bool> mincore_arr;
//         // std::vector<bool> heap_arr(NR_PVT_HEAP_ELEMENTS, false);
//         std::vector<unsigned long long int> all_hkeys;
//         uinode->file_heap_lock.lock();  // CAREFUL: REVERT TO DOING THE UNLOCK INSIDE THE LOOP IN NORMAL FLOW. LOCK NEEDS TO BE OUTSIDE WHILE DOING THIS DEBUGGING
// #endif // ENABLE_MINCORE_DEBUG

        if(unlikely(!uinode || !uinode->file_heap)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid uinode or fileheap\n", "SPEEDYIO_ERRCO_0172\n");
                goto exit_update_pvt_heap;
        }

        /**
         * TODO: FADV_SEQUENTIAL or FADV_NORMAL has been called on this uinode:
         * This means that the OS will perform prefetching operations with reads.
         *
         * To keep the book keeping logic correct, it is imperative that any pages resident
         * in memory be accounted for in the pvt heap. Needless to say, any pages that are
         * not accounted for in the pvt heap may never get evicted since the OS's evictor is
         * never let to run by the shared lib's evictor. One thing to note is that the
         * probability of a page not being accounted for is small since PVT_HEAP_PG_ORDER is
         * typically large but it is not entirely impossible.
         * Therefore, the idea here is that it is okay to overestimate the pages in memory
         * rather than leaving some out from the pvt heap.
         * So, for file that have FADV_SEQ/FADV_NORMAL or no fadv done atall, we predict the\
         * number of pages the OS might have prefetched and add those to the heap.
         *
         * Broadly speaking, each linux kernel version has its own prefetching logic.
         *
         * The below information about the kernel is for linux version upto 5.0.xx
         * Verify all of the info before doing any of the steps involving it.
         * there are three variables inside the kernel which come from the corresponding files:
         * bdi->io_pages - /sys/block/<dev>/queue/max_hw_sectors_kb
         *               - /sys/block/<dev>/queue/max_sectors_kb
         * bdi->ra_pages - VM_MAX_READAHEAD [set to 128 KB]
         * ra->ra_pages - bdi->ra_pages (x 2)
         *
         * How many pages are prefetched by the kernel depends on:
         * 1. Resident pages in the same spatial locality of the current req
         * 2. How seq does the OS think this file is (used as a multiplier for prefetch window)
         * 3. If there is congestion on the block device
         * 4. what is the size of the read
         *      - eg. large reads are broken into multiple smaller reads affecting prefetch window
         * 5. How much memory is available on the system.
         * etc.
         *
         * Since most of these things are not visible to the userspace, it is very difficult to
         * predict how much the OS has prefetched without actually querying the cache state of
         * said file using mincore (a very expensive operation).
         * It has been noted that for a given read request, it is safe to assume that the max
         * prefetched size decided by the OS will always be ≤ max_hw_sectors_kb. So, as a safe
         * upper limit we can add this number of each read size for the book keeping.
         *
         * Now, the problem is that cassandra allows multiple devices for its data base; which
         * means that for a given read, one has to also identify which device it came from.
         * This can be identified in handle_open and maintained in the struct inode.
         *
         * We are going to omit implementing this feature for now; reasons:
         * 1. ENABLE_POSIX_FADV_RANDOM_FOR_WHITELISTED_FILES disables any OS prefetching for all the
         * whitelisted files. This means we never hit a scenario where the OS is prefetching in the
         * normal course of things. (handle_open())
         * 2. We NOOP any FADV_SEQ, FADV_NORMAL from the user application. (handle_fadvise())
         * 3. The only time FADV_SEQ is called for files Cassandra is using for compactions (handle_fadvise())
         * 4. Such files are soon to be unlinked by cassandra anyway. Cassandra also calls FADV_DONTNEED
         * for them proactively.
         *
         * So, the possibility of performance loss due to unaccounted pages from these moribund
         * files is slim.
         *
         * In the future as in when this feature becomes important do the following:
         * 1. get the list of all block devices which will store whitelisted files (from the config file)
         * 2. get their relevant linux kernel counters such as read_ahead_kb, max_hw_sectors_kb, max_sectors_kb
         * 3. identify which device does this particular read pertain to (from its struct inode)
         * 4. Calculate a comfortable max prefetch window given these linux counters and add it to size.
         * Note: read_ahead_kb and max_sectors_kb are tunable parameters; you will have to check these periodically.
         *
         */
        if(get_fadv_from_uinode(uinode) && from_read){
                size += 0;
        }

        first_portion_nr = PORTION_NR_FROM_OFFSET(offset, portion_order);
        last_portion_nr  = PORTION_NR_FROM_OFFSET(offset + size - 1, portion_order);

        /*
                1. check if this portion already in the heap_node_array
                2. if yes, then update the key
                3. if not, then insert new portion into the heap
        */

        // printf("%s: {ino:%lu, dev:%lu}, offset:%ld, size:%ld portion_sz:%ld\n",
        //        __func__, uinode->ino, uinode->dev_id, curr_offset, size_left, portion_sz);

        for (portion_nr = first_portion_nr; portion_nr <= last_portion_nr; portion_nr++) {  // Insert/update each portion in the heap

                uinode->file_heap_lock.lock();

                /*Update the nr_accesses once for this syscall*/
                if(portion_nr == first_portion_nr){
                        uinode->nr_accesses += 1;

#ifdef GHEAP_TRIGGER
                        uinode->gheap_trigger->now += 1;
#endif //GHEAP_TRIGGER
                }

                /*check if this portion is already allocated*/
                if((*uinode->file_heap_node_ids)[portion_nr] == -1){

                        off_t *p_nr = (off_t*)malloc(sizeof(off_t));
                        if(unlikely(!p_nr)){
                                SPEEDYIO_FPRINTF("%s:ERROR malloc failed p_nr\n", "SPEEDYIO_ERRCO_0173\n");
                                uinode->file_heap_lock.unlock();
                                KILLME();
                                goto exit_update_pvt_heap;
                        }
                        *p_nr = portion_nr;
#ifdef EVICTION_FREQ
                        portion_key = 1;
                        /*
                        printf("heap_insert id:%d, {ino:%lu, dev:%lu}, portion_nr:%ld\n",
                                (*uinode->file_heap_node_ids)[portion_nr], uinode->ino, uinode->dev_id, portion_nr);
                        */
#elif defined(EVICTION_LRU) && defined(BELADY_PROOF)
                        portion_key = timestamp;
#elif EVICTION_LRU
                        portion_key = ticks_now() - first_rdtsc;
#elif defined(ENABLE_EVICTION)
#error "Only EVICTION_FREQ and EVICTION_LRU implemented for update_pvt_heap"
#endif //EVICTION_FREQ and EVICTION_LRU

                        (*uinode->file_heap_node_ids)[portion_nr] =
                                                heap_insert(uinode->file_heap, portion_key, (void*)p_nr);

                        if(unlikely((*uinode->file_heap_node_ids)[portion_nr] == -1)){
                                SPEEDYIO_FPRINTF("%s:ERROR heap_insert failed {ino:%lu, dev:%lu} portion_nr:%ld\n", "SPEEDYIO_ERRCO_0174 %lu %lu %ld\n", uinode->ino, uinode->dev_id, portion_nr);
                                uinode->file_heap_lock.unlock();
                                KILLME();
                                goto exit_update_pvt_heap;
                        }

                        // SPEEDYIO_PRINTF("%s:INFO called heap_insert for {ino:%lu, dev:%lu} portion_nr:%ld (*uinode->file_heap_node_ids)[portion_nr]:%d\n", "SPEEDYIO_INFOCO_0018 %lu %lu %ld %d\n", uinode->ino, uinode->dev_id, portion_nr, (*uinode->file_heap_node_ids)[portion_nr]);

                }
                else { //portion already in the heap
#if defined(EVICTION_LRU) && defined(BELADY_PROOF)
                        portion_key = timestamp;
#elif EVICTION_LRU
                        portion_key = ticks_now() - first_rdtsc;
#elif EVICTION_FREQ
                        portion_key = heap_get_key_by_id(uinode->file_heap,
                                                (*uinode->file_heap_node_ids)[portion_nr]);

                        if(unlikely(portion_key == ADD_TO_KEY_REDUCE_PRIORITY)){
                                SPEEDYIO_FPRINTF("%s:MISCONFIG portion_key is equal to ADD_TO_KEY_REDUCE_PRIORITY!! increase this limit !\n", "SPEEDYIO_MISCONFIGCO_0005\n");
                                uinode->file_heap_lock.unlock();
                                KILLME();
                                goto exit_update_pvt_heap;
                        }

                        /*This means the node was previously evicted. Now being accessed again*/
                        if(portion_key > ADD_TO_KEY_REDUCE_PRIORITY){
                                        //resume from the previous freq.
                                        portion_key -= ADD_TO_KEY_REDUCE_PRIORITY;
                        }
                        portion_key += 1;

#elif defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP) && defined(EVICTION_COMPLEX)
#error "only EVICTION_FREQ and EVICTION_LRU implemented for pvt heap, not EVICTION_COMPLEX"
#endif //EVICTION_LRU and BELADY_PROOF

                        // SPEEDYIO_PRINTF("%s:INFO calling heap_update_key for {ino:%lu, dev:%lu} portion_nr:%ld uinode->file_heap_node_ids[portion_nr]:%d\n", "SPEEDYIO_INFOCO_0019 %lu %lu %ld %d\n", uinode->ino, uinode->dev_id, portion_nr, (*uinode->file_heap_node_ids)[portion_nr]);
                        heap_update_key(uinode->file_heap, (*uinode->file_heap_node_ids)[portion_nr], portion_key);
                }

                current_min = heap_read_min(uinode->file_heap)->key;
                uinode->file_heap_lock.unlock();
        }

// #ifdef ENABLE_MINCORE_DEBUG
//         if(nr_pvt_heap_calls % 10000 != 0){
//                 goto unlock_mincore_exit;
//         }
//         // debug_printf("%s:INFO MINCORE called for {ino:%lu, dev:%lu} \t offset: %zu \t size: %lu\n", __func__, uinode->ino, uinode->dev_id, offset, size);
//         mincore_arr = check_mincore(uinode);
//         if (portion_sz != PAGESIZE) {debug_printf("%s: WARNING: cannot compare pages accurately coz heap portion_sz: %ld is not equal to PAGESIZE", __func__, portion_sz);}
//         all_hkeys = heap_get_all_keys(uinode->file_heap);
//         for (auto in_memory: mincore_arr) {
//                 if (in_memory) {
//                         mincore_in_mem_count += 1;
//                 }
//         }
//         for (auto hkey: all_hkeys) {
//                 if (hkey != ULONG_MAX) {
//                         heap_in_mem_count += 1;
//                 }else{
//                         heap_out_of_mem_count += 1;
//                 }
//         }
//         if ( (mincore_in_mem_count - heap_in_mem_count >= 1 || mincore_in_mem_count - heap_in_mem_count <= -1)) {
//                 debug_printf("%s: tid: %d \t counts mismatch for {ino:%lu, dev:%lu} \t mincore_in_mem_count: %lld \t heap_in_mem_count: %lld \t heap_out_of_mem_count: %lld \t offset: %zu \t size: %lu  heap_size: %lu\n",
//                         __func__, tid, uinode->ino, uinode->dev_id, mincore_in_mem_count, heap_in_mem_count, heap_out_of_mem_count, offset, size, uinode->file_heap->size);

//                 if (fstat(uinode->fdlist[0].fd, &file_stat) == -1){
//                                 perror("fstat");
//                                 throw std::runtime_error("update_heap: failed to get file size");
//                 }
//                 print_mincore_array(mincore_arr, file_stat.st_size);

//                 debug_printf("BITMAP $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");

//                 #ifdef ENABLE_PER_INODE_BITMAP
//                 BitArrayPrint(uinode->cache_state);
//                 #endif //ENABLE_PER_INODE_BITMAP

//                 throw std::runtime_error("update_heap: counts mismatch");
//         }
// unlock_mincore_exit:
//         uinode->file_heap_lock.unlock(); // CAREFUL: REVERT TO DOING THE UNLOCK INSIDE THE LOOP IN NORMAL FLOW. LOCK NEEDS TO BE OUTSIDE WHILE DOING THIS DEBUGGING
// #endif // ENABLE_MINCORE_DEBUG

exit_update_pvt_heap:
        return current_min;
}

/*TODO: Take the pvt heap lock to delete this pvt heap*/
void destroy_pvt_heap(struct Heap *pvt_heap){
        if(!pvt_heap){
                SPEEDYIO_FPRINTF("%s:ERROR invalid pvt_heap\n", "SPEEDYIO_ERRCO_0175\n");
                goto exit_destroy_pvt_heap;
        }

        debug_printf("%s: destroying fileheap\n", __func__);
        heap_destroy(pvt_heap);

exit_destroy_pvt_heap:
        return;
}

/*Returns the key for the min in the pvt heap of this uinode*/
unsigned long long int get_min_key(struct inode* uinode){
        unsigned long long int ret = 0;
        HeapItem *min = nullptr;

        uinode->file_heap_lock.lock();
        if(!uinode->file_heap){
                goto unlock_and_return;
        }

        min = heap_read_min(uinode->file_heap);
        if(!min){
                SPEEDYIO_FPRINTF("%s:ERROR min is NULL {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0176 %lu %lu\n", uinode->ino, uinode->dev_id);
                goto unlock_and_return;
        }
        ret = min->key;

unlock_and_return:
        uinode->file_heap_lock.unlock();
        return ret;
}

/*Eviction thread functions*/

/**
 * This function returns the file to be evicted
 * returns: the victim inode with unlinked_lock taken.
 *
 * NOTE: The caller will have to unlock the unlinked_lock
 * after it is done operating on the victim_uinode
 */
struct inode *get_victim_uinode(){

        int victim_file_id = -1;
        struct HeapItem *victim_file_data = nullptr;
        struct inode *victim_uinode = nullptr;

        g_heap_lock.lock();

        debug_printf("%s: total_nodes:%d\n", __func__, g_file_heap->size);

        if(unlikely(g_file_heap->size < MIN_FILES_REQD_TO_EVICT)){
                goto unlock_and_return;
        }

        victim_file_data = heap_read_min(g_file_heap);

        if(unlikely(!victim_file_data)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_file_data is NULL\n", "SPEEDYIO_ERRCO_0177\n");
                goto unlock_and_return;
        }

#if defined(EVICTION_FREQ)
        //Do not return the data which has been evicted earlier but not accessed since.
        if(victim_file_data->key >= ADD_TO_KEY_REDUCE_PRIORITY){
                // SPEEDYIO_FPRINTF("%s: victim_file_data->key is gt ADD_TO_KEY_REDUCE_PRIORITY\n", "SPEEDYIO_OTHERCO_0006\n");
                goto unlock_and_return;
        }
#endif
        victim_uinode = (struct inode*)victim_file_data->dataptr;

        if(unlikely(!victim_uinode)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_uinode is null\n", "SPEEDYIO_ERRCO_0178\n");
                goto unlock_and_return;
        }

        if(!victim_uinode->unlinked_lock.try_lock()){
                /**
                 * Unable to take unlinked_lock. This can mean:
                 * 1. It is being unlinked [check_fdlist_and_unlink]
                 * 2. It is being put [iter_i_map_and_put_unused]
                 * 3. It is being reused [add_fd_to_inode]
                 * 4. It's nr_links being updated [update_nr_links]
                 *
                 * Right after uinode->unlinked is set, said uinode
                 * is removed from g_heap. Since we are here, it could
                 * be in an intermediate state. Better to not operate
                 * on it for now.
                 *
                 * XXX: Check if we should update the heap aswell.
                 */
                cfprintf(stderr, "%s:WARNING failed to take unlinked_lock on ino:%lu, dev_id:%lu.. Skipping\n",
                        __func__, victim_uinode->ino, victim_uinode->dev_id);
                victim_uinode = nullptr;
                goto unlock_and_return;
        }

        /**
         * At this point we have the uinode->unlinked_lock.
         * This will prevent the following during evictions:
         * 1. Being unlinked [check_fdlist_and_unlink]
         * This will not prevent fds being removed from the fdlist [handle_close].
         * But in an odd scenario where the file is being unlinked, syscall will
         * be blocked from the OS while we are evicting it.
         * This should not happen very often.
         *
         * 2. Being put by the bg cleaner. [iter_i_map_and_put_unused]
         * This case is extremely unlikely since unlinked uinodes are removed from
         * the g_heap immediately, so we shouldn't see this uinode in the first place.
         *
         * 3. Being reused [add_fd_to_inode]
         * This is also extremely unlikely since this requires the OS to reuse the
         * ino for another file which is infrequent.
         *
         * All of these conditions, while unlikely, have
         * been safeguarded to preserve correctness.
         */

        if(unlikely(victim_uinode->is_deleted())){
                /**
                 * A deleted victim_uinode need not be evicted.
                 */
                SPEEDYIO_FPRINTF("%s:NOTE victim_uinode is_deleted\n", "SPEEDYIO_ERRCO_0178\n");
                victim_uinode->unlinked_lock.unlock();
                victim_uinode = nullptr;
                goto unlock_and_return;
        }

#if defined(EVICTION_FREQ)
        /**
         * Adding ADD_TO_KEY_REDUCE_PRIORITY does the following things
         * 1. lowers the priority(to back to the queue) of this file in the gheap
         * 2. Does not lose the current frequency of the file.
         * subtracting ADD_TO_KEY_REDUCE_PRIORITY will reveal it.
         * 3. Marks that this file has been subject to eviction if
         * it surfaces again as a potential victim uinode.
         */
        heap_update_key(g_file_heap, victim_uinode->heap_id,
                victim_file_data->key + ADD_TO_KEY_REDUCE_PRIORITY);

#elif defined(EVICTION_LRU)

#if defined(VICTIM_UINODE_ULONGMAX) || defined(BELADY_PROOF)
        /**
         * BELADY_PROOF passes timestamp from the replay file.
         * So, if we set the new time to current __rdtsc, it would
         * be incorrect. For global_lru ULONG_MAX is correct;
         * pvt_lru updates the value to that uinode's min later which
         * is correct.
         */
        heap_update_key(g_file_heap, victim_uinode->heap_id, ULONG_MAX);
#else //only global Heap
        heap_update_key(g_file_heap, victim_uinode->heap_id, (ticks_now()-first_rdtsc));
#endif //ENABLE_PVT_HEAP

#elif defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP) && defined(EVICTION_COMPLEX)
#error "only EVICTION_FREQ and EVICTION_LRU implemented for pvt heap"
#endif //EVICTION_FREQ

unlock_and_return:
        g_heap_lock.unlock();
        return victim_uinode;
}


void evict_full_file(struct inode *inode){
        int result;
        int fd;
        int opened = false;
        std::string err;

        //double elapsed_seconds = (__rdtsc() - first_rdtsc) / cpu_freq;

        if(unlikely(!inode)){
                SPEEDYIO_FPRINTF("%s:ERROR inode in nullptr\n", "SPEEDYIO_ERRCO_0179\n");
                goto exit_evict_full_file;
        }
        fd = inode->fdlist[0].fd;
        if(unlikely(fd < 3)){
                fd = real_open(inode->filename, O_RDONLY, 0);
                if(fd == -1){
                        SPEEDYIO_FPRINTF("%s:ERROR failed to open %s\n", "SPEEDYIO_ERRCO_0180 %s\n", inode->filename);
                        goto exit_evict_full_file;
                }
                opened = true;
        }

#ifdef SYNC_BEFORE_FULL_EVICT
        sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER);
#endif

        result = real_posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);

        if(result != 0){
                debug_fprintf(stderr, "%s: posix_fadvise failed: %s {ino:%lu, dev:%lu}, fd:%d\n",
                                __func__, strerror(result), inode->ino, inode->dev_id, inode->fdlist[0]);
        }

        if(opened){
                real_close(fd);
        }

exit_evict_full_file:
        return;
}

void evict_file_portion(struct inode *uinode, int fd, off_t offset, size_t size){
        int result;
        int opened = false;
        std::string err;
        off_t end, pos, this_size;

// #ifdef ENABLE_MINCORE_DEBUG
//         std::vector<bool> mincore_arr;
//         size_t start_page;
//         size_t end_page;
//         size_t i;
// #endif

        // if(unlikely(fd < 3 || offset < 0 || size <= 0)){
        if(unlikely(offset < 0 || size <= 0)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid fd:%d\n", "SPEEDYIO_ERRCO_0181 %d\n", fd);
                goto exit_evict_file_portion;
        }

        if(fd < 3){
                fd = real_open(uinode->filename, O_RDONLY, 0);
                if(fd == -1){
                        // cfprintf(stderr, "%s:NOTE failed to open %s ...Skipping\n", __func__, uinode->filename);
                        goto exit_evict_file_portion;
                }
                opened = true;
        }

        // printf("%s: evicting filename:%s\n", __func__, uinode->filename);

#ifndef NOSYNC_BEFORE_RANGE_EVICT
        sync_file_range(fd, offset, size, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER);
#endif

#ifdef DBG_FADV_SLEEP
        usleep(10000);
#endif //DBG_FADV_SLEEP

#ifndef DBG_NO_DONTNEED

        #ifdef SMALLER_FADVISE
                end = offset + size;
                for(pos = offset; pos < end; pos += FADV_CHUNK_KB){
                        this_size = (end - pos < FADV_CHUNK_KB) ? (end - pos) : FADV_CHUNK_KB;
                        result = real_posix_fadvise(fd, pos, this_size, POSIX_FADV_DONTNEED);
                }
        #else
                result = real_posix_fadvise(fd, offset, size, POSIX_FADV_DONTNEED);
        #endif //SMALLER_FADVISE


        if(result != 0){
                debug_fprintf(stderr, "%s: posix_fadvise failed: %s fd:%d\n",
                                __func__, strerror(result), fd);
        }

#endif //DBG_NO_DONTNEED


        if(opened){
                real_close(fd);
        }


        /*MINCORE stuff*/
// #ifdef ENABLE_MINCORE_DEBUG
//         mincore_arr = check_mincore(uinode);
//         // Get the system page size (or use a defined constant PAGESIZE if available)

//         // Calculate the first and last page numbers for the given offset and size
//         start_page = offset / PAGESIZE;
//         end_page   = (offset + size - 1) / PAGESIZE;
//         // Check the mincore array for each page in the range
//         for (i = start_page; i <= end_page; i++) {
//             if (mincore_arr[i]) {
//                 throw std::runtime_error("evict_file_portions: Error: One or more pages in the specified range are still in cache.");
//             }
//         }
// #endif //ENABLE_MINCORE_DEBUG

exit_evict_file_portion:
        return;
}


/*
 * This function checkes if the current portion of this file is to be evicted
 * vs the first portion of the next potential victim file.
 * XXX: THIS IS NOT UPDATED. LOOK AT IT BEFORE USING
 */
bool keep_evicting_from_this_file(struct inode *victim_inode){
        /*
         * 1. Get the next file from the g_heap
         * 2. get its first victim portion
         * 3. check if victim_inode.portion.freq <= EVICTION_MULTIPLIER_THETA *
         *                                          2nd_victim_inode.portion.freq
         * currently this works well only for EVICTION_FREQ.
         * with EVICTION_LRU, we have observed a performance drop using this.
         * Hence we only use this for EVICTION_FREQ.
         * This function is not tested for EVICTION_COMPLEX.
         */

        float victim_portion_freq;
        int victim_portion_id = -1;
        HeapItem *next_victim;
        HeapItem *victim_portion;
        HeapItem *victim_portion_2;
        float victim_portion_2_freq;
        struct inode *next_victim_inode;

        victim_inode->file_heap_lock.lock();
        victim_portion = heap_read_min(victim_inode->file_heap);
        if(unlikely(!victim_portion)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_portion is NULL {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0183 %lu %lu\n", victim_inode->ino, victim_inode->dev_id);
                victim_inode->file_heap_lock.unlock();
                return false;
        }
        victim_portion_freq = victim_portion->key;
        victim_portion_id = victim_portion->id;
        victim_inode->file_heap_lock.unlock();

#ifdef EVICTION_FREQ
        if(victim_portion_freq >= ADD_TO_KEY_REDUCE_PRIORITY){
                //printf("returning false {ino:%lu, dev:%lu}, freq:%f portion_id:%d\n", victim_inode->ino, victim_uinode->dev_id, victim_portion_freq, victim_portion_id);
                return false;
        }
#endif

        g_heap_lock.lock();
        next_victim = heap_read_min(g_file_heap);
        next_victim_inode = (struct inode*) next_victim->dataptr;
        g_heap_lock.unlock();

        next_victim_inode->file_heap_lock.lock();
        victim_portion_2 = heap_read_min(next_victim_inode->file_heap);
        if(unlikely(!victim_portion_2)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_portion_2 is NULL {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0184 %lu %lu\n", next_victim_inode->ino, next_victim_inode->dev_id);
                next_victim_inode->file_heap_lock.unlock();
                return false;
        }
        victim_portion_2_freq = victim_portion_2->key;
        next_victim_inode->file_heap_lock.unlock();

        if((victim_portion_2_freq * EVICTION_MULTIPLIER_THETA) < victim_portion_freq){
                //next file has low freq items. goto that.

                /*update the global heap with the current pvt heap min in the victim_inode*/
                g_heap_lock.lock();
                heap_update_key(g_file_heap, victim_inode->heap_id, victim_portion_freq);
                g_heap_lock.unlock();

                //printf("victim_1_freq:%f, victim_2_freq:%f HENCE CHANGING FILE\n", victim_portion_freq, victim_portion_2_freq);
                return false;
        }

        // printf("%s: min first portion:%f {ino:%lu, dev:%lu}, min second victim:%f, {ino:%lu, dev:%lu}\n", __func__,
        //         victim_portion_freq, victim_inode->ino, victim_inode->dev_id,
        //         victim_portion_2_freq, next_victim_inode->ino, next_victim_inode->dev_id);
exit:
        return true;
}


#ifdef BELADY_PROOF

/**
 * Eviction event using one lru.
 */
struct mock_eviction_item *evict_from_one_lru(long sz_to_claim_kb)
{
        size_t portion_sz = 1UL << (PAGE_SHIFT + PVT_HEAP_PG_ORDER);
        struct HeapItem* victim_portion;
        struct lru_entry *victim_entry;
        struct inode *uinode = nullptr;

        struct mock_eviction_item *eviction_event = nullptr;
        eviction_event = (struct mock_eviction_item*)malloc(sizeof(struct mock_eviction_item));
        if(!eviction_event){
                SPEEDYIO_FPRINTF("%s:ERROR malloc failed for eviction_event\n", "SPEEDYIO_ERRCO_0185\n");
                KILLME();
        }
        eviction_event->ino = 0UL;
        eviction_event->dev_id = 0UL;
        eviction_event->offset = 0;
        eviction_event->size = -1;

        g_heap_lock.lock();

        victim_portion = heap_read_min(g_file_heap);

        if(!victim_portion){
                SPEEDYIO_FPRINTF("%s:ERROR victim_file_data is nullptr\n", "SPEEDYIO_ERRCO_0186\n");
                KILLME();
        }

        victim_entry = (struct lru_entry*)victim_portion->dataptr;

        if(!victim_entry || !victim_entry->uinode){
                SPEEDYIO_FPRINTF("%s:ERROR victim_entry or its uinode is nullptr\n", "SPEEDYIO_ERRCO_0187\n");
                KILLME();
        }

        uinode = victim_entry->uinode;

        eviction_event->ino = uinode->ino;
        eviction_event->dev_id = uinode->dev_id;
        eviction_event->size = portion_sz;
        eviction_event->offset = (victim_entry->portion_nr*portion_sz);

#ifdef EVICTION_LRU
        heap_update_key(g_file_heap, (*uinode->file_heap_node_ids)[victim_entry->portion_nr], ULONG_MAX);
#else
#error "only EVICTION_LRU implemented in ONE_LRU"
#endif

        g_heap_lock.unlock();

        if(!eviction_event || eviction_event->ino == -1){
                free(eviction_event);
                eviction_event = nullptr;
        }
        return eviction_event;
}
#endif //BELADY_PROOF


void new_evict_file_portion(int fd, off_t offset, size_t size){
        int result;
        off_t end, pos, this_size;

        if(unlikely(fd < 3 || offset < 0 || size <= 0)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid fd:%d\n", "SPEEDYIO_ERRCO_0181 %d\n", fd);
                goto exit_new_evict_file_portion;
        }

#ifndef NOSYNC_BEFORE_RANGE_EVICT
        sync_file_range(fd, offset, size, SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER);
#endif //NOSYNC_BEFORE_RANGE_EVICT

#ifndef DBG_NO_DONTNEED

        printf("%s: called for fd:%d, offset:%lu, size:%ld\n", __func__, fd, offset, size);

        #ifdef SMALLER_FADVISE
                end = offset + size;
                for(pos = offset; pos < end; pos += FADV_CHUNK_KB){
                        this_size = (end - pos < FADV_CHUNK_KB) ? (end - pos) : FADV_CHUNK_KB;
                        result = real_posix_fadvise(fd, pos, this_size, POSIX_FADV_DONTNEED);
                }
        #else //ONE big fadvise
                result = real_posix_fadvise(fd, offset, size, POSIX_FADV_DONTNEED);
        #endif //SMALLER_FADVISE

#endif //DBG_NO_DONTNEED


exit_new_evict_file_portion:
        return;        
}


long new_evict_portions(long sz_to_claim_kb){

        struct inode *victim_inode = nullptr;
        int victim_portion_id = -1;
        unsigned long long int victim_portion_key;
        unsigned long long int last_victim_portion_key;
        struct HeapItem* victim_portion;
        struct HeapItem* last_victim_portion;
        long size_claimed_kb = 0;
        size_t portion_sz = 1UL << (PAGE_SHIFT + PVT_HEAP_PG_ORDER);
        off_t portion_nr;
        int fd;
        bool exit = false;
        struct timespec start, end;

        // printf("%s CALLED XXXXXXXXXXX\n", __func__);

        if(unlikely(sz_to_claim_kb <= 0)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid sz_to_claim_kb:%ld\n", "SPEEDYIO_ERRCO_0189 %ld\n", sz_to_claim_kb);
                goto exit_new_evict_portions;
        }

        victim_inode = get_victim_uinode();
        if(unlikely(!victim_inode)) {
                goto exit_new_evict_portions;
        }
        if(unlikely(!victim_inode->file_heap)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_inode has no file_heap\n", "SPEEDYIO_ERRCO_0190\n");
                goto exit_new_evict_portions;
        }

        victim_inode->file_heap_lock.lock();

        victim_portion = heap_read_min(victim_inode->file_heap);
        if(unlikely(!victim_portion)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_portion is nullptr {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0191 %lu %lu\n", victim_inode->ino, victim_inode->dev_id);
                exit = true;
                goto skip_eviction;
        }

        victim_portion_id = victim_portion->id;
        victim_portion_key = victim_portion->key;
        if (victim_portion_key == ULONG_MAX) {
                exit = true;
                goto skip_eviction;
        }

        portion_nr = *(off_t*)victim_portion->dataptr;

        fd = victim_inode->fdlist[0].fd;

        size_claimed_kb += portion_sz / KB;


        clock_gettime(CLOCK_MONOTONIC, &start);

        heap_update_key(victim_inode->file_heap, victim_portion_id, ULONG_MAX);

        clock_gettime(CLOCK_MONOTONIC, &end);
        bin_time_to_pow2_us(start, end, &ulong_heap_update);

skip_eviction:

        last_victim_portion = heap_read_min(victim_inode->file_heap);
        last_victim_portion_key = last_victim_portion->key;


        g_heap_lock.lock();
        heap_update_key(g_file_heap, victim_inode->heap_id, last_victim_portion_key);
        g_heap_lock.unlock();


unlock_exit:
        victim_inode->file_heap_lock.unlock();
        
        /*Releasing unlinked_lock which was held by get_victim_uinode*/
        victim_inode->unlinked_lock.unlock();
        
        if(exit = true){
                // printf("%s: exiting XXXXXXXXX\n", __func__);
                goto exit_new_evict_portions;
        }

        // eviction outside the lock
        new_evict_file_portion(fd, (portion_nr*portion_sz), portion_sz);


exit_new_evict_portions:
        return size_claimed_kb;
}



/*
 * This function evicts portions of low priority files
 * until the size_tobe_cleared is achieved
 *
 * returns the amount of memory reclaimed
 *
 * XXX: This function might be problematic if the file has been deleted in the middle of eviction
 *
 * XXX: This function will not work with EVICTION_COMPLEX
 */
#ifdef BELADY_PROOF
struct mock_eviction_item *evict_portions(long sz_to_claim_kb)
#else
long evict_portions(long sz_to_claim_kb)
#endif
{
        struct inode *victim_inode = nullptr;
        int victim_portion_id = -1;
        unsigned long long int victim_portion_key;
        unsigned long long int last_victim_portion_key;
        struct HeapItem* victim_portion;
        struct HeapItem* last_victim_portion;
        long size_claimed_kb = 0;
        size_t portion_sz = 1UL << (PAGE_SHIFT + PVT_HEAP_PG_ORDER);
        off_t portion_nr;
        bool exit = false;
        int fd;
        struct timespec start, end;

#ifdef BELADY_PROOF
        struct mock_eviction_item *eviction_event = nullptr;
        eviction_event = (struct mock_eviction_item*)malloc(sizeof(struct mock_eviction_item));
        if(!eviction_event){
                SPEEDYIO_FPRINTF("%s:ERROR malloc failed for eviction_event\n", "SPEEDYIO_ERRCO_0188\n");
                // KILLME();
                goto exit_evict_portions;
        }
        eviction_event->ino = 0;
        eviction_event->dev_id = 0;
#endif //BELADY_PROOF

        if(unlikely(sz_to_claim_kb <= 0)){
                SPEEDYIO_FPRINTF("%s:ERROR invalid sz_to_claim_kb:%ld\n", "SPEEDYIO_ERRCO_0189 %ld\n", sz_to_claim_kb);
                goto exit_evict_portions;
        }

        victim_inode = get_victim_uinode();
        if(unlikely(!victim_inode)) {
                goto exit_evict_portions;
        }
        if(unlikely(!victim_inode->file_heap)){
                SPEEDYIO_FPRINTF("%s:ERROR victim_inode has no file_heap\n", "SPEEDYIO_ERRCO_0190\n");
                goto exit_evict_portions;
        }

#ifdef DBG_ONLY_GET_VICTIM_UINODE
        victim_inode->unlinked_lock.unlock();
        goto exit_evict_portions;
#endif //DBG_ONLY_GET_VICTIM_UINODE


#ifdef ENABLE_UINODE_LOCK
        /**
         * Since get_victim_uinode takes victim_uinode->unlinked_lock
         * this location might be incorrect for uinode_lock.
         *
         * uinode_lock should be the first lock taken for a uinode and
         * relinquished last.
         *
         * XXX: Check and update the location of uinode_lock:lock and unlock.
         */
        victim_inode->uinode_lock.lock();
#endif //ENABLE_UINODE_LOCK

        do{
                //get portion from uinode
                victim_inode->file_heap_lock.lock();

                victim_portion = heap_read_min(victim_inode->file_heap);
                if(unlikely(!victim_portion)){
                        SPEEDYIO_FPRINTF("%s:ERROR victim_portion is nullptr {ino:%lu, dev:%lu}\n", "SPEEDYIO_ERRCO_0191 %lu %lu\n", victim_inode->ino, victim_inode->dev_id);
                        exit = true;
                        goto err_unlock_exit;
                }
                victim_portion_id = victim_portion->id;
                victim_portion_key = victim_portion->key;
                // printf("victim_inode: %d\tvictim_portion_id: %d\tvictim portion key: %llu %d\n", 
                //         victim_inode->ino, victim_portion_id, victim_portion_key, victim_portion_key == ULONG_MAX);
                if (victim_portion_key == ULONG_MAX) {
                        exit = true;
                        goto err_unlock_exit;
                }

#ifdef EVICTION_FREQ
                if(unlikely(victim_portion->key == ADD_TO_KEY_REDUCE_PRIORITY)){
                        //This portion has seen frequencies added to ADD_TO_KEY_REDUCE_PRIORITY
                        SPEEDYIO_FPRINTF("%s:MISCONFIG victim_portion_data->key is equal to ADD_TO_KEY_REDUCE_PRIORITY increase ADD_TO_KEY_REDUCE_PRIORITY\n", "SPEEDYIO_MISCONFIGCO_0006\n");
                        exit = true;
                        goto err_unlock_exit;
                }
                else if(victim_portion->key > ADD_TO_KEY_REDUCE_PRIORITY){
                        //this portion has been evicted and not accessed since
                        //this means no other portion is accessed and hence exit this file
                        /*
                        // SPEEDYIO_FPRINTF("%s: victim_portion->key=%f is gt ADD_TO_KEY_REDUCE_PRIORITY=%d {ino:%lu, dev:%lu}\n", "SPEEDYIO_OTHERCO_0007 %f %d %lu %lu\n", victim_portion->key, ADD_TO_KEY_REDUCE_PRIORITY, victim_inode->ino, victim_inode->dev_id);
                        */
                        exit = true;
                        goto err_unlock_exit;
                }

// #elif defined(ENABLE_EVICTION) && defined(ENABLE_PVT_HEAP) && !defined(EVICTION_FREQ)
// #error "only EVICTION_FREQ implemented for pvt heap"

#endif //EVICTION_FREQ
                //evict portion
                portion_nr = *(off_t*)victim_portion->dataptr;

                // printf("%s: {ino:%lu, dev:%lu}, portion_nr:%ld, off:%ld, size:%ld freq:%f inode_freq:%f nr_accesses:%ld\n",
                //         __func__, victim_inode->ino, victim_inode->dev_id, portion_nr, (portion_nr*portion_sz), portion_sz,
                //         victim_portion->key, heap_get_key_by_id(g_file_heap, victim_inode->heap_id) - ADD_TO_KEY_REDUCE_PRIORITY,
                //         victim_inode->nr_accesses);

                /**
                 * Files that are in the cache, all fds removed and not unlinked should still be able to evict
                 *
                 * XXX: we are currently opening and closing a file without an active
                 * fdlist. This is a stop gap solution. Think about a better solution.
                 */
                // if(victim_inode->fdlist_index == -1){
                // SPEEDYIO_FPRINTF("%s:WARNING fdlist_index is -1; {ino:%lu, dev:%lu}\n", "SPEEDYIO_WARNCO_0007 %lu %lu\n", victim_inode->ino, victim_inode->dev_id);
                //         exit = true;
                //         goto err_unlock_exit;
                // }

// #if defined(ENABLE_MINCORE_DEBUG) && defined(ENABLE_PER_INODE_BITMAP)
//                 clear_range_bitmap(victim_inode, PG_NR_FROM_OFFSET(portion_nr*portion_sz)-1, BYTES_TO_PG(portion_sz));
// #endif //ENABLE_MINCORE_DEBUG && ENABLE_PER_INODE_BITMAP

                // SPEEDYIO_PRINTF("%s:INFO evicting portion_nr:%ld, {ino:%lu, dev:%lu}, fd:%d, offset:%ld, size:%lu\n", "SPEEDYIO_INFOCO_0020 %ld %lu %lu %d %ld %lu\n", portion_nr, victim_inode->ino, victim_inode->dev_id, victim_inode->fdlist[0].fd, (portion_nr*portion_sz), portion_sz);

#ifdef BELADY_PROOF
                eviction_event->ino = victim_inode->ino;
                eviction_event->dev_id = victim_inode->dev_id;
                eviction_event->offset = (portion_nr*portion_sz);
                eviction_event->size = portion_sz;
#else

#ifndef EVICTOR_OUTSIDE_LOCK
                evict_file_portion(victim_inode, victim_inode->fdlist[0].fd, (portion_nr*portion_sz), portion_sz);
#else
                fd = victim_inode->fdlist[0].fd;
#endif //EVICTOR_OUTSIDE_LOCK

#endif //BELADY_PROOF

                size_claimed_kb += portion_sz / KB;

#ifndef DBG_DISABLE_DOWHILE_UPDATEKEY

#ifdef EVICTION_FREQ
                //reduce priority for this portion
                heap_update_key(victim_inode->file_heap, victim_portion_id,
                        (victim_portion->key+ADD_TO_KEY_REDUCE_PRIORITY));

#elif EVICTION_LRU
                clock_gettime(CLOCK_MONOTONIC, &start);

                heap_update_key(victim_inode->file_heap, victim_portion_id, ULONG_MAX);

                clock_gettime(CLOCK_MONOTONIC, &end);
                bin_time_to_pow2_us(start, end, &ulong_heap_update);

#elif defined(ENABLE_EVICTION)
#error "only EVICTION_FREQ && EVICTION_LRU implemented for pvt heap"
#endif //EVICTION_FREQ

#endif //DBG_DISABLE_DOWHILE_UPDATEKEY

err_unlock_exit:
                victim_inode->file_heap_lock.unlock();

                if(exit == true){
                        break;
                }
        }
#ifdef EVICTION_LRU
        while(false);
        // while(size_claimed_kb < sz_to_claim_kb);
#elif EVICTION_FREQ
        #error "keep_evicting_from_this_file is not upto date. check floats and other things thoroughly"
        while(keep_evicting_from_this_file(victim_inode) && (size_claimed_kb < sz_to_claim_kb));
#else
        /*
         * This is only placed here to remove the compilation errors
         * when EVICTION is not enabled.
         */
        while(true);
#endif
        /**
         * Here for EVICTION_FREQ we check keep_evicting_from_this_file but not for EVICTION_LRU
         * This is because keep_evicting_from_this_file compares the minimum keys from the top file in gheap
         * This would lead to a change in file even though there are low utilization file portions left.
         * We have observed with LRU that this decreases performance.
         * One thing that is not clear; with while(false) and while(size_claimed_kb < sz_to_claim_kb) LRU
         * works similarly. Check if that is the case with different system settings. For now I am keeping the latter
         * since that seems to be more complete.
         */



#ifdef EVICTOR_OUTSIDE_LOCK
        evict_file_portion(victim_inode, fd, (portion_nr*portion_sz), portion_sz);
#endif //EVICTOR_OUTSIDE_LOCK



#ifdef DBG_ONLY_DOWHILE
        victim_inode->unlinked_lock.unlock();
        goto exit_evict_portions;
#endif //DBG_ONLY_DOWHILE

        victim_inode->file_heap_lock.lock();
        last_victim_portion = heap_read_min(victim_inode->file_heap);
        last_victim_portion_key = last_victim_portion->key;

        // if (last_victim_portion_key == ULONG_MAX) {
        //         auto file_heap_keys = heap_get_all_keys(victim_inode->file_heap);
        //         unsigned long long int ulong_count = 0;
        //         for (auto hkey: file_heap_keys) {
        //                 if (hkey == ULONG_MAX) {ulong_count+=1;}
        //         }
        //         printf("ULONG_MAX min condition reached in file_heap. ulong_count = %llu / %lu\n", ulong_count, victim_inode->file_heap->size);
        // }

        g_heap_lock.lock();
        // SPEEDYIO_PRINTF("%s:INFO global_heap_update_key for {ino:%lu, dev:%lu}, heap_id:%d\n", "SPEEDYIO_INFOCO_0021 %lu %lu %d\n", victim_inode->ino, victim_inode->dev_id, victim_inode->heap_id);
        if(!victim_inode->is_deleted()){
                heap_update_key(g_file_heap, victim_inode->heap_id, last_victim_portion_key);
        }else{
                SPEEDYIO_PRINTF("%s:WARNING victim_inode {ino:%lu, dev:%lu} removed from gheap in the middle of eviction\n", "SPEEDYIO_WARNCO_0008 %lu %lu\n", victim_inode->ino, victim_inode->dev_id);
        }
        g_heap_lock.unlock();
        victim_inode->file_heap_lock.unlock();


#ifdef ENABLE_UINODE_LOCK
        victim_inode->uinode_lock.unlock();
#endif //ENABLE_UINODE_LOCK

        /*Releasing unlinked_lock which was held by get_victim_uinode*/
        victim_inode->unlinked_lock.unlock();

exit_evict_portions:
#ifdef BELADY_PROOF
        //check if eviction_event is populated; if not, then free it.
        if(!eviction_event || eviction_event->ino == -1){
                free(eviction_event);
                eviction_event = nullptr;
        }
        return eviction_event;
#else
        return size_claimed_kb;
#endif
}


/**
 * This function is called to evict in full file granularity.
 * used when GLOBAL_LRU enabled
 */
#ifdef BELADY_PROOF
struct mock_eviction_item *evict_file(void)
#else
int evict_file(void)
#endif
{
        struct inode *victim_uinode;
        int ret = 1; //0 means no victim found, 1 means victim found and evicted

#ifdef BELADY_PROOF
        struct mock_eviction_item *eviction_event = nullptr;
        eviction_event = (struct mock_eviction_item*)malloc(sizeof(struct mock_eviction_item));
#endif //BELADY_PROOF

        victim_uinode = get_victim_uinode();

        if(!victim_uinode){
                SPEEDYIO_FPRINTF("%s:ERROR no victim found\n", "SPEEDYIO_ERRCO_0192\n");
                ret = 0;
                goto exit_evict_file;
        }
        debug_printf("%s: victim {ino:%lu, dev:%lu}\n", __func__, victim_uinode->ino, victim_uinode->dev_id);

#ifdef BELADY_PROOF
        eviction_event->ino = victim_uinode->ino;
        eviction_event->dev_id = victim_uinode->dev_id;
        eviction_event->offset = 0;
        eviction_event->size = 0;
#else
        evict_full_file(victim_uinode);
#endif //BELADY_PROOF

        /*This lock was held by get_victim_uinode*/
        victim_uinode->unlinked_lock.unlock();

exit_evict_file:
#ifdef BELADY_PROOF
        if(ret == 0 && eviction_event){
                free(eviction_event);
                eviction_event = nullptr;
        }
        return eviction_event;
#else
        return ret;
#endif //BELADY_PROOF
}


void *concurrent_eviction(void *arg){

        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

        int sleep_milliseconds = SYSTEM_UTIL_SLEEP_MS;
        struct timespec ts;

        long free_mem_kb;
        long min_mem_reqd_kb;

        unsigned long long int ctr = 0;

        // printf("%s: STARTED XXXXXXXXXXXXX\n", __func__);

        //SYSTEM monitor bg thread has not been started
        while(getFreeMemoryKB() <= 0){}

        while(true){

try_again:
                /*stops here if thread killed*/
                pthread_testcancel();

                /*pauses here if stop_speedyio is triggered*/
                evictor_is_paused();

                /*
                printf("%s: Free Mem:%ld KB, minreqdmem:%ld KB\n", __func__,
                                getFreeMemoryKB(), getMinMemoryRequiredKB());
                */
                free_mem_kb = getFreeMemoryKB();
                min_mem_reqd_kb = getMinMemoryRequiredKB() + EVICTION_LOW_MEM_WATERMARK;

                if(free_mem_kb < min_mem_reqd_kb){

#ifdef DBG_EVICTOR_ONLYSLEEP
                        goto evictor_sleep;
#endif //DBG_EVICTOR_ONLYSLEEP


#if defined(ENABLE_PVT_HEAP)
        // #ifdef EVICTOR_OUTSIDE_LOCK
        //                 new_evict_portions(min_mem_reqd_kb - free_mem_kb);
        // #else
                        evict_portions(min_mem_reqd_kb - free_mem_kb);
        // #endif //EVICTOR_OUTSIDE_LOCK

                        /*
                        if(evicted_sz < min_mem_reqd_kb - free_mem_kb){
                                goto try_again;
                        }
                        */
#else //One global heap
                        if(evict_file() == 0){
                                goto evictor_sleep;
                        }
#endif //ENABLE_PVT_HEAP
                }

evictor_sleep:
                if (ctr % EVICTOR_SLEEP_FREQ == 0) {
                        ts.tv_sec = sleep_milliseconds / 1000;              // Convert milliseconds to seconds
                        ts.tv_nsec = (sleep_milliseconds % 1000) * 1000000L;  // Convert remaining milliseconds to nanoseconds
                        if (nanosleep(&ts, NULL) == -1) {
                                SPEEDYIO_FPRINTF("%s:ERROR nanosleep failed\n", "SPEEDYIO_ERRCO_0193\n");
                        }
                }
                ctr++;
        }
exit:
        return nullptr;
}

//TESTING FUNCTIONS

/**
 * This removes all elements from the gheap while printing them.
 * The gheap is empty after calling this.
 */
void print_full_gheap(){

        struct HeapItem *item = nullptr;
        unsigned long long int key;
        struct inode *uinode = nullptr;
        struct lru_entry *entry = nullptr;

        g_heap_lock.lock();

        SPEEDYIO_PRINTF("%s:INFO START ################################################################# heapsize:%lu\n", "SPEEDYIO_INFOCO_0022 %lu\n", g_file_heap->size);

        while(g_file_heap->size > 0){
                item = heap_extract_min(g_file_heap);
                if(!item){
                        SPEEDYIO_FPRINTF("%s:ERROR heapitem is nullptr\n", "SPEEDYIO_ERRCO_0194\n");
                        exit(EXIT_FAILURE);
                }

                key = item->key;

#if defined(ENABLE_ONE_LRU) && defined(BELADY_PROOF)
                entry = (struct lru_entry*)item->dataptr;
                if(!entry){
                        SPEEDYIO_FPRINTF("%s:ERROR entry is nullptr\n", "SPEEDYIO_ERRCO_0195\n");
                        exit(EXIT_FAILURE);
                }
                uinode = entry->uinode;
                if(!uinode){
                        SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr\n", "SPEEDYIO_ERRCO_0196\n");
                        exit(EXIT_FAILURE);
                }
                SPEEDYIO_PRINTF("%s:INFO {ino:%lu, dev:%lu}, key:%llu\n", "SPEEDYIO_INFOCO_0023 %lu %lu %llu\n", uinode->ino, uinode->dev_id, key);
#else
                uinode = (struct inode*)item->dataptr;
                if(!uinode){
                        SPEEDYIO_FPRINTF("%s:ERROR uinode is nullptr\n", "SPEEDYIO_ERRCO_0197\n");
                        exit(EXIT_FAILURE);
                }
                SPEEDYIO_PRINTF("%s:INFO {ino:%lu, dev:%lu}, key:%llu\n", "SPEEDYIO_INFOCO_0024 %lu %lu %llu\n", uinode->ino, uinode->dev_id, key);
#endif
        }

        SPEEDYIO_PRINTF("%s:INFO DONE ################################################################# heapsize:%lu\n", "SPEEDYIO_INFOCO_0025 %lu\n", g_file_heap->size);

        g_heap_lock.unlock();
        return;
}
