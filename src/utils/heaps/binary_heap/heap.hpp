// heap.hpp for a binary heap

#ifndef HEAP_HPP
#define HEAP_HPP

#include <vector>
#include <unordered_map>
#include <cstddef>

/**
 * The user data is stored in a simple struct:
 *    key      = the "priority" (we do a min-heap by key)
 *    dataptr  = arbitrary user pointer
 *    id       = a unique ID to identify this item
 *
 * We only need 'id' internally for update-key lookups, but
 * we store it so we can bubble up/down quickly.
 */
struct HeapItem {
    unsigned long long int       key;
    void*       dataptr;
    int         id;
};

/**
 * Our main heap structure:
 *    storage: the array-based binary heap
 *    id2index: maps each id to its current index in 'storage'
 *    capacity: maximum number of items we allow
 *    size: how many items are currently in the heap
 *    next_id: used to generate unique IDs for insert
 *
 * We do "min-heap" => top of 'storage' is the minimum key.
 */
struct Heap {
    std::vector<HeapItem>              storage;
    std::unordered_map<int, std::size_t> id2index;
    std::size_t capacity;
    std::size_t size;
    int next_id;
    static const std::size_t NAME_SIZE = 64;  // Maximum length for the name (including null terminator)
    char heap_name[NAME_SIZE];
};

/**
 * Create and initialize the heap with a given capacity.
 */
Heap* heap_init(std::size_t capacity, const char* heap_name);

/**
 * Destroy the heap, freeing all items (including their data).
 */
void heap_destroy(Heap* H);

/**
 * Clears all the contents of the heap.
 */
void heap_clear(Heap *H);

/**
 * Insert a new item into the heap with given 'key' and 'dataptr'.
 * The 'id' is automatically assigned internally (H->next_id++).
 * id value is returned
 * Note: keys start from 0.
 */
int heap_insert(Heap* H, unsigned long long int key, void* dataptr);

/**
 * Update the key of the item with a given 'id' to 'newKey'.
 *   - If newKey < oldKey, the item might bubble up.
 *   - If newKey > oldKey, the item might bubble down.
 * No return value.
 */
void heap_update_key(Heap* H, int id, unsigned long long int newKey);

/**
 * Remove the element with the given id
 */
void heap_delete_key_by_id(Heap* H, int id);

/**
 * Read (but do not remove) the minimum item in the heap.
 * Returns:
 *   - pointer to the HeapItem if the heap is non-empty
 *   - nullptr if the heap is empty
 */
HeapItem* heap_read_min(Heap* H);

/**
 * Extract the minimum item from the heap (remove and return it).
 * Returns:
 *   - pointer to a newly allocated HeapItem on success
 *       (the caller is responsible for freeing it)
 *   - nullptr if the heap is empty
 *
 * We return a *copy* allocated on the heap, because once removed,
 * it won't reside in 'storage' anymore.
 */
HeapItem* heap_extract_min(Heap* H);


/*
 * returns the key for element with given id
 */
unsigned long long int heap_get_key_by_id(Heap* H, int id);

/*
 * Returns a vector of all keys in the heap.
 */
std::vector<unsigned long long int> heap_get_all_keys(Heap* H);

/*
 * Returns a vector of all dataptrs in the heap.
 */
std::vector<void*> heap_get_all_dataptrs(Heap* H);

#endif // HEAP_HPP
