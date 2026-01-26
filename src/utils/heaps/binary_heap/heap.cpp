// heap.cpp for binary heap

#include "heap.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <algorithm>    // for std::swap
#include <limits>       // for std::numeric_limits
#include <stdexcept>    // For std::runtime_error

#include "utils/util.hpp"

//------------------------------------------------------------------
// Helper: swap items in 'storage' and fix 'id2index' map
//------------------------------------------------------------------
static inline void swap_items(std::vector<HeapItem>& storage,
                              std::unordered_map<int, std::size_t>& id2index,
                              std::size_t i, std::size_t j)
{
    std::swap(storage[i], storage[j]);
    // fix the id2index mapping
    id2index[storage[i].id] = i;
    id2index[storage[j].id] = j;
}

//------------------------------------------------------------------
// Helper: bubble_up
//------------------------------------------------------------------
static void bubble_up(Heap* H, std::size_t idx)
{
    while (idx > 0) {
        std::size_t parent = (idx - 1) / 2;
        if (H->storage[idx].key >= H->storage[parent].key) {
            break; // min-heap property satisfied
        }
        // else swap
        swap_items(H->storage, H->id2index, idx, parent);
        idx = parent;
    }
}

//------------------------------------------------------------------
// Helper: bubble_down
//------------------------------------------------------------------
static void bubble_down(Heap* H, std::size_t idx)
{
    for (;;) {
        std::size_t left  = 2 * idx + 1;
        std::size_t right = 2 * idx + 2;
        std::size_t smallest = idx;

        if (left < H->size && H->storage[left].key < H->storage[smallest].key) {
            smallest = left;
        }
        if (right < H->size && H->storage[right].key < H->storage[smallest].key) {
            smallest = right;
        }
        if (smallest == idx) {
            break; // no change
        }
        swap_items(H->storage, H->id2index, idx, smallest);
        idx = smallest;
    }
}

//------------------------------------------------------------------
// heap_init
//------------------------------------------------------------------
Heap* heap_init(std::size_t capacity, const char* heap_name)
{
    if (std::strlen(heap_name) >= Heap::NAME_SIZE) {
        throw std::invalid_argument("Heap name is too long. Maximum allowed length is " +
                                    std::to_string(Heap::NAME_SIZE - 1) + " characters.");
    }
    Heap* H = new Heap();
    H->capacity = capacity;
    H->size = 0;
    H->next_id = 0;
    // storage is empty initially, reserve capacity
    /**
     * since storage is a vector the default behaviour in this code
     * was to reserve the capacity. This increases the memory usage
     * uselessly for whitelisted files that are extremely small or
     * transient (will get created and destroyed quickly).
     *
     * Not reserving the capacity doesnt change the correctness of the
     * heap. Hence to reduce memory usage by the lib, we disable reserve.
     */
    //H->storage.reserve(capacity);

    // Copy the name into the fixed-size array safely.
    std::strncpy(H->heap_name, heap_name, Heap::NAME_SIZE - 1);
    H->heap_name[Heap::NAME_SIZE - 1] = '\0'; // Ensure null termination

    return H;
}

//------------------------------------------------------------------
// heap_destroy
//------------------------------------------------------------------
void heap_destroy(Heap* H)
{
    if (!H) return;
    delete H;
}

//------------------------------------------------------------------
// heap_clear
//------------------------------------------------------------------
void heap_clear(Heap *H)
{
    if (!H) return;

    H->storage.clear();      // Remove all HeapItem elements.
    H->id2index.clear();     // Clear the mapping from id to index.
    H->size = 0;             // Set current size to zero.
    H->next_id = 0;          // Reset next_id (or to its initial value).
}

//------------------------------------------------------------------
// heap_insert
//------------------------------------------------------------------
int heap_insert(Heap* H, unsigned long long int key, void* dataptr)
{
    if (!H) {
        SPEEDYIO_FPRINTF("%s:ERROR H==NULL, insert attempted on a null heap\n", "SPEEDYIO_ERRCO_0198\n");
        KILLME();
    }
    if (H->size >= H->capacity) {
        SPEEDYIO_FPRINTF("%s:ERROR capacity exceeded\n", "SPEEDYIO_ERRCO_0199\n");
        KILLME();
    }

    // 1) create the new item
    HeapItem item;
    item.key = key;
    item.dataptr = dataptr;
    item.id = H->next_id;
    H->next_id += 1;
    // 2) put it at the end
    H->storage.push_back(item);
    H->size++;

    // 3) record in id2index
    std::size_t idx = H->size - 1;
    H->id2index[item.id] = idx;

    // 4) bubble up
    bubble_up(H, idx);

    return item.id;
}

//------------------------------------------------------------------
// heap_update_key
//------------------------------------------------------------------
void heap_update_key(Heap* H, int id, unsigned long long int newKey)
{
    if (!H) {
        SPEEDYIO_FPRINTF("%s:ERROR H==NULL, called on a null heap\n", "SPEEDYIO_ERRCO_0200\n");
    }
    auto it = H->id2index.find(id);
    if (it == H->id2index.end()) {
        // no such item
        SPEEDYIO_FPRINTF("%s:ERROR %s invalid id=%d\n", "SPEEDYIO_ERRCO_0201 %s %d\n", H->heap_name, id);
        KILLME();
    }
    std::size_t idx = it->second;

    unsigned long long int oldKey = H->storage[idx].key;
    H->storage[idx].key = newKey;

    // If newKey < oldKey, bubble up
    if (newKey < oldKey) {
        bubble_up(H, idx);
    }
    // If newKey > oldKey, bubble down
    else if (newKey > oldKey) {
        bubble_down(H, idx);
    }
    // else equal => do nothing
}

//------------------------------------------------------------------
// heap_delete_key_by_id
//------------------------------------------------------------------
void heap_delete_key_by_id(Heap* H, int id)
{
    if (!H) {
        SPEEDYIO_FPRINTF("%s:ERROR H==NULL, delete attempted on a null heap\n", "SPEEDYIO_ERRCO_0202\n");
        KILLME();
    }
    if (H->size == 0) {
        SPEEDYIO_FPRINTF("%s:ERROR heap is empty\n", "SPEEDYIO_ERRCO_0203\n");
        KILLME();
    }

    // 1) Locate the item by ID
    auto it = H->id2index.find(id);
    if (it == H->id2index.end()) {
        SPEEDYIO_FPRINTF("%s:ERROR invalid id=%d\n", "SPEEDYIO_ERRCO_0204 %d\n", id);
        KILLME();
    }
    std::size_t idx = it->second;

    // 2) Swap the target item with the last element in the heap.
    std::size_t last = H->size - 1;
    if (idx != last) {
        swap_items(H->storage, H->id2index, idx, last);
    }

    // 3) Remove the last element (which is the target) and update the mapping.
    H->id2index.erase(H->storage[last].id);
    H->storage.pop_back();
    H->size--;

    // 4) Restore the heap invariant.
    // If we deleted an element that was not the last one,
    // the item at idx (swapped in from the end) may now violate the heap property.
    if (idx < H->size) {
        // Try bubbling up if the new key is less than its parent's key.
        if (idx > 0 && H->storage[idx].key < H->storage[(idx - 1) / 2].key) {
            bubble_up(H, idx);
        }
        // Otherwise, bubble down if necessary.
        else {
            bubble_down(H, idx);
        }
    }
}

//------------------------------------------------------------------
// heap_read_min
//------------------------------------------------------------------
HeapItem* heap_read_min(Heap* H)
{
    if (!H || H->size == 0) {
        return nullptr;
    }
    return &H->storage[0];
}

//------------------------------------------------------------------
// heap_extract_min
//------------------------------------------------------------------
HeapItem* heap_extract_min(Heap* H)
{
    if (!H || H->size == 0) {
        return nullptr;
    }
    // min item is at index 0
    HeapItem* ret = (HeapItem*)malloc(sizeof(HeapItem));
    // copy the data
    *ret = H->storage[0];

    // 1) swap with the last item
    std::size_t last = H->size - 1;
    swap_items(H->storage, H->id2index, 0, last);

    // 2) remove the last item from storage and map
    int removed_id = H->storage[last].id;
    H->id2index.erase(removed_id);
    H->storage.pop_back();
    H->size--;

    // 3) bubble down the new root
    if (H->size > 0) {
        bubble_down(H, 0);
    }
    return ret;
}

//------------------------------------------------------------------
// heap_get_key_by_id
//------------------------------------------------------------------
unsigned long long int heap_get_key_by_id(Heap* H, int id)
{
    if (!H || H->size == 0) {
        SPEEDYIO_FPRINTF("%s:ERROR H is NULL or heap is empty\n", "SPEEDYIO_ERRCO_0205\n");
        KILLME();
    }

    if (id < 0) {
        SPEEDYIO_FPRINTF("%s:ERROR Negative ID passed: %d\n", "SPEEDYIO_ERRCO_0206 %d\n", id);
        KILLME();
    }

    auto it = H->id2index.find(id);
    if (it == H->id2index.end() || it->second >= H->size) {
        SPEEDYIO_FPRINTF("%s:ERROR Invalid ID passed: %d\n", "SPEEDYIO_ERRCO_0207 %d\n", id);
        KILLME();
    }

    return H->storage[it->second].key;
}

//------------------------------------------------------------------
// heap_get_all_keys
//------------------------------------------------------------------
std::vector<unsigned long long int> heap_get_all_keys(Heap* H)
{
    if (!H) {
        SPEEDYIO_FPRINTF("%s:ERROR H==NULL, called on a null heap\n", "SPEEDYIO_ERRCO_0208\n");
        KILLME();
    }
    std::vector<unsigned long long int> keys;
    keys.reserve(H->size);
    for (const auto& item : H->storage) {
        keys.push_back(item.key);
    }
    return keys;
}

//------------------------------------------------------------------
// heap_get_all_dataptrs
//------------------------------------------------------------------
std::vector<void*> heap_get_all_dataptrs(Heap* H)
{
    if (!H) {
        SPEEDYIO_FPRINTF("%s:ERROR H==NULL, called on a null heap\n", "SPEEDYIO_ERRCO_0209\n");
        KILLME();
    }
    std::vector<void*> dataptrs;
    dataptrs.reserve(H->size);
    for (const auto& item : H->storage) {
        dataptrs.push_back(item.dataptr);
    }
    return dataptrs;
}
