/**
 * This program just checks if the hashtable can insert more than
 * MAX_ENTRIES which is passed to create_hashtable.
 *
 * Note: that the create_hashtable takes in minimum entries to allocate instead
 * of a max nr_entries.
 * So MAX_ENTRIES worth of space is allocated at creation time.
 * When nr_elements go beyond MAX_ENTRIES * max_load_factor(0.65 in hashtable.c)
 * the size of hashmap is doubled using hashtable_expand
 *
 * The theoretical max nr_elements is bounded by the memory in the system and
 * the biggest prime in the array primes in hashtable.c
 */

/*g++ test_hashtable.cpp hashtable.c -o test_hashtable*/
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "hashtable.h"

struct hashtable *i_map;

#define MAX_ENTRIES 1000000

struct key {
    int k;
};

struct value {
    int value;
};

/*Required initialization for hashtable*/
DEFINE_HASHTABLE_INSERT(insert_some, struct key, struct value);
DEFINE_HASHTABLE_SEARCH(search_some, struct key, struct value);
DEFINE_HASHTABLE_REMOVE(remove_some, struct key, struct value);

static unsigned int hashfromkey(void *ky){
    struct key *k = (struct key *)ky;
    return (((k->k << 17) | (k->k >> 15)));
}

static int equalkeys(void *k1, void *k2){
    struct key *key1 = (struct key *)k1;
    struct key *key2 = (struct key *)k2;
    return (key1->k == key2->k);
}

struct hashtable *init_inode_map(void){
    return create_hashtable(MAX_ENTRIES, hashfromkey, equalkeys);
}

int insert_to_hashtable(int key, int val){

    int ret = false;
    struct key *k = (struct key*)malloc(sizeof(struct key));
    struct value *v = (struct value *)malloc(sizeof(struct value));

    if(!key || !v){
            fprintf(stderr, "%s:%d unable to allocate memory\n", __func__, __LINE__);
            free(k);
            free(v);
            goto insert_to_hashtable_exit;
    }
    k->k = key;
    v->value = val;

    if(!insert_some(i_map, k, v)){
            goto insert_to_hashtable_exit;
    }

    ret = true;

insert_to_hashtable_exit:
    return ret;
}

int get_from_hashtable(int k){

    struct value *val= NULL;
    struct key key;
    key.k = k;
    
    val = search_some(i_map, &key);
    if(val){
        return val->value;
    }
    else{
        return -120;
    }
}

int main(){
    i_map = init_inode_map();

    int inserted = 0;
    int i;

    //Enter 2x MAX_ENTRIES in the hashtable
    for (i = 1; i <= MAX_ENTRIES * 10; i++) {
        if (insert_to_hashtable(i, i)) {
            inserted++;
        } else {
            printf("Insertion failed for key: %d\n", i);
        }
    }

    printf("Total successfully inserted: %d\n", inserted);

    // Verify all the values
    for (i = 1; i <= MAX_ENTRIES * 10; i++) {
        int val = get_from_hashtable(i);
        if (val != -120){
            if(i != val){
                printf("Key:%d = value:%d\n", i, val);
            }
        }else{
            printf("key:%d has no entry in hashtable\n", i);
        }
    }

    return 0;
}