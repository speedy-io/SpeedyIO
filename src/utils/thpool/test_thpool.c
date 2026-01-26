#include <stdio.h>
#include <unistd.h>
#include <time.h>

#include "thpool.h"
#include "./simple/thpool-simple.h"

//void print_num(int *num){
void print_num(void *num){
        //int *a = (int *)num;
        //sleep(*(int *)num);
        //printf("NUM=%d\n", *a);
        return;
}

long long get_time_in_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}


int main(){
        threadpool thpool;
        thpool = thpool_init(4);



        threadpool_t *pool;
        pool = threadpool_create(4, 1000000, 0);

        int i;
        int a = 12234;

#if 0
        long long start_time_put = get_time_in_ns();

        for(i=0; i<1000000; i++){
                thpool_add_work(thpool, (void*)print_num, (void*)a);
        }
        long long end_time_put = get_time_in_ns();

        printf("Total time taken to add threads to pool = %lld ns\n", 
                        end_time_put - start_time_put);

#endif

        long long start_time_put = get_time_in_ns();

        for(i=0; i<1000000; i++){
                threadpool_add(pool, print_num, (void*)&a, 0);
        }
        long long end_time_put = get_time_in_ns();

        printf("Total time taken to add threads to pool = %lld ns\n", 
                        end_time_put - start_time_put);

        return 0;
}

