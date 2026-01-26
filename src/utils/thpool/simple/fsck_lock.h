#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <pthread.h>
#include <sched.h>

#define _ENABLE_PTHREAD_LOCK

struct fsck_lock {
	int locked;
	long tid_holder;
};

#ifdef _ENABLE_PTHREAD_LOCK
typedef pthread_mutex_t  fsck_lock_t;
#else
typedef spinlock fsck_lock_t;
#endif

int fsck_lock_init(fsck_lock_t *mutex, pthread_mutexattr_t *attr);
int fsck_lock(fsck_lock_t *mutex);
int fsck_unlock(fsck_lock_t *mutex);
