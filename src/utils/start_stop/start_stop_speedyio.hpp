#ifndef _START_STOP_SPEEDYIO_HPP
#define _START_STOP_SPEEDYIO_HPP

#include <pthread.h>
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>

extern pthread_mutex_t evictor_pause_lock;
extern pthread_cond_t evictor_pause_cond;
extern bool evictor_paused;

void evictor_is_paused();
void resume_speedyio();
void stop_speedyio();
void* start_stop_trigger_checking(void* arg);

#endif //_START_STOP_SPEEDYIO_HPP
