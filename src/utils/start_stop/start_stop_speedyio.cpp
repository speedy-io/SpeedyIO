#include <stdio.h>
#include <unistd.h>

#include "start_stop_speedyio.hpp"
#include "utils/util.hpp"
#include "utils/parse_config/config.hpp"

pthread_mutex_t evictor_pause_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t evictor_pause_cond = PTHREAD_COND_INITIALIZER;
bool evictor_paused = false;

/**
 * Does not return while evictor_paused remains true
 */
void evictor_is_paused(){
        pthread_mutex_lock(&evictor_pause_lock);
        while(evictor_paused){
            // Wait until unpaused
            pthread_cond_wait(&evictor_pause_cond, &evictor_pause_lock);
        }
        pthread_mutex_unlock(&evictor_pause_lock);
}

/*
 * Pauses SpeedyIO
 */
void stop_speedyio(){
        printf("Stopped SpeedyIO\n");
        pthread_mutex_lock(&evictor_pause_lock);
        evictor_paused = true;
        pthread_mutex_unlock(&evictor_pause_lock);
}

/*
 * resumes SpeedyIO
 */
void resume_speedyio(){
        printf("Resumed SpeedyIO\n");
        pthread_mutex_lock(&evictor_pause_lock);
        evictor_paused = false;
        pthread_cond_signal(&evictor_pause_cond);
        pthread_mutex_unlock(&evictor_pause_lock);
}


/**
 * This checks the triggers that can start/stop speedyio
 * For now, it is only looking the existance of START_STOP_TRIGGER_FILE
 * in the filesystem.
 * TODO: trigger using instructions from server
 */
void* start_stop_trigger_checking(void* arg){
        bool file_was_present = false;
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

        while(true){
                /*stops here if thread killed*/
                pthread_testcancel();

                //Add any trigger here
                // if(access(cfg->start_stop_path, F_OK) == 0){
                if(access(START_STOP_TRIGGER_FILE, F_OK) == 0){
                        if(!file_was_present){
                                file_was_present = true;
                                stop_speedyio();
                        }
                }
                else{
                        if(file_was_present){
                                file_was_present = false;
                                resume_speedyio();
                        }
                }
                sleep(START_STOP_SLEEP);
        }
        return NULL;
}
