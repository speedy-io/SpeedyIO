#ifndef _PER_THREAD_DS_HPP
#define _PER_THREAD_DS_HPP
/*
 * Per-Thread constructors can be made using
 * constructors for thread local objects
 * this can also be used for per thread monitoring
 */
class per_thread_data{
    public:
        int touchme; //just touch this variable if you want to call the constructor

        std::unordered_map<int, std::weak_ptr<struct perfd_struct>> *fd_map = nullptr;

#ifdef PRINT_READ_EVENTS
        int read_events_fd = -1;
#endif // PRINT_READ_EVENTS

#ifdef PRINT_WRITE_EVENTS
        int write_events_fd = -1;
#endif // PRINT_WRITE_EVENTS

        /*
         * XXX: Since while writing an sst file, a single thread opens
         * and writes - we can store fd->struct fd here to reduce lock contention
         */

        //constructor
        per_thread_data(){
                int pid, tid;
                debug_printf("Constructor for thread:%ld\n", gettid());

                init_g_fd_map(); //check the explanation at its definition.

#ifdef PER_THREAD_DS
                fd_map = new std::unordered_map<int, std::weak_ptr<struct perfd_struct>>;
#endif //PER_THREAD_DS

#ifdef PRINT_READ_EVENTS
                pid = getpid(), tid = gettid();
                std::string read_events_filename = "read_events_pid_" + std::to_string(pid) + "_tid_" + std::to_string(tid) + ".replay";
                read_events_fd = open_event_logger_file(read_events_filename);
#endif // PRINT_READ_EVENTS

#ifdef PRINT_WRITE_EVENTS
                pid = getpid(), tid = gettid();
                std::string write_events_filename = "write_events_pid_" + std::to_string(pid) + "_tid_" + std::to_string(tid) + ".replay";
                write_events_fd = open_event_logger_file(write_events_filename);
#endif // PRINT_WRITE_EVENTS

        }

        ~per_thread_data(){
                if(fd_map){
                        delete fd_map;
                }
        }
};
#endif
