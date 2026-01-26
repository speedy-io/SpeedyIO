
#include "trigger.hpp"

/**
 * returns true if counter (now) has incremented atleast 'step' counts
 * else returns false
 */
bool trigger_check(struct trigger *t) {
        // wrap-safe distance in modulo-2^64 space
        uint64_t delta = (uint64_t)(t->now - t->last);
        if (delta >= t->step) {
                t->last = t->now;
                return true;
        }
        return false;
}

bool sanitize_struct_trigger(struct trigger *t){
    bool ret = false;

    if(!t){
        goto exit_sanitize;
    }

    t->step = 0;
    t->last = 0;
    t->now = 0;

exit_sanitize:
    return ret;
}