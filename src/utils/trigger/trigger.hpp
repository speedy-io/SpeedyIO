#ifndef _TRIGGER_HPP
#define _TRIGGER_HPP

#include <stdint.h>
#include <sys/types.h>

/**
 * ADD COMMENTS HERE ABOUT IT
 */

struct trigger{
        uint64_t step;   // fire every 'step' counts
        uint64_t last;   // counter value at last fire
        uint64_t now;    // <-- update this elsewhere in your code

        trigger(){
                step = 0;
                last = 0;
                now = 0;
        }
};

bool trigger_check(struct trigger *t);
bool sanitize_struct_trigger(struct trigger *t);

#endif //_TRIGGER_HPP