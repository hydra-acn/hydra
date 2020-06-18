#include "fakerand.h"

void activate_fakerand(unsigned int seed) {
    RAND_set_rand_method(RAND_stdlib());
    RAND_seed(&seed, sizeof(seed));
}
