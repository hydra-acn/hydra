// wrapping stdlib's rand() for usage as openssl RNG (for testing only!!!)
// source: https://stackoverflow.com/questions/7437177/force-openssls-rngs-to-return-a-repeatable-byte-sequence
#include <assert.h>
#include <openssl/rand.h>
#include <stdlib.h>

// These don't need to do anything if you don't have anything for them to do.
static void stdlib_rand_cleanup() { }
static int stdlib_rand_add(const void* buf, int num, double add_entropy) { return 0; }
static int stdlib_rand_status() { return 1; }

// Seed the RNG.  srand() takes an unsigned int, so we just use the first
// sizeof(unsigned int) bytes in the buffer to seed the RNG.
static int stdlib_rand_seed(const void* buf, int num)
{
    assert(num >= sizeof(unsigned int));
    srand(*((unsigned int*)buf));
    return 0;
}

// Fill the buffer with random bytes.  For each byte in the buffer, we generate
// a random number and clamp it to the range of a byte, 0-255.
static int stdlib_rand_bytes(unsigned char* buf, int num)
{
    for (int index = 0; index < num; ++index) {
        buf[index] = rand() % 256;
    }
    return 1;
}

// Create the table that will link OpenSSL's rand API to our functions.
RAND_METHOD stdlib_rand_meth = {
    stdlib_rand_seed,
    stdlib_rand_bytes,
    stdlib_rand_cleanup,
    stdlib_rand_add,
    stdlib_rand_bytes,
    stdlib_rand_status
};

// This is a public-scope accessor method for our table.
RAND_METHOD* RAND_stdlib() { return &stdlib_rand_meth; }

// set the fake rng in openssl and seed it accordingly
void activate_fakerand(unsigned int seed);
