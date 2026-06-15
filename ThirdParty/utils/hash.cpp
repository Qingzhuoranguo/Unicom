#include "hash.h"


namespace hash {

uint32_t hash32(const std::string& s)
{
    uint32_t hash = 2166136261u; // FNV offset basis
    uint32_t prime = 16777619u;  // FNV prime

    for (unsigned char c : s) {
        hash ^= c;
        hash *= prime;
    }
    return hash;
}

uint64_t hash64(const std::string& s)
{
    uint64_t hash = 1469598103934665603ull;      // FNV offset basis
    uint64_t prime = 1099511628211ull;     // FNV prime

    for (unsigned char c : s) {
        hash ^= c;
        hash *= prime;
    }
    return hash;
}

}