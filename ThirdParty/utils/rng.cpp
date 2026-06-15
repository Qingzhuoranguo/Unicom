#include "hash.h"

#include <random>
#include <chrono>

namespace rng {

uint32_t RNG32() {
    static std::mt19937 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    return rng();
}

uint64_t RNG64() {
    static std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    return rng();
}

}