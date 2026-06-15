#pragma once
#include <string>
#include <cstdint>

namespace hash {

uint32_t hash32(const std::string& s);
uint64_t hash64(const std::string& s);



}