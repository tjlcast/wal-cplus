// include/utils.h
#ifndef UTILS_H
#define UTILS_H

#include <vector>
#include <cstdint>
#include <string>

std::string base64_encode(const uint8_t *buf, size_t bufLen, bool url_safe = false);
std::vector<uint8_t> base64_decode(const std::string &encoded_string);

size_t ReadVarint(const uint8_t *buf, size_t bufLen, uint64_t *value);
void WriteVarint(uint64_t value, std::vector<uint8_t> &out);

#endif // UTILS_H