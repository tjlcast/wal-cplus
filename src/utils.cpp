#include "wal.h"
#include <vector>
#include <cstdint>
#include <stdexcept>

// Base64 encoding/decoding functions
static const std::string base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

std::string base64_encode(const uint8_t *buf, size_t bufLen, bool url_safe)
{
    std::string ret;
    int i = 0;
    int j = 0;
    uint8_t char_array_3[3];
    uint8_t char_array_4[4];

    while (bufLen--)
    {
        char_array_3[i++] = *(buf++);
        if (i == 3)
        {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                              ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                              ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for (i = 0; i < 4; i++)
            {
                if (url_safe)
                {
                    if (base64_chars[char_array_4[i]] == '+')
                    {
                        ret += '-';
                    }
                    else if (base64_chars[char_array_4[i]] == '/')
                    {
                        ret += '_';
                    }
                    else
                    {
                        ret += base64_chars[char_array_4[i]];
                    }
                }
                else
                {
                    ret += base64_chars[char_array_4[i]];
                }
            }
            i = 0;
        }
    }

    if (i)
    {
        for (j = i; j < 3; j++)
        {
            char_array_3[j] = '\0';
        }

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                          ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                          ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for (j = 0; j < i + 1; j++)
        {
            if (url_safe)
            {
                if (base64_chars[char_array_4[j]] == '+')
                {
                    ret += '-';
                }
                else if (base64_chars[char_array_4[j]] == '/')
                {
                    ret += '_';
                }
                else
                {
                    ret += base64_chars[char_array_4[j]];
                }
            }
            else
            {
                ret += base64_chars[char_array_4[j]];
            }
        }

        while (i++ < 3)
        {
            if (url_safe)
            {
                ret += '=';
            }
            else
            {
                ret += '=';
            }
        }
    }

    return ret;
}

std::vector<uint8_t> base64_decode(const std::string &encoded_string)
{
    size_t in_len = encoded_string.size();
    int i = 0;
    int j = 0;
    int in_ = 0;
    uint8_t char_array_4[4], char_array_3[3];
    std::vector<uint8_t> ret;

    auto is_base64 = [](unsigned char c)
    {
        return (isalnum(c) || (c == '+') || (c == '/') ||
                (c == '-') || (c == '_'));
    };

    while (in_len-- && (encoded_string[in_] != '=') &&
           is_base64(encoded_string[in_]))
    {
        char_array_4[i++] = encoded_string[in_];
        in_++;
        if (i == 4)
        {
            for (i = 0; i < 4; i++)
            {
                size_t pos = base64_chars.find(char_array_4[i]);
                if (pos == std::string::npos)
                {
                    // Handle URL-safe characters
                    if (char_array_4[i] == '-')
                    {
                        char_array_4[i] = '+';
                    }
                    else if (char_array_4[i] == '_')
                    {
                        char_array_4[i] = '/';
                    }
                    else
                    {
                        throw std::runtime_error("invalid base64 character");
                    }
                    pos = base64_chars.find(char_array_4[i]);
                }
                char_array_4[i] = static_cast<uint8_t>(pos);
            }

            char_array_3[0] = (char_array_4[0] << 2) +
                              ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) +
                              ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

            for (i = 0; i < 3; i++)
            {
                ret.push_back(char_array_3[i]);
            }
            i = 0;
        }
    }

    if (i)
    {
        for (j = i; j < 4; j++)
        {
            char_array_4[j] = 0;
        }

        for (j = 0; j < 4; j++)
        {
            size_t pos = base64_chars.find(char_array_4[j]);
            if (pos == std::string::npos)
            {
                // Handle URL-safe characters
                if (char_array_4[j] == '-')
                {
                    char_array_4[j] = '+';
                }
                else if (char_array_4[j] == '_')
                {
                    char_array_4[j] = '/';
                }
                else
                {
                    throw std::runtime_error("invalid base64 character");
                }
                pos = base64_chars.find(char_array_4[j]);
            }
            char_array_4[j] = static_cast<uint8_t>(pos);
        }

        char_array_3[0] = (char_array_4[0] << 2) +
                          ((char_array_4[1] & 0x30) >> 4);
        char_array_3[1] = ((char_array_4[1] & 0xf) << 4) +
                          ((char_array_4[2] & 0x3c) >> 2);
        char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

        for (j = 0; j < i - 1; j++)
        {
            ret.push_back(char_array_3[j]);
        }
    }

    return ret;
}

// Varint encoding/decoding functions
size_t ReadVarint(const uint8_t *buf, size_t bufLen, uint64_t *value)
{
    *value = 0;
    size_t i = 0;
    int shift = 0;

    for (; i < bufLen && i < 10; i++)
    {
        *value |= (uint64_t)(buf[i] & 0x7F) << shift;
        if ((buf[i] & 0x80) == 0)
        {
            return i + 1;
        }
        shift += 7;
    }

    return 0;
}

void WriteVarint(uint64_t value, std::vector<uint8_t> &out)
{
    while (value >= 0x80)
    {
        out.push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}