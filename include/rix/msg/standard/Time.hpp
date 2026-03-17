#pragma once

#include <cstdint>
#include <vector>
#include <array>
#include <map>
#include <string>
#include <cstring>

#include "rix/msg/serialization.hpp"
#include "rix/msg/message.hpp"

namespace rix {
namespace msg {
namespace standard {

class Time : public Message {
  public:
    int32_t sec{};
    int32_t nsec{};

    Time() = default;
    Time(const Time &other) = default;
    ~Time() = default;

    size_t size() const override {
        using namespace detail;
        size_t size = 0;
        size += size_number(sec);
        size += size_number(nsec);
        return size;
    }

    std::array<uint64_t, 2> hash() const override {
        return {0xe80974cc496bf99dULL, 0xf7f4f2296e012a33ULL};
    }

    void serialize(uint8_t *dst, size_t &offset) const override {
        using namespace detail;
        serialize_number(dst, offset, sec);
        serialize_number(dst, offset, nsec);
    }

    bool deserialize(const uint8_t *src, size_t size, size_t &offset) override {
        using namespace detail;
        if (!deserialize_number(sec, src, size, offset)) { return false; };
        if (!deserialize_number(nsec, src, size, offset)) { return false; };
        return true;
    }
};

} // namespace standard
} // namespace msg
} // namespace rix