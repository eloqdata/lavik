// Counts allocation requests, NOT elapsed time or live-server allocator calls.
// Uses the production codec and reproduces its owning result/argument loops;
// storage I/O, retained admission and RESP encoding are deliberately excluded.
#include <cstdio>
#include <cstdlib>
#include <new>
#include <optional>
#include <string>
#include <vector>

#include "keylane/storage/detail/hash_codec.h"

static bool counting = false;
static std::size_t calls = 0, bytes = 0;
void* operator new(std::size_t size) {
  if (counting) { ++calls; bytes += size; }
  if (auto* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

int main() {
  using namespace keylane::storage;
  HashValue source;
  for (int i = 0; i < 10; ++i) {
    auto field = "field" + std::to_string(i);
    source.entries_.push_back({ComputeDigest(field), field, std::string(128, 'x')});
  }
  auto encoded = EncodeHashValue(source);
  if (!encoded.ok()) return 1;
  for (bool direct : {false, true}) {
    calls = bytes = 0;
    counting = true;
    std::vector<std::optional<std::string>> result;
    if (direct) {
      auto reader = HashValueReader::Open(*encoded);
      if (!reader.ok()) return 2;
      auto inspect = *reader;
      for (std::size_t i = 0; i < reader->size(); ++i)
        if (!inspect.Next().ok()) return 3;
      result.reserve(reader->size() * 2);
      for (std::size_t i = 0; i < reader->size(); ++i) {
        auto entry = reader->Next();
        if (!entry.ok()) return 4;
        result.emplace_back(std::in_place, entry->field_);
        result.emplace_back(std::in_place, entry->value_);
      }
    } else {
      auto decoded = DecodeHashValue(*encoded);
      if (!decoded.ok()) return 5;
      result.reserve(decoded->entries_.size() * 2);
      for (auto& entry : decoded->entries_) {
        result.emplace_back(std::move(entry.field_));
        result.emplace_back(std::move(entry.value_));
      }
    }
    counting = false;
    for (std::size_t i = 0; i < source.entries_.size(); ++i)
      if (*result[i*2] != source.entries_[i].field_ ||
          *result[i*2+1] != source.entries_[i].value_) return 6;
    std::printf("read mode=%s payload=%zu allocations=%zu requested_bytes=%zu\n",
                direct ? "direct" : "move", encoded->size(), calls, bytes);
  }
  for (bool reserve : {false, true}) {
    calls = bytes = 0;
    counting = true;
    std::vector<std::string_view> fields, values;
    if (reserve) { fields.reserve(10); values.reserve(10); }
    for (const auto& entry : source.entries_) {
      fields.push_back(entry.field_);
      values.push_back(entry.value_);
    }
    counting = false;
    if (fields.size() != 10 || values.size() != 10) return 7;
    std::printf("arguments reserve=%d allocations=%zu requested_bytes=%zu capacity=%zu/%zu\n",
                reserve, calls, bytes, fields.capacity(), values.capacity());
  }
}
