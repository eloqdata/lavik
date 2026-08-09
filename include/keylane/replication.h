#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "celer/rpc/rpc.h"
#include "celer/runtime/task.h"

namespace celer {
class Service;
class Worker;
}  // namespace celer

namespace keylane::storage {
class StorageEngine;
}  // namespace keylane::storage

namespace keylane {

struct ReplicationOptions {
  std::uint16_t listen_port = 0;
  std::string target_ip;
  std::uint16_t target_port = 0;
  bool replica_read_only = false;
};

class ReplicationManager {
 public:
  ReplicationManager(storage::StorageEngine* storage,
                     ReplicationOptions options);
  ReplicationManager(const ReplicationManager&) = delete;
  ReplicationManager& operator=(const ReplicationManager&) = delete;
  ~ReplicationManager();

  celer::Service* service() noexcept;
  void StorageReady(celer::Worker& worker);
  bool replica_read_only() const noexcept { return options_.replica_read_only; }

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ReplicationOptions options_;
};

}  // namespace keylane
