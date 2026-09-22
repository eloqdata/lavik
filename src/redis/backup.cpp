/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "backup.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/fault_injection.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/rdb.h"
#include "lavik/rdb_collection.h"
#include "lavik/resp.h"
#include "lua_eval.h"
#include "spdlog/spdlog.h"

namespace lavik {
namespace {

class RdbOutputQueue {
 public:
  explicit RdbOutputQueue(std::string target_path)
      : target_path_(std::move(target_path)), thread_([this] { Run(); }) {}

  ~RdbOutputQueue() {
    if (!done()) {
      RequestAbort(absl::CancelledError("RDB output abandoned"));
    }
    if (thread_.joinable()) thread_.join();
  }

  RdbOutputQueue(const RdbOutputQueue&) = delete;
  RdbOutputQueue& operator=(const RdbOutputQueue&) = delete;

  bool ready() const noexcept { return ready_.load(std::memory_order_acquire); }
  bool done() const noexcept { return done_.load(std::memory_order_acquire); }
  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

  bool TryBeginEntry(unsigned owner) {
    std::lock_guard lock(mutex_);
    if (finishing_ || failed_.load(std::memory_order_relaxed) ||
        entry_owner_ != UINT_MAX)
      return false;
    entry_owner_ = owner;
    return true;
  }

  void EndEntry(unsigned owner) {
    std::lock_guard lock(mutex_);
    assert(entry_owner_ == owner);
    entry_owner_ = UINT_MAX;
  }

  bool TryPush(std::string* fragment, unsigned owner = UINT_MAX) {
    if (fragment == nullptr) return false;
    std::lock_guard lock(mutex_);
    if (finishing_ || failed_.load(std::memory_order_relaxed)) return false;
    if (entry_owner_ != owner) return false;
    // A single Redis value may exceed the queue budget. Admit it only into an
    // empty queue, preserving a bounded one-value overshoot.
    if (!queue_.empty() &&
        (queued_bytes_ >= kMaximumQueuedBytes ||
         fragment->size() > kMaximumQueuedBytes - queued_bytes_)) {
      return false;
    }
    queued_bytes_ += fragment->size();
    queue_.push_back(std::move(*fragment));
    condition_.notify_one();
    return true;
  }

  void RequestFinish() {
    {
      std::lock_guard lock(mutex_);
      finishing_ = true;
    }
    condition_.notify_all();
  }

  void RequestAbort(absl::Status status) {
    Fail(std::move(status));
    {
      std::lock_guard lock(mutex_);
      finishing_ = true;
      queue_.clear();
      queued_bytes_ = 0;
    }
    condition_.notify_all();
  }

  absl::Status result() {
    if (thread_.joinable()) thread_.join();
    std::lock_guard lock(status_mutex_);
    return status_;
  }

 private:
  void Fail(absl::Status status) {
    {
      std::lock_guard lock(status_mutex_);
      if (status_.ok()) status_ = std::move(status);
    }
    failed_.store(true, std::memory_order_release);
  }

  void Run() {
    auto writer = rdb::FileWriter::Open(target_path_);
    if (!writer.ok()) {
      Fail(writer.status());
      ready_.store(true, std::memory_order_release);
      done_.store(true, std::memory_order_release);
      return;
    }
    ready_.store(true, std::memory_order_release);
    while (true) {
      std::string fragment;
      {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return finishing_ || !queue_.empty(); });
        if (queue_.empty()) {
          if (finishing_) break;
          continue;
        }
        fragment = std::move(queue_.front());
        queue_.pop_front();
        queued_bytes_ -= fragment.size();
      }
      absl::Status written = writer->WriteFragment(fragment);
      if (!written.ok()) {
        Fail(std::move(written));
        break;
      }
    }
    if (!failed()) {
      absl::Status finished = writer->Finish();
      if (!finished.ok()) Fail(std::move(finished));
    }
    done_.store(true, std::memory_order_release);
  }

  static constexpr std::size_t kMaximumQueuedBytes = 64ULL * 1024 * 1024;
  std::string target_path_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<std::string> queue_;
  std::size_t queued_bytes_ = 0;
  // A lease covers one complete RDB key, not one queue fragment. Other
  // producers cannot occupy the bounded queue while the lease owner loads
  // its next page, so backpressure cannot strand a partially emitted key.
  unsigned entry_owner_ = UINT_MAX;
  bool finishing_ = false;
  std::mutex status_mutex_;
  absl::Status status_;
  std::atomic<bool> ready_{false};
  std::atomic<bool> done_{false};
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

// Only the fields used by the delayed snapshot fence survive the command that
// requested BGSAVE SCHEDULE. Keeping this immutable value avoids retaining a
// connection-owned CommandRequest after its reply has been sent.
struct BackupRequestContext {
  std::uint64_t serving_generation_ = 0;
  bool serving_generation_valid_ = false;
  bool replication_origin_ = false;
};

BackupRequestContext CaptureRequestContext(const CommandRequest& request) {
  return BackupRequestContext{
      .serving_generation_ = request.serving_generation_,
      .serving_generation_valid_ =
          static_cast<bool>(request.serving_generation_valid_),
      .replication_origin_ = static_cast<bool>(request.replication_origin_),
  };
}

class BackupJob : public std::enable_shared_from_this<BackupJob> {
 public:
  BackupJob(storage::StorageEngine* storage, std::string target_path,
            std::uint64_t session_id, BackupRequestContext request)
      : storage_(storage),
        output_(std::move(target_path)),
        session_id_(session_id),
        serving_generation_(request.serving_generation_),
        serving_generation_valid_(request.serving_generation_valid_),
        replication_origin_(request.replication_origin_) {}

  Task<absl::Status> Run() {
    struct CutGuard {
      BackupJob* job_;
      bool complete_ = false;
      ~CutGuard() {
        if (complete_) return;
        job_->cut_failed_.store(true, std::memory_order_release);
        job_->cut_ready_.store(true, std::memory_order_release);
      }
    } cut_guard{this};
    while (!output_.ready()) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    if (output_.failed()) co_return output_.result();

    while (!CloseAllCommandDbGates()) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    struct GateGuard {
      bool open_ = false;
      ~GateGuard() {
        if (!open_) OpenAllCommandDbGates();
      }
    } gates;
    LAVIK_FAULT_INJECT(
        // Keep the real cut closed until the process test observes replica
        // admission contention and removes its hold file. Bound the hold so a
        // failed fixture cannot strand shutdown; GateGuard also covers errors.
        if (const char* hold = std::getenv("LAVIK_BACKUP_CUT_HOLD_FILE");
            hold != nullptr && std::filesystem::exists(hold)) {
          spdlog::warn("backup test checkpoint: database gates closed");
          const auto deadline =
              std::chrono::steady_clock::now() + std::chrono::seconds(20);
          while (std::filesystem::exists(hold)) {
            if (std::chrono::steady_clock::now() >= deadline)
              co_return absl::DeadlineExceededError(
                  "backup test cut hold expired");
            absl::Status waited = co_await bycorf::SleepFor(
                *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
            if (!waited.ok()) co_return waited;
          }
        });
    while (CommandDbOperationsActive()) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }

    // SAVE and BGSAVE manage their own all-database gate, so normal command
    // dispatch cannot perform the post-admission generation check for them.
    // Validate after winning and draining exclusivity: if a replacement won
    // first, its partial candidate must never become an externally requested
    // backup. If this cut wins first, the replacement waits on these gates and
    // the storage snapshot preserves the old population after they reopen.
    CommandRequest fence_request;
    fence_request.serving_generation_ = serving_generation_;
    fence_request.serving_generation_valid_ = serving_generation_valid_;
    fence_request.replication_origin_ = replication_origin_;
    if (const char* error = CommandServingGenerationError(fence_request);
        error != nullptr) [[unlikely]] {
      cut_error_ = error;
      co_return absl::FailedPreconditionError(cut_error_);
    }

    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const std::uint64_t snapshot_time_ms =
        now > 0 ? static_cast<std::uint64_t>(now) : 1;
    saved_change_cuts_.clear();
    saved_change_cuts_.reserve(storage_->worker_count());
    unsigned begun = 0;
    for (; begun < storage_->worker_count(); ++begun) {
      auto cut = co_await bycorf::SubmitTo(begun, [this, snapshot_time_ms] {
        absl::Status status =
            storage_->BeginRdbSnapshot(session_id_, snapshot_time_ms);
        return std::pair{std::move(status), LocalDatasetChangesTotal()};
      });
      if (!cut.first.ok()) {
        for (unsigned worker = 0; worker < begun; ++worker) {
          (void)co_await bycorf::SubmitTaskTo(
              worker, [this] { return storage_->EndRdbSnapshot(session_id_); });
        }
        co_return cut.first;
      }
      saved_change_cuts_.push_back(cut.second);
    }
    for (const LuaFunctionLibrary& library : SnapshotLuaFunctionLibraries()) {
      std::string fragment = rdb::EncodeFunctionLibraryEntry(library.code_);
      while (!output_.TryPush(&fragment)) {
        if (output_.failed()) {
          for (unsigned worker = 0; worker < begun; ++worker) {
            (void)co_await bycorf::SubmitTaskTo(worker, [this] {
              return storage_->EndRdbSnapshot(session_id_);
            });
          }
          co_return absl::InternalError("RDB output writer failed");
        }
        absl::Status yielded = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!yielded.ok()) {
          for (unsigned worker = 0; worker < begun; ++worker) {
            (void)co_await bycorf::SubmitTaskTo(worker, [this] {
              return storage_->EndRdbSnapshot(session_id_);
            });
          }
          co_return yielded;
        }
      }
    }
    OpenAllCommandDbGates();
    gates.open_ = true;
    cut_ready_.store(true, std::memory_order_release);
    cut_guard.complete_ = true;

    remaining_.store(storage_->worker_count(), std::memory_order_release);
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      auto context =
          std::make_unique<std::shared_ptr<BackupJob>>(shared_from_this());
      bycorf::PostNotification(
          bycorf::ThisWorker().cross_core_, worker,
          bycorf::RemoteNotification{
              .context_ = context.release(),
              .value_ = worker,
              .run_fn_ =
                  [](void* raw, std::uint64_t worker_id) noexcept {
                    std::unique_ptr<std::shared_ptr<BackupJob>> job(
                        static_cast<std::shared_ptr<BackupJob>*>(raw));
                    (*job)->SpawnWorker(static_cast<unsigned>(worker_id));
                  },
          });
    }
    while (remaining_.load(std::memory_order_acquire) != 0) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    absl::Status scan_status = status();
    if (scan_status.ok()) {
      output_.RequestFinish();
    } else {
      // Do not atomically replace the previous dump with a partial snapshot.
      // Marking the sink failed makes FileWriter destroy its temporary file.
      output_.RequestAbort(scan_status);
    }
    while (!output_.done()) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    absl::Status output_status = output_.result();
    co_return scan_status.ok() ? output_status : scan_status;
  }

  bool cut_ready() const noexcept {
    return cut_ready_.load(std::memory_order_acquire);
  }
  bool cut_failed() const noexcept {
    return cut_failed_.load(std::memory_order_acquire);
  }
  std::string_view cut_error() const noexcept { return cut_error_; }
  const std::vector<std::uint64_t>& saved_change_cuts() const noexcept {
    return saved_change_cuts_;
  }

 private:
  static Task<absl::Status> RunOwnedWorker(std::shared_ptr<BackupJob> job,
                                           unsigned worker_id) {
    // Keep this coroutine frame: it owns job until ScanWorker completes.
    co_return co_await job->ScanWorker(worker_id);
  }

  void SpawnWorker(unsigned worker_id) {
    bycorf::ThisWorker().self_->SpawnBackground(
        RunOwnedWorker(shared_from_this(), worker_id));
  }

  Task<absl::Status> PushEntrySpan(unsigned worker_id, std::string_view bytes) {
    constexpr std::size_t fragment_bytes = 1024 * 1024;
    while (!bytes.empty()) {
      const auto piece = bytes.substr(0, fragment_bytes);
      std::string fragment(piece);
      while (!output_.TryPush(&fragment, worker_id)) {
        if (output_.failed())
          co_return absl::InternalError("RDB output writer failed");
        auto status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                std::chrono::milliseconds(1));
        if (!status.ok()) co_return status;
      }
      bytes.remove_prefix(piece.size());
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> WriteCollection(unsigned worker_id,
                                     const storage::RdbSnapshotValue& value) {
    auto encoder = rdb::CollectionFileEncoder::Create(
        value.db_id_, value.key_, value.value_.value_type_,
        value.value_.logical_size_, value.value_.expire_at_ms_);
    if (!encoder.ok()) co_return encoder.status();
    auto drain = [&]() -> Task<absl::Status> {
      while (auto span = encoder->Next()) {
        auto status = co_await PushEntrySpan(worker_id, *span);
        if (!status.ok()) co_return status;
      }
      co_return absl::OkStatus();
    };
    auto status = co_await drain();
    if (!status.ok()) co_return status;
    std::uint64_t cursor = 0;
    for (;;) {
      auto page = co_await storage_->ReadRdbCollectionPage(
          session_id_, value.collection_token_, cursor);
      if (!page.ok()) co_return page.status();
      status = encoder->StartPage(*page);
      if (!status.ok()) co_return status;
      status = co_await drain();
      if (!status.ok()) co_return status;
      cursor = page->next_cursor_;
      if (page->done_) break;
      // Keep at most one decoded page while disk/output waits. The encoder
      // has drained its borrowed spans before this page goes out of scope.
    }
    status = encoder->Finish();
    if (!status.ok()) co_return status;
    co_return co_await storage_->FinishRdbCollection(session_id_,
                                                     value.collection_token_);
  }

  Task<absl::Status> ScanWorker(unsigned worker_id) {
    absl::Status status = absl::OkStatus();
    storage::RdbSnapshotCursor cursor;
    unsigned reads_since_yield = 0;
    try {
      while (status.ok()) {
        // Grouped keys carry only a retained-view token; pages are loaded after
        // this producer acquires exclusive ownership of the file-entry stream.
        auto batch = co_await storage_->ReadRdbSnapshotBatch(
            session_id_, cursor, 1, 8ULL * 1024 * 1024);
        if (!batch.ok()) {
          status = batch.status();
          break;
        }
        cursor = batch->cursor_;
        for (storage::RdbSnapshotValue& value : batch->values_) {
          while (!output_.TryBeginEntry(worker_id)) {
            if (output_.failed()) {
              status = absl::InternalError("RDB output writer failed");
              break;
            }
            status = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                               std::chrono::milliseconds(1));
            if (!status.ok()) break;
          }
          if (!status.ok()) break;
          struct EntryLease {
            RdbOutputQueue* queue;
            unsigned owner;
            ~EntryLease() { queue->EndEntry(owner); }
          } lease{&output_, worker_id};
          if (value.collection_token_ != 0) {
            status = co_await WriteCollection(worker_id, value);
            if (!status.ok()) break;
            continue;
          }
          auto fragment =
              rdb::EncodeFileEntry(value.db_id_, value.key_, value.value_);
          // Dirty tracking holds only pinned physical locations. Drop this
          // transient materialization as soon as its RDB bytes exist, before a
          // full output queue can suspend this worker.
          std::string().swap(value.value_.encoded_);
          std::string().swap(value.key_);
          if (!fragment.ok()) {
            status = fragment.status();
            break;
          }
          status = co_await PushEntrySpan(worker_id, *fragment);
          if (!status.ok()) break;
        }
        if (!status.ok() || batch->done_) break;
        if (++reads_since_yield == 64) {
          reads_since_yield = 0;
          co_await bycorf::Yield(*bycorf::ThisWorker().self_);
        }
      }
    } catch (const std::bad_alloc&) {
      // Cancellation still closes the token and releases physical pins. The
      // incomplete temporary output must not replace the previous dump.
      RecordMemoryRejection();
      status = absl::ResourceExhaustedError("OOM RDB backup producer");
    }
    absl::Status ended = co_await storage_->EndRdbSnapshot(session_id_);
    if (status.ok()) status = std::move(ended);
    Complete(std::move(status));
    co_return absl::OkStatus();
  }

  void Complete(absl::Status status) {
    if (!status.ok()) {
      std::lock_guard lock(status_mutex_);
      if (status_.ok()) status_ = std::move(status);
    }
    remaining_.fetch_sub(1, std::memory_order_acq_rel);
  }

  absl::Status status() const {
    std::lock_guard lock(status_mutex_);
    return status_;
  }

  storage::StorageEngine* storage_;
  RdbOutputQueue output_;
  std::uint64_t session_id_ = 0;
  std::uint64_t serving_generation_ = 0;
  bool serving_generation_valid_ = false;
  bool replication_origin_ = false;
  std::atomic<unsigned> remaining_{0};
  std::atomic<bool> cut_ready_{false};
  std::atomic<bool> cut_failed_{false};
  // Written before cut_failed_'s release store and read only after its acquire
  // load, so the command coroutine can preserve the exact Redis fence error.
  std::string cut_error_;
  std::vector<std::uint64_t> saved_change_cuts_;
  mutable std::mutex status_mutex_;
  absl::Status status_;
};

storage::StorageEngine* g_backup_storage = nullptr;
std::string g_backup_target_path;
std::vector<RdbSaveRule> g_save_rules;
// Scheduling decisions and these two fields belong to worker zero. The atomic
// active bit is only the main thread's low-frequency shutdown join; ordinary
// writes never touch shared backup state.
std::optional<BackupRequestContext> g_scheduled_backup;
std::chrono::steady_clock::time_point g_last_successful_save;
std::chrono::steady_clock::time_point g_next_automatic_attempt;
std::atomic<bool> g_backup_active{false};
std::atomic<bool> g_automatic_backups_stopped{false};
std::atomic<std::uint64_t> g_next_backup_session{1};
std::atomic<std::uint64_t> g_last_save_seconds{0};

std::shared_ptr<BackupJob> MakeBackup(BackupRequestContext request) {
  assert(bycorf::ThisWorker().id_ == 0);
  std::uint64_t session =
      g_next_backup_session.fetch_add(1, std::memory_order_relaxed);
  if (session == 0) {
    session = g_next_backup_session.fetch_add(1, std::memory_order_relaxed);
  }
  return std::make_shared<BackupJob>(g_backup_storage, g_backup_target_path,
                                     session, request);
}

Task<absl::Status> FinishBackup(std::shared_ptr<BackupJob> job) {
  assert(bycorf::ThisWorker().id_ == 0);
  absl::Status status = co_await job->Run();
  if (status.ok()) {
    const auto& cuts = job->saved_change_cuts();
    for (unsigned worker = 0; worker < cuts.size(); ++worker) {
      co_await bycorf::SubmitTo(worker, [saved = cuts[worker]] {
        MarkLocalDatasetChangesSaved(saved);
        return true;
      });
    }
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    g_last_save_seconds.store(now > 0 ? static_cast<std::uint64_t>(now) : 1,
                              std::memory_order_release);
    g_last_successful_save = std::chrono::steady_clock::now();
    g_next_automatic_attempt = g_last_successful_save;
    spdlog::info("RDB backup completed: {}", g_backup_target_path);
  } else {
    // Match Redis's bounded retry behavior closely enough to avoid retrying a
    // broken output path on every one-second policy tick.
    g_next_automatic_attempt =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    spdlog::error("RDB backup failed: {}", status.message());
  }

  if (g_scheduled_backup.has_value()) {
    BackupRequestContext scheduled = *g_scheduled_backup;
    g_scheduled_backup.reset();
    auto next = MakeBackup(scheduled);
    bycorf::ThisWorker().self_->Spawn(FinishBackup(std::move(next)));
    co_return status;
  }
  g_backup_active.store(false, std::memory_order_release);
  g_backup_active.notify_all();
  co_return status;
}

std::shared_ptr<BackupJob> TryStartBackup(BackupRequestContext request) {
  assert(bycorf::ThisWorker().id_ == 0);
  if (g_backup_active.load(std::memory_order_relaxed)) return nullptr;
  g_backup_active.store(true, std::memory_order_release);
  return MakeBackup(request);
}

CommandReply Reply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

}  // namespace

void InitRdbBackup(storage::StorageEngine* storage, std::string target_path,
                   std::vector<RdbSaveRule> save_rules) {
  g_backup_storage = storage;
  g_backup_target_path = std::move(target_path);
  g_save_rules = std::move(save_rules);
  g_scheduled_backup.reset();
  g_last_successful_save = std::chrono::steady_clock::now();
  g_next_automatic_attempt = g_last_successful_save;
  g_backup_active.store(false, std::memory_order_relaxed);
  g_automatic_backups_stopped.store(false, std::memory_order_relaxed);
}

bool AutomaticRdbBackupsConfigured() noexcept { return !g_save_rules.empty(); }

Task<CommandReply> ExecuteRdbBackupCommand(const CommandRequest& request,
                                           ReplyBuilder& reply_builder) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [&request, &reply_builder]() -> Task<CommandReply> {
          co_return co_await ExecuteRdbBackupCommand(request, reply_builder);
        });
  }
  if (request.kind_ == CommandKind::kLastSave) {
    co_return Reply(reply_builder.AppendInteger(
        g_last_save_seconds.load(std::memory_order_acquire)));
  }
  if (g_backup_storage == nullptr || g_backup_target_path.empty()) {
    co_return Reply(
        reply_builder.AppendError("ERR RDB backup is not configured"));
  }
  const bool schedule =
      request.kind_ == CommandKind::kBgSave && request.args_.size() == 2;
  if (schedule && !absl::EqualsIgnoreCase(request.args_[1], "SCHEDULE")) {
    co_return Reply(reply_builder.AppendError("ERR syntax error"));
  }
  const BackupRequestContext context = CaptureRequestContext(request);
  std::shared_ptr<BackupJob> job = TryStartBackup(context);
  if (job == nullptr) {
    if (schedule) {
      if (!g_scheduled_backup.has_value()) g_scheduled_backup = context;
      co_return Reply(
          reply_builder.AppendSimpleString("Background saving scheduled"));
    }
    co_return Reply(
        reply_builder.AppendError("ERR Background save already in progress"));
  }
  if (request.kind_ == CommandKind::kBgSave) {
    std::shared_ptr<BackupJob> cut = job;
    bycorf::ThisWorker().self_->Spawn(FinishBackup(std::move(job)));
    while (!cut->cut_ready()) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) {
        co_return Reply(reply_builder.AppendError(
            absl::StrCat("ERR RDB cut failed: ", yielded.message())));
      }
    }
    if (cut->cut_failed()) {
      if (!cut->cut_error().empty()) {
        co_return Reply(reply_builder.AppendError(cut->cut_error()));
      }
      co_return Reply(reply_builder.AppendError("ERR RDB cut failed"));
    }
    co_return Reply(
        reply_builder.AppendSimpleString("Background saving started"));
  }
  absl::Status status = co_await FinishBackup(job);
  if (!status.ok()) {
    if (!job->cut_error().empty()) {
      co_return Reply(reply_builder.AppendError(job->cut_error()));
    }
    co_return Reply(reply_builder.AppendError(
        absl::StrCat("ERR RDB save failed: ", status.message())));
  }
  co_return Reply(reply_builder.AppendSimpleString("OK"));
}

Task<absl::Status> RunRdbBackupScheduler(bycorf::Worker& worker) {
  assert(worker.id() == 0);
  while (!worker.stop_requested()) {
    absl::Status slept =
        co_await bycorf::SleepFor(worker, std::chrono::seconds(1));
    if (!slept.ok()) co_return absl::OkStatus();
    if (g_automatic_backups_stopped.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();
    }
    if (g_save_rules.empty() ||
        g_backup_active.load(std::memory_order_relaxed) ||
        std::chrono::steady_clock::now() < g_next_automatic_attempt) {
      continue;
    }

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - g_last_successful_save)
            .count();
    bool time_eligible = false;
    for (const RdbSaveRule& rule : g_save_rules) {
      if (elapsed >= 0 &&
          static_cast<std::uint64_t>(elapsed) >= rule.seconds_) {
        time_eligible = true;
        break;
      }
    }
    if (!time_eligible) continue;

    const std::uint64_t changes = co_await CollectDatasetChangesSinceLastSave();
    // Shutdown and explicit commands can run while the cross-worker
    // collection is suspended. Re-check ownership state before starting work
    // from a decision made against that earlier snapshot.
    if (g_automatic_backups_stopped.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();
    }
    if (g_backup_active.load(std::memory_order_relaxed)) {
      continue;
    }
    bool should_save = false;
    for (const RdbSaveRule& rule : g_save_rules) {
      if (elapsed >= 0 &&
          static_cast<std::uint64_t>(elapsed) >= rule.seconds_ &&
          changes >= rule.changes_) {
        should_save = true;
        break;
      }
    }
    if (!should_save) continue;

    auto job = TryStartBackup(BackupRequestContext{});
    if (job != nullptr) {
      spdlog::info("automatic RDB save triggered after {} changes", changes);
      worker.Spawn(FinishBackup(std::move(job)));
    }
  }
  co_return absl::OkStatus();
}

void StopAutomaticRdbBackups() noexcept {
  g_automatic_backups_stopped.store(true, std::memory_order_release);
}

void WaitForRdbBackupDrained() noexcept {
  bool active = g_backup_active.load(std::memory_order_acquire);
  while (active) {
    g_backup_active.wait(active, std::memory_order_acquire);
    active = g_backup_active.load(std::memory_order_acquire);
  }
}

}  // namespace lavik
