#include "backup.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/rdb.h"
#include "keylane/metrics.h"
#include "keylane/resp.h"
#include "lua_eval.h"
#include "spdlog/spdlog.h"

namespace keylane {
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

  bool TryPush(std::string* fragment) {
    if (fragment == nullptr) return false;
    std::lock_guard lock(mutex_);
    if (finishing_ || failed_.load(std::memory_order_relaxed)) return false;
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
  bool finishing_ = false;
  std::mutex status_mutex_;
  absl::Status status_;
  std::atomic<bool> ready_{false};
  std::atomic<bool> done_{false};
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

class BackupJob : public std::enable_shared_from_this<BackupJob> {
 public:
  BackupJob(storage::StorageEngine* storage, std::string target_path,
            std::uint64_t session_id)
      : storage_(storage),
        output_(std::move(target_path)),
        session_id_(session_id) {}

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
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    if (output_.failed()) co_return output_.result();

    while (!CloseAllCommandDbGates()) {
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    struct GateGuard {
      bool open_ = false;
      ~GateGuard() {
        if (!open_) OpenAllCommandDbGates();
      }
    } gates;
    while (CommandDbOperationsActive()) {
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
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
      auto cut = co_await celer::SubmitTo(
          begun, [this, snapshot_time_ms] {
            absl::Status status =
                storage_->BeginRdbSnapshot(session_id_, snapshot_time_ms);
            return std::pair{std::move(status), LocalDatasetChangesTotal()};
          });
      if (!cut.first.ok()) {
        for (unsigned worker = 0; worker < begun; ++worker) {
          (void)co_await celer::SubmitTaskTo(
              worker, [this] { return storage_->EndRdbSnapshot(session_id_); });
        }
        co_return cut.first;
      }
      saved_change_cuts_.push_back(cut.second);
    }
    for (const LuaFunctionLibrary& library :
         SnapshotLuaFunctionLibraries()) {
      std::string fragment = rdb::EncodeFunctionLibraryEntry(library.code_);
      while (!output_.TryPush(&fragment)) {
        if (output_.failed()) {
          for (unsigned worker = 0; worker < begun; ++worker) {
            (void)co_await celer::SubmitTaskTo(worker, [this] {
              return storage_->EndRdbSnapshot(session_id_);
            });
          }
          co_return absl::InternalError("RDB output writer failed");
        }
        absl::Status yielded = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!yielded.ok()) {
          for (unsigned worker = 0; worker < begun; ++worker) {
            (void)co_await celer::SubmitTaskTo(worker, [this] {
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
      celer::PostNotification(
          celer::ThisWorker().cross_core_, worker,
          celer::RemoteNotification{
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
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
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
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
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
    celer::ThisWorker().self_->SpawnBackground(
        RunOwnedWorker(shared_from_this(), worker_id));
  }

  Task<absl::Status> ScanWorker(unsigned worker_id) {
    (void)worker_id;
    absl::Status status = absl::OkStatus();
    storage::RdbSnapshotCursor cursor;
    unsigned reads_since_yield = 0;
    while (status.ok()) {
      // Never retain a batch of unencoded old values. Materialize exactly one,
      // turn it into an RDB fragment, and release it immediately; only encoded
      // fragments are allowed to accumulate in the bounded output queue.
      auto batch = co_await storage_->ReadRdbSnapshotBatch(
          session_id_, cursor, 1, 8ULL * 1024 * 1024);
      if (!batch.ok()) {
        status = batch.status();
        break;
      }
      cursor = batch->cursor_;
      for (storage::RdbSnapshotValue& value : batch->values_) {
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
        while (!output_.TryPush(&*fragment)) {
          if (output_.failed()) {
            status = absl::InternalError("RDB output writer failed");
            break;
          }
          absl::Status yielded = co_await celer::SleepFor(
              *celer::ThisWorker().self_, std::chrono::milliseconds(1));
          if (!yielded.ok()) {
            status = yielded;
            break;
          }
        }
        if (!status.ok()) break;
      }
      if (!status.ok() || batch->done_) break;
      if (++reads_since_yield == 64) {
        reads_since_yield = 0;
        co_await celer::Yield(*celer::ThisWorker().self_);
      }
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
  std::atomic<unsigned> remaining_{0};
  std::atomic<bool> cut_ready_{false};
  std::atomic<bool> cut_failed_{false};
  std::vector<std::uint64_t> saved_change_cuts_;
  mutable std::mutex status_mutex_;
  absl::Status status_;
};

storage::StorageEngine* g_backup_storage = nullptr;
std::string g_backup_target_path;
std::atomic<bool> g_backup_active{false};
std::atomic<std::uint64_t> g_next_backup_session{1};
std::atomic<std::uint64_t> g_last_save_seconds{0};

Task<absl::Status> FinishBackup(std::shared_ptr<BackupJob> job) {
  absl::Status status = co_await job->Run();
  if (status.ok()) {
    const auto& cuts = job->saved_change_cuts();
    for (unsigned worker = 0; worker < cuts.size(); ++worker) {
      co_await celer::SubmitTo(worker, [saved = cuts[worker]] {
        MarkLocalDatasetChangesSaved(saved);
        return true;
      });
    }
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    g_last_save_seconds.store(now > 0 ? static_cast<std::uint64_t>(now) : 1,
                              std::memory_order_release);
    spdlog::info("RDB backup completed: {}", g_backup_target_path);
  } else {
    spdlog::error("RDB backup failed: {}", status.message());
  }
  g_backup_active.store(false, std::memory_order_release);
  g_backup_active.notify_all();
  co_return status;
}

std::shared_ptr<BackupJob> TryStartBackup() {
  bool expected = false;
  if (!g_backup_active.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
    return nullptr;
  }
  std::uint64_t session =
      g_next_backup_session.fetch_add(1, std::memory_order_relaxed);
  if (session == 0) {
    session = g_next_backup_session.fetch_add(1, std::memory_order_relaxed);
  }
  return std::make_shared<BackupJob>(g_backup_storage, g_backup_target_path,
                                     session);
}

CommandReply Reply(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

}  // namespace

void InitRdbBackup(storage::StorageEngine* storage, std::string target_path) {
  g_backup_storage = storage;
  g_backup_target_path = std::move(target_path);
}

Task<CommandReply> ExecuteRdbBackupCommand(const CommandRequest& request,
                                           ReplyBuilder& reply_builder) {
  if (request.kind_ == CommandKind::kLastSave) {
    co_return Reply(reply_builder.AppendInteger(
        g_last_save_seconds.load(std::memory_order_acquire)));
  }
  if (g_backup_storage == nullptr || g_backup_target_path.empty()) {
    co_return Reply(
        reply_builder.AppendError("ERR RDB backup is not configured"));
  }
  std::shared_ptr<BackupJob> job = TryStartBackup();
  if (job == nullptr) {
    co_return Reply(
        reply_builder.AppendError("ERR Background save already in progress"));
  }
  if (request.kind_ == CommandKind::kBgSave) {
    std::shared_ptr<BackupJob> cut = job;
    celer::ThisWorker().self_->Spawn(FinishBackup(std::move(job)));
    while (!cut->cut_ready()) {
      absl::Status yielded = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!yielded.ok()) {
        co_return Reply(reply_builder.AppendError(
            absl::StrCat("ERR RDB cut failed: ", yielded.message())));
      }
    }
    if (cut->cut_failed()) {
      co_return Reply(reply_builder.AppendError("ERR RDB cut failed"));
    }
    co_return Reply(
        reply_builder.AppendSimpleString("Background saving started"));
  }
  absl::Status status = co_await FinishBackup(std::move(job));
  if (!status.ok()) {
    co_return Reply(reply_builder.AppendError(
        absl::StrCat("ERR RDB save failed: ", status.message())));
  }
  co_return Reply(reply_builder.AppendSimpleString("OK"));
}

void WaitForRdbBackupDrained() noexcept {
  bool active = g_backup_active.load(std::memory_order_acquire);
  while (active) {
    g_backup_active.wait(active, std::memory_order_acquire);
    active = g_backup_active.load(std::memory_order_acquire);
  }
}

}  // namespace keylane
