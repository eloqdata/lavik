#include <charconv>
#include <fstream>
#include <iterator>
#include <sstream>
#include <system_error>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "tests/cluster/fault_harness.h"

namespace keylane::test::cluster {
namespace {

constexpr std::string_view kTraceMagic = "KFT1";

bool IsUnescaped(unsigned char byte) {
  return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
         (byte >= '0' && byte <= '9') || byte == '-' || byte == '_' ||
         byte == '.';
}

char HexDigit(unsigned value) {
  return value < 10 ? static_cast<char>('0' + value)
                    : static_cast<char>('A' + value - 10);
}

std::string Escape(std::string_view input) {
  std::string result;
  result.reserve(input.size());
  for (const unsigned char byte : input) {
    if (IsUnescaped(byte)) {
      result.push_back(static_cast<char>(byte));
      continue;
    }
    result.push_back('%');
    result.push_back(HexDigit(byte >> 4));
    result.push_back(HexDigit(byte & 0x0f));
  }
  return result;
}

std::optional<unsigned> ParseHex(char value) {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'A' && value <= 'F') return value - 'A' + 10;
  return std::nullopt;
}

absl::StatusOr<std::string> Unescape(std::string_view input) {
  std::string result;
  result.reserve(input.size());
  for (std::size_t i = 0; i < input.size(); ++i) {
    if (input[i] != '%') {
      result.push_back(input[i]);
      continue;
    }
    if (i + 2 >= input.size()) {
      return absl::InvalidArgumentError("truncated KFT1 escape");
    }
    const auto high = ParseHex(input[i + 1]);
    const auto low = ParseHex(input[i + 2]);
    if (!high.has_value() || !low.has_value()) {
      return absl::InvalidArgumentError("invalid KFT1 escape");
    }
    result.push_back(static_cast<char>((*high << 4) | *low));
    i += 2;
  }
  return result;
}

template <typename Integer>
absl::StatusOr<Integer> ParseInteger(std::string_view input,
                                     std::string_view field) {
  Integer value = 0;
  const auto parsed =
      std::from_chars(input.data(), input.data() + input.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size()) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid KFT1 ", field, ": ", input));
  }
  return value;
}

std::string_view ModeName(TraceMode mode) {
  switch (mode) {
    case TraceMode::kExactModel:
      return "exact-model";
    case TraceMode::kProcessActionScript:
      return "process-action-script";
  }
  return "unknown";
}

absl::StatusOr<TraceMode> ParseMode(std::string_view value) {
  if (value == "exact-model") return TraceMode::kExactModel;
  if (value == "process-action-script") {
    return TraceMode::kProcessActionScript;
  }
  return absl::InvalidArgumentError("unknown KFT1 mode");
}

std::string_view RecordKindName(TraceRecordKind kind) {
  switch (kind) {
    case TraceRecordKind::kChoice:
      return "choice";
    case TraceRecordKind::kAcknowledgment:
      return "ack";
    case TraceRecordKind::kObservation:
      return "observation";
    case TraceRecordKind::kFinding:
      return "finding";
  }
  return "unknown";
}

absl::StatusOr<TraceRecordKind> ParseRecordKind(std::string_view value) {
  if (value == "choice") return TraceRecordKind::kChoice;
  if (value == "ack") return TraceRecordKind::kAcknowledgment;
  if (value == "observation") return TraceRecordKind::kObservation;
  if (value == "finding") return TraceRecordKind::kFinding;
  return absl::InvalidArgumentError("unknown KFT1 record kind");
}

class TemporaryTrace {
 public:
  explicit TemporaryTrace(std::filesystem::path path)
      : path_(std::move(path)) {}
  TemporaryTrace(const TemporaryTrace&) = delete;
  TemporaryTrace& operator=(const TemporaryTrace&) = delete;
  ~TemporaryTrace() {
    if (!installed_) {
      std::error_code ignored;
      std::filesystem::remove(path_, ignored);
    }
  }

  const std::filesystem::path& path() const noexcept { return path_; }
  void MarkInstalled() noexcept { installed_ = true; }

 private:
  std::filesystem::path path_;
  bool installed_ = false;
};

}  // namespace

std::string EncodeAction(const Action& action) {
  std::string result = Escape(action.name_);
  result.push_back(';');
  result += std::to_string(action.arguments_.size());
  for (const std::int64_t argument : action.arguments_) {
    result.push_back(';');
    result += std::to_string(argument);
  }
  result.push_back(';');
  result += Escape(action.payload_);
  return result;
}

absl::StatusOr<Action> DecodeAction(std::string_view encoded) {
  std::vector<std::string_view> fields = absl::StrSplit(encoded, ';');
  if (fields.size() < 3) {
    return absl::InvalidArgumentError("invalid KFT1 action");
  }
  auto name = Unescape(fields[0]);
  if (!name.ok()) return name.status();
  auto argument_count = ParseInteger<std::size_t>(fields[1], "argument count");
  if (!argument_count.ok()) return argument_count.status();
  if (fields.size() != *argument_count + 3) {
    return absl::InvalidArgumentError("KFT1 action argument count mismatch");
  }
  Action action{.name_ = std::move(*name), .arguments_ = {}, .payload_ = {}};
  action.arguments_.reserve(*argument_count);
  for (std::size_t i = 0; i < *argument_count; ++i) {
    auto argument =
        ParseInteger<std::int64_t>(fields[i + 2], "action argument");
    if (!argument.ok()) return argument.status();
    action.arguments_.push_back(*argument);
  }
  auto payload = Unescape(fields.back());
  if (!payload.ok()) return payload.status();
  action.payload_ = std::move(*payload);
  return action;
}

std::string EncodeTrace(const Trace& trace) {
  std::ostringstream output;
  output << kTraceMagic << '\n';
  output << "mode\t" << ModeName(trace.mode_) << '\n';
  output << "scenario\t" << Escape(trace.scenario_) << '\n';
  output << "schema\t" << trace.schema_ << '\n';
  output << "seed\t" << trace.seed_ << '\n';
  output << "initial\t" << Escape(trace.initial_state_hash_) << '\n';
  for (const TraceRecord& record : trace.records_) {
    output << "record\t" << record.logical_time_ << '\t'
           << RecordKindName(record.kind_) << '\t' << Escape(record.name_)
           << '\t' << Escape(record.payload_) << '\n';
  }
  return output.str();
}

absl::StatusOr<Trace> DecodeTrace(std::string_view encoded) {
  std::vector<std::string_view> lines = absl::StrSplit(encoded, '\n');
  if (!lines.empty() && lines.back().empty()) lines.pop_back();
  if (lines.size() < 6 || lines[0] != kTraceMagic) {
    return absl::InvalidArgumentError("missing or invalid KFT1 header");
  }
  auto header_value =
      [&](std::size_t index,
          std::string_view expected) -> absl::StatusOr<std::string_view> {
    std::vector<std::string_view> fields = absl::StrSplit(lines[index], '\t');
    if (fields.size() != 2 || fields[0] != expected) {
      return absl::InvalidArgumentError(
          absl::StrCat("invalid KFT1 ", expected, " header"));
    }
    return fields[1];
  };

  Trace trace;
  auto mode_text = header_value(1, "mode");
  if (!mode_text.ok()) return mode_text.status();
  auto mode = ParseMode(*mode_text);
  if (!mode.ok()) return mode.status();
  trace.mode_ = *mode;
  auto scenario_text = header_value(2, "scenario");
  if (!scenario_text.ok()) return scenario_text.status();
  auto scenario = Unescape(*scenario_text);
  if (!scenario.ok()) return scenario.status();
  trace.scenario_ = std::move(*scenario);
  auto schema_text = header_value(3, "schema");
  if (!schema_text.ok()) return schema_text.status();
  auto schema = ParseInteger<std::uint32_t>(*schema_text, "schema");
  if (!schema.ok()) return schema.status();
  trace.schema_ = *schema;
  auto seed_text = header_value(4, "seed");
  if (!seed_text.ok()) return seed_text.status();
  auto seed = ParseInteger<std::uint64_t>(*seed_text, "seed");
  if (!seed.ok()) return seed.status();
  trace.seed_ = *seed;
  auto initial_text = header_value(5, "initial");
  if (!initial_text.ok()) return initial_text.status();
  auto initial = Unescape(*initial_text);
  if (!initial.ok()) return initial.status();
  trace.initial_state_hash_ = std::move(*initial);

  for (std::size_t i = 6; i < lines.size(); ++i) {
    std::vector<std::string_view> fields = absl::StrSplit(lines[i], '\t');
    if (fields.size() != 5 || fields[0] != "record") {
      return absl::InvalidArgumentError("invalid KFT1 record");
    }
    auto logical_time =
        ParseInteger<std::uint64_t>(fields[1], "record logical time");
    if (!logical_time.ok()) return logical_time.status();
    auto kind = ParseRecordKind(fields[2]);
    if (!kind.ok()) return kind.status();
    auto name = Unescape(fields[3]);
    if (!name.ok()) return name.status();
    auto payload = Unescape(fields[4]);
    if (!payload.ok()) return payload.status();
    trace.records_.push_back(TraceRecord{.logical_time_ = *logical_time,
                                         .kind_ = *kind,
                                         .name_ = std::move(*name),
                                         .payload_ = std::move(*payload)});
  }
  return trace;
}

absl::Status WriteTraceAtomically(const Trace& trace,
                                  const std::filesystem::path& path) {
  std::error_code error;
  if (!path.parent_path().empty()) {
    std::filesystem::create_directories(path.parent_path(), error);
    if (error) {
      return absl::InternalError(
          absl::StrCat("create trace directory failed: ", error.message()));
    }
  }
  TemporaryTrace temporary(path.string() + ".tmp");
  {
    std::ofstream output(temporary.path(), std::ios::binary | std::ios::trunc);
    if (!output) {
      return absl::InternalError("open temporary trace failed");
    }
    output << EncodeTrace(trace);
    output.flush();
    if (!output) {
      return absl::InternalError("write temporary trace failed");
    }
  }
  std::filesystem::rename(temporary.path(), path, error);
  if (error) {
    return absl::InternalError(
        absl::StrCat("install trace failed: ", error.message()));
  }
  temporary.MarkInstalled();
  return absl::OkStatus();
}

absl::StatusOr<Trace> ReadTrace(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return absl::NotFoundError("trace file not found");
  std::string encoded((std::istreambuf_iterator<char>(input)),
                      std::istreambuf_iterator<char>());
  return DecodeTrace(encoded);
}

}  // namespace keylane::test::cluster
