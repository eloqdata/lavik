#include "keylane/meta/value_codec.h"

#include <string>

namespace keylane::meta {

void WriteActorContext(MetaWriter& writer, const ActorContext& actor) {
  writer.WriteString(actor.principal_);
  writer.WriteString(actor.readable_time_);
}

absl::StatusOr<ActorContext> ReadActorContext(MetaReader& reader) {
  auto principal = reader.ReadString(kMaxMetaPrincipalBytes);
  if (!principal.ok()) return principal.status();
  auto readable_time = reader.ReadString(kMaxMetaActorReadableTimeBytes);
  if (!readable_time.ok()) return readable_time.status();
  return ActorContext{std::string(*principal), std::string(*readable_time)};
}

void WriteMetaDirectiveSpec(MetaWriter& writer,
                            const MetaDirectiveSpec& directive) {
  WriteFixedArray(writer, directive.directive_id_);
  WriteFixedArray(writer, directive.attempt_id_);
  writer.WriteString(directive.recipient_node_id_);
  writer.WriteString(directive.target_node_id_);
  WriteFixedArray(writer, directive.target_boot_id_);
  WriteFixedArray(writer, directive.assignment_id_);
  writer.WriteString(directive.source_node_id_);
  WriteFixedArray(writer, directive.source_assignment_id_);
  WriteFixedArray(writer, directive.source_boot_id_);
  WriteFixedArray(writer, directive.source_replication_history_id_);
  writer.WriteString(directive.group_id_);
  writer.WriteU64(directive.group_term_);
  writer.WriteU64(directive.population_manifest_revision_);
  WriteFixedArray(writer, directive.population_manifest_digest_);
  writer.WriteU64(directive.partition_replication_epoch_);
  writer.WriteString(directive.kind_);
  writer.WriteString(directive.payload_);
}

absl::StatusOr<MetaDirectiveSpec> ReadMetaDirectiveSpec(MetaReader& reader) {
  MetaDirectiveSpec directive;
  auto directive_id = ReadFixedArray<16>(reader);
  if (!directive_id.ok()) return directive_id.status();
  directive.directive_id_ = *directive_id;
  auto attempt_id = ReadFixedArray<16>(reader);
  if (!attempt_id.ok()) return attempt_id.status();
  directive.attempt_id_ = *attempt_id;
  auto recipient_node = reader.ReadString(kMetaNodeIdBytes);
  if (!recipient_node.ok()) return recipient_node.status();
  directive.recipient_node_id_ = std::string(*recipient_node);
  auto target_node = reader.ReadString(kMetaNodeIdBytes);
  if (!target_node.ok()) return target_node.status();
  directive.target_node_id_ = std::string(*target_node);
  auto target_boot = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!target_boot.ok()) return target_boot.status();
  directive.target_boot_id_ = *target_boot;
  auto assignment_id = ReadFixedArray<16>(reader);
  if (!assignment_id.ok()) return assignment_id.status();
  directive.assignment_id_ = *assignment_id;
  auto source_node = reader.ReadString(kMetaNodeIdBytes);
  if (!source_node.ok()) return source_node.status();
  directive.source_node_id_ = std::string(*source_node);
  auto source_assignment_id = ReadFixedArray<16>(reader);
  if (!source_assignment_id.ok()) return source_assignment_id.status();
  directive.source_assignment_id_ = *source_assignment_id;
  auto source_boot = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!source_boot.ok()) return source_boot.status();
  directive.source_boot_id_ = *source_boot;
  auto source_history = ReadFixedArray<kMetaReplicationHistoryIdBytes>(reader);
  if (!source_history.ok()) return source_history.status();
  directive.source_replication_history_id_ = *source_history;
  auto group_id = reader.ReadString(kMaxMetaGroupIdBytes);
  if (!group_id.ok()) return group_id.status();
  directive.group_id_ = std::string(*group_id);
  auto group_term = reader.ReadU64();
  if (!group_term.ok()) return group_term.status();
  directive.group_term_ = *group_term;
  auto manifest_revision = reader.ReadU64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  directive.population_manifest_revision_ = *manifest_revision;
  auto manifest_digest = ReadFixedArray<32>(reader);
  if (!manifest_digest.ok()) return manifest_digest.status();
  directive.population_manifest_digest_ = *manifest_digest;
  auto partition_replication_epoch = reader.ReadU64();
  if (!partition_replication_epoch.ok()) {
    return partition_replication_epoch.status();
  }
  directive.partition_replication_epoch_ = *partition_replication_epoch;
  auto kind = reader.ReadString(kMaxMetaDirectiveKindBytes);
  if (!kind.ok()) return kind.status();
  directive.kind_ = std::string(*kind);
  auto payload = reader.ReadString(kMaxMetaPayloadBytes);
  if (!payload.ok()) return payload.status();
  directive.payload_ = std::string(*payload);

  return directive;
}

}  // namespace keylane::meta
