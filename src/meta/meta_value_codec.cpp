#include "meta/meta_value_codec.h"

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

void WriteMetaPolicyReference(MetaWriter& writer,
                              const MetaPolicyReference& reference) {
  writer.WriteString(reference.policy_id_);
  writer.WriteU64(reference.version_);
}

absl::StatusOr<MetaPolicyReference> ReadMetaPolicyReference(
    MetaReader& reader) {
  auto policy_id = reader.ReadString(kMaxMetaPolicyIdBytes);
  if (!policy_id.ok()) return policy_id.status();
  auto version = reader.ReadU64();
  if (!version.ok()) return version.status();
  return MetaPolicyReference{std::string(*policy_id), *version};
}

void WriteMetaEvidenceSummary(MetaWriter& writer,
                              const MetaEvidenceSummary& evidence) {
  writer.WriteString(evidence.node_id_);
  WriteFixedArray(writer, evidence.boot_incarnation_);
  writer.WriteU64(evidence.group_term_);
  writer.WriteU64(evidence.population_manifest_id_);
  writer.WriteU64(evidence.replication_history_id_);
  WriteFixedArray(writer, evidence.operation_id_);
  WriteFixedArray(writer, evidence.kind_hash_);
}

absl::StatusOr<MetaEvidenceSummary> ReadMetaEvidenceSummary(
    MetaReader& reader) {
  auto node_id = reader.ReadString(kMetaNodeIdBytes);
  if (!node_id.ok()) return node_id.status();
  auto boot_incarnation = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!boot_incarnation.ok()) return boot_incarnation.status();
  auto group_term = reader.ReadU64();
  if (!group_term.ok()) return group_term.status();
  auto population_manifest_id = reader.ReadU64();
  if (!population_manifest_id.ok()) return population_manifest_id.status();
  auto replication_history_id = reader.ReadU64();
  if (!replication_history_id.ok()) return replication_history_id.status();
  auto operation_id = ReadFixedArray<16>(reader);
  if (!operation_id.ok()) return operation_id.status();
  auto kind_hash = ReadFixedArray<32>(reader);
  if (!kind_hash.ok()) return kind_hash.status();

  MetaEvidenceSummary evidence;
  evidence.node_id_ = std::string(*node_id);
  evidence.boot_incarnation_ = *boot_incarnation;
  evidence.group_term_ = *group_term;
  evidence.population_manifest_id_ = *population_manifest_id;
  evidence.replication_history_id_ = *replication_history_id;
  evidence.operation_id_ = *operation_id;
  evidence.kind_hash_ = *kind_hash;
  return evidence;
}

}  // namespace keylane::meta
