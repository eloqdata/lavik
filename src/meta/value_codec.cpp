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

#include "lavik/meta/value_codec.h"

#include <string>

#include "lavik/status_macros.h"

namespace lavik::meta {

void WriteActorContext(MetaWriter& writer, const ActorContext& actor) {
  writer.WriteString(actor.principal_);
  writer.WriteString(actor.readable_time_);
}

absl::StatusOr<ActorContext> ReadActorContext(MetaReader& reader) {
  ActorContext actor;
  LAVIK_ASSIGN_OR_RETURN(actor.principal_,
                         reader.ReadString(kMaxMetaPrincipalBytes));
  LAVIK_ASSIGN_OR_RETURN(actor.readable_time_,
                         reader.ReadString(kMaxMetaActorReadableTimeBytes));
  return actor;
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
  LAVIK_ASSIGN_OR_RETURN(directive.directive_id_, ReadFixedArray<16>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.attempt_id_, ReadFixedArray<16>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.recipient_node_id_,
                         reader.ReadString(kMetaNodeIdBytes));
  LAVIK_ASSIGN_OR_RETURN(directive.target_node_id_,
                         reader.ReadString(kMetaNodeIdBytes));
  LAVIK_ASSIGN_OR_RETURN(directive.target_boot_id_,
                         ReadFixedArray<kMetaBootIncarnationBytes>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.assignment_id_, ReadFixedArray<16>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.source_node_id_,
                         reader.ReadString(kMetaNodeIdBytes));
  LAVIK_ASSIGN_OR_RETURN(directive.source_assignment_id_,
                         ReadFixedArray<16>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.source_boot_id_,
                         ReadFixedArray<kMetaBootIncarnationBytes>(reader));
  LAVIK_ASSIGN_OR_RETURN(
      directive.source_replication_history_id_,
      ReadFixedArray<kMetaReplicationHistoryIdBytes>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.group_id_,
                         reader.ReadString(kMaxMetaGroupIdBytes));
  LAVIK_ASSIGN_OR_RETURN(directive.group_term_, reader.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(directive.population_manifest_revision_,
                         reader.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(directive.population_manifest_digest_,
                         ReadFixedArray<32>(reader));
  LAVIK_ASSIGN_OR_RETURN(directive.partition_replication_epoch_,
                         reader.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(directive.kind_,
                         reader.ReadString(kMaxMetaDirectiveKindBytes));
  LAVIK_ASSIGN_OR_RETURN(directive.payload_,
                         reader.ReadString(kMaxMetaPayloadBytes));

  return directive;
}

}  // namespace lavik::meta
