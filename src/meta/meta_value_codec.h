#pragma once

// Shared codecs for metadata value types that appear in more than one
// durable envelope. These functions own only the byte layout and decode-time
// field caps. Command validation and store-specific semantic invariants stay
// with their respective owners.

#include "absl/status/statusor.h"
#include "meta/meta_commands.h"
#include "meta/meta_encoding.h"

namespace keylane::meta {

void WriteActorContext(MetaWriter& writer, const ActorContext& actor);
absl::StatusOr<ActorContext> ReadActorContext(MetaReader& reader);

void WriteMetaPolicyReference(MetaWriter& writer,
                              const MetaPolicyReference& reference);
absl::StatusOr<MetaPolicyReference> ReadMetaPolicyReference(MetaReader& reader);

void WriteMetaEvidenceSummary(MetaWriter& writer,
                              const MetaEvidenceSummary& evidence);
absl::StatusOr<MetaEvidenceSummary> ReadMetaEvidenceSummary(MetaReader& reader);

}  // namespace keylane::meta
