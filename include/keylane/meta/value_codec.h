#pragma once

// Shared codecs for metadata value types that appear in more than one
// durable envelope. These functions own only the byte layout and decode-time
// field caps. Command validation and store-specific semantic invariants stay
// with their respective owners.

#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"

namespace keylane::meta {

void WriteActorContext(MetaWriter& writer, const ActorContext& actor);
absl::StatusOr<ActorContext> ReadActorContext(MetaReader& reader);

void WriteMetaDirectiveSpec(MetaWriter& writer,
                            const MetaDirectiveSpec& directive);
absl::StatusOr<MetaDirectiveSpec> ReadMetaDirectiveSpec(MetaReader& reader);

}  // namespace keylane::meta
