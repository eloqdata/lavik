# Replace unreleased failover formats without compatibility

## Status

Accepted

Keylane has not yet shipped its first stable release. The failover command,
snapshot, Full Desired State, and Operation-result layouts are development
formats, so their schemas can be replaced without legacy decoders, dual writes,
or mixed-version negotiation. Keylane-owned format markers stay at v1 during
this pre-stable phase rather than gaining additional development versions.
An equal marker does not establish compatibility with an earlier layout.

During this phase, compatibility with earlier development binaries and
persisted formats is not required; only the current binaries, state, and wire
layouts are supported. This is a pre-stable development policy, not a permanent
exclusion of compatibility for stable releases.
