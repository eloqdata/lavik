# Replace unreleased failover formats without compatibility

## Status

Accepted

The failover command, snapshot, Full Desired State, and Operation-result
layouts are not deployed to users. The simplified design therefore replaces
their schemas without a legacy decoder, dual write, or mixed-version
negotiation. Keylane-owned format markers stay at v1 while unreleased; schemas
are replaced in place rather than assigned additional development versions.
An equal marker does not establish compatibility with an earlier layout.
Only the resulting current binaries, state, and wire layouts are supported.
Old binaries and old persisted formats are outside the product contract.
