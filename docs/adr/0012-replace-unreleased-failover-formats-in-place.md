# Replace unreleased failover formats in place

## Status

Accepted

The failover command, snapshot, Full Desired State, and Operation-result
layouts are not deployed to users. The simplified design therefore replaces
their current schemas in place without a schema-version bump, legacy decoder,
dual write, or mixed-version negotiation. Tests and fixtures move atomically
to the new layout, and rollout assumes homogeneous Meta and Data binaries.
