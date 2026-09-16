# Replace unreleased failover formats without compatibility

## Status

Accepted

The failover command, snapshot, Full Desired State, and Operation-result
layouts are not deployed to users. The simplified design therefore replaces
their schemas without a legacy decoder, dual write, or mixed-version
negotiation. A format version may advance to make an old durable or wire object
fail closed at its outer envelope; that discriminator does not create a
compatibility path. Only the resulting current binaries, state, and wire
layouts are supported. Old binaries and old persisted formats are outside the
product contract.
