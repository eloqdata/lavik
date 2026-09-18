<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Replace unreleased failover formats without compatibility

## Status

Accepted

Lavik has not yet shipped its first stable release. The failover command,
snapshot, Full Desired State, and Operation-result layouts are development
formats, so their schemas can be replaced without legacy decoders, dual writes,
or mixed-version negotiation. Lavik-owned format markers stay at v1 during
this pre-stable phase rather than gaining additional development versions.
An equal marker does not establish compatibility with an earlier layout.

During this phase, compatibility with earlier development binaries and
persisted formats is not required; only the current binaries, state, and wire
layouts are supported. This is a pre-stable development policy, not a permanent
exclusion of compatibility for stable releases.
