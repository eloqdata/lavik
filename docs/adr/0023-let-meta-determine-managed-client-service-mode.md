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

# Let Meta determine managed client service mode

## Status

Accepted for implementation in [#90](https://github.com/eloqdata/lavik/issues/90).

Meta's Committed State is the sole source of the managed Data cluster's
Single/Cluster Client Service Mode. Cluster creation declares that contract;
Data nodes obtain and install it rather than independently selecting a local
mode and asking Meta to check for agreement. Data still reports its actual
boot-local capabilities: receiving a declaration cannot make an incompatible
binary eligible to serve.

This removes duplicated deployment configuration at the cost of making mode
resolution part of managed startup. The committed declaration must be
available before creation waits for Data readiness. Data resolves the mode
before mode-dependent storage initialization and opens business admission only
after the normal readiness and authority checks. An unavailable Meta does not
authorize fallback to standalone service. This replaces the local mode
selection contract introduced by #88; native replication and HA remain shared
between client modes.

Meta connection configuration selects managed startup. Without it, a Data
node follows the existing standalone startup and recovery rules, including
when its data previously belonged to a Meta-managed deployment. Prior
management alone introduces no rejection, detach procedure, or additional
validation. Do not add a durable Meta-attachment marker to enforce deployment
configuration history. Existing population recovery identities and shutdown
proofs remain recovery metadata rather than a prohibition on standalone use.
Ordinary storage validation and existing incomplete-FULL fences still apply;
configuration changes do not bypass those checks.
