#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic follower-to-leader discovery regression (Debug Meta required).

Usage: gate_meta_follower_retry.py /path/to/lavik-meta /path/to/lavik [workdir]
"""

import os
import sys

import gate_data_control as D
import harness as H


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__)
        return 2
    work_argv = [sys.argv[0], sys.argv[1]]
    if len(sys.argv) == 4:
        work_argv.append(sys.argv[3])
    workdir, keep = H.make_workdir(work_argv, "meta_follower_retry_")
    variable = "LAVIK_TEST_META_FORCE_FOLLOWER_FILE"
    previous = os.environ.get(variable)
    hold = os.path.join(workdir, "force-follower")
    os.environ[variable] = hold
    try:
        D.run_plaintext(sys.argv[1], sys.argv[2], workdir, follower_retry_hold=hold)
        H.log("PASS: previously visited follower supplies replacement FDS")
        return 0
    except Exception as exc:  # noqa: BLE001 - the shared gate prints process logs
        print(f"[gate-meta-follower-retry] FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        if previous is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = previous
        H.cleanup(workdir, keep)


if __name__ == "__main__":
    H.set_tag("gate-meta-follower-retry")
    sys.exit(main())
