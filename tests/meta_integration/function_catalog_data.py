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

"""Binary catalog snapshots shared by Function and real HA process gates."""

from gate_native_replication import Client, H


def library(name, value):
    return (
        f"#!lua name={name}\n"
        f"redis.register_function('{name}_value', function(keys, args) "
        f"return '{value}' end)"
    )


def snapshot(client):
    return (
        client.call("FUNCTION", "DUMP", decode=False),
        client.call("FUNCTION", "LIST", "WITHCODE"),
    )


def node_snapshot(node):
    client = Client(node)
    try:
        return snapshot(client)
    finally:
        client.close()


def library_code(node, name):
    for fields in node_snapshot(node)[1]:
        item = dict(zip(fields[::2], fields[1::2]))
        if item["library_name"] == name:
            return item["library_code"]
    raise H.Failure(f"missing library {name}")
