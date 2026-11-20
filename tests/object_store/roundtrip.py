#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise the Bycorf probe against disposable loopback object services."""

import argparse
import base64
import os
import secrets
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection, HTTPException
from urllib.parse import urlsplit

import boto3
from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from botocore.config import Config


@contextmanager
def service(image, port, credentials, ready_path, ready_statuses, *command):
    # Only the ID returned for this run may be removed. Docker chooses a free
    # loopback port and --rm discards the anonymous data volume on shutdown.
    args = ["docker", "run", "--rm", "-d", "-p", f"127.0.0.1::{port}"]
    for name in credentials:
        args.extend(["-e", name])
    ident = subprocess.check_output(
        [*args, image, *command], env=os.environ | credentials, text=True
    ).strip()
    try:
        binding = subprocess.check_output(
            ["docker", "port", ident, f"{port}/tcp"], text=True
        ).strip()
        local_port = int(binding.rsplit(":", 1)[1])
        deadline = time.monotonic() + 30
        while True:
            connection = HTTPConnection("127.0.0.1", local_port, timeout=1)
            try:
                connection.request("GET", ready_path)
                if connection.getresponse().status in ready_statuses:
                    break
            except (OSError, HTTPException):
                pass
            finally:
                connection.close()
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{image} did not become ready")
            time.sleep(0.1)
        yield local_port
    finally:
        subprocess.run(
            ["docker", "rm", "-f", ident], check=True, stdout=subprocess.DEVNULL
        )


def run_probe(binary, provider, port, urls):
    targets = []
    for url in urls:
        parsed = urlsplit(url)
        if parsed.hostname != "127.0.0.1" or parsed.port != port or not parsed.query:
            raise ValueError("expected signed loopback URL")
        targets.append(f"{parsed.path}?{parsed.query}")
    subprocess.run(
        [binary, provider, str(port)],
        input="\n".join(targets) + "\n",
        text=True,
        check=True,
        timeout=45,
    )
    print(
        f"{provider}: binary PUT/GET, overwrite, stale If-Match, prefix listing and DELETE passed",
        flush=True,
    )


def rustfs(binary):
    key = secrets.token_hex(12).upper()
    secret = secrets.token_hex(24)
    with service(
        "rustfs/rustfs:1.0.0",
        9000,
        {"RUSTFS_ACCESS_KEY": key, "RUSTFS_SECRET_KEY": secret},
        "/health/ready",
        {200},
        "/data",
    ) as port:
        client = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{port}",
            region_name="us-east-1",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                proxies={},
                connect_timeout=2,
                read_timeout=5,
            ),
        )
        bucket = "lavik-probe"
        client.create_bucket(Bucket=bucket)
        urls = [
            client.generate_presigned_url(
                operation,
                Params={"Bucket": bucket, "Key": "blocks/1001_8192"},
                ExpiresIn=300,
            )
            for operation in ("put_object", "get_object", "delete_object")
        ]
        urls.append(
            client.generate_presigned_url(
                "list_objects_v2",
                Params={"Bucket": bucket, "Prefix": "blocks/"},
                ExpiresIn=300,
            )
        )
        run_probe(binary, "s3", port, urls)


def azurite(binary):
    account = "lavikprobe"
    key = base64.b64encode(secrets.token_bytes(32)).decode()
    with service(
        "mcr.microsoft.com/azure-storage/azurite:3.35.0",
        10000,
        {"AZURITE_ACCOUNTS": f"{account}:{key}"},
        "/lavikprobe/probe/uncreated",
        {403, 404},
        "azurite-blob",
        "--blobHost",
        "0.0.0.0",
    ) as port:
        client = BlobServiceClient(
            f"http://127.0.0.1:{port}/{account}",
            credential=key,
            api_version="2023-11-03",
            use_env_settings=False,
        )
        container = client.create_container("lavik-probe")
        blob = container.get_blob_client("blocks/1001_8192")
        sas = generate_blob_sas(
            account,
            container.container_name,
            blob.blob_name,
            account_key=key,
            permission=BlobSasPermissions(read=True, write=True, delete=True),
            expiry=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        listing = generate_container_sas(
            account,
            container.container_name,
            account_key=key,
            permission=ContainerSasPermissions(list=True),
            expiry=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        list_url = (
            f"{container.url}?restype=container&comp=list&prefix=blocks%2F&{listing}"
        )
        run_probe(binary, "azure", port, [f"{blob.url}?{sas}"] * 3 + [list_url])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", help="path to lavik_object_store_probe")
    parser.add_argument("--provider", choices=("s3", "azure", "both"), default="both")
    args = parser.parse_args()
    if args.provider in ("s3", "both"):
        rustfs(args.binary)
    if args.provider in ("azure", "both"):
        azurite(args.binary)
