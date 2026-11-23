# Local object storage round trip

This test runs one Bycorf worker against RustFS (S3) and Azurite (Azure Blob).
The driver starts a disposable Docker container on a random loopback port
and creates the bucket or container. For RustFS, it passes fresh credentials
to the C++ [S3 client](../../include/lavik/object_store/s3_client.h) over
stdin. For Azurite, it signs short-lived request targets with the Azure SDK.
Both use Lavik's [HTTP client](../../include/lavik/net/http_client.h).

The worker writes and reads a binary payload, replaces it using its ETag, and
checks that a stale conditional PUT preserves the replacement. It lists by
prefix, deletes the object and verifies that a subsequent GET reports a
missing key. The S3 case also follows a continuation token, checks that
reserved characters and UTF-8 survive the round trip, excludes a key outside
the prefix, and verifies that RustFS rejects an invalid signature.

Use a Linux host with io_uring, Docker access, the Lavik build prerequisites,
and Python 3.10 or newer.
From the repository root, with submodules initialized:

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build --target lavik_http_client_test lavik_s3_protocol_test lavik_object_store_probe
ctest --test-dir build --output-on-failure -R '^lavik_(http_client|s3)_protocol$'
python3 -m venv /tmp/lavik-object-store-venv
/tmp/lavik-object-store-venv/bin/pip install boto3==1.44.7 azure-storage-blob==12.19.1
/tmp/lavik-object-store-venv/bin/python tests/object_store/roundtrip.py build/lavik_object_store_probe
```

The HTTP protocol test runs its delayed HTTPS peer on the same worker as the
client. The S3 protocol test checks a fixed SigV4 signature for temporary
credentials and exercises invalid and truncated listing responses. Both tests
run offline.

Pass `--provider s3` or `--provider azure` to run one service. Each run
generates its own credentials and removes its containers on exit.
