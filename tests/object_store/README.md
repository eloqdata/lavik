# Local object storage round trip

This test runs one Bycorf worker against RustFS (S3) and Azurite (Azure Blob).
The driver starts a disposable Docker container on a random loopback port,
creates the bucket or container, and signs short-lived request targets with
the provider SDK. The C++ worker sends the object requests with Bycorf TCP on
io_uring and parses the HTTP responses with Boost.Beast.

The worker writes and reads a binary payload, replaces it using its ETag, and
checks that a stale conditional PUT returns 412 and preserves the replacement.
It lists the object by prefix, deletes it and checks that GET returns 404.

Use a Linux host with io_uring, Docker access, the Lavik build prerequisites,
Boost.Beast headers (`libboost-dev` on Ubuntu) and Python 3.10 or newer.
From the repository root, with submodules initialized:

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build --target lavik_object_store_probe
python3 -m venv /tmp/lavik-object-store-venv
/tmp/lavik-object-store-venv/bin/pip install boto3==1.44.7 azure-storage-blob==12.19.1
/tmp/lavik-object-store-venv/bin/python tests/object_store/roundtrip.py build/lavik_object_store_probe
```

Pass `--provider s3` or `--provider azure` to run one service. Each run
generates its own credentials and removes its containers on exit.
