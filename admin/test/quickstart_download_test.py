#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
"""Exercise the actual downloader with verified local TLS and injected handshakes."""

import hashlib
import http.server
import io
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tarfile
import tempfile
import threading
import unittest


@unittest.skipUnless(shutil.which("flock"), "downloader runs in the Linux runtime")
class QuickstartDownload(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.release = self.root / "release"
        self.existing = self.root / "existing"
        self.existing.mkdir()
        self.arch = (
            "aarch64" if os.uname().machine in ("aarch64", "arm64") else "x86_64"
        )
        self.archive = f"lavik-nightly-linux-{self.arch}-minimal.tar.gz"
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode="w:gz") as archive:
            for name, data in {
                "lavik": b"#!/bin/sh\necho --meta-seed\n",
                "lavik-meta": b"#!/bin/sh\nexit 0\n",
                "lavik-ctl": b"#!/bin/sh\nexit 0\n",
                "runtime/bin/node": b"#!/bin/sh\nexit 0\n",
                "REVISION": b"test-revision\n",
                "VERSION": b"nightly\n",
            }.items():
                entry = tarfile.TarInfo("package/" + name)
                entry.size = len(data)
                entry.mode = 0o755
                archive.addfile(entry, io.BytesIO(data))
        package = payload.getvalue()
        self.content = {
            "/" + self.archive: package,
            "/" + self.archive + ".sha256": (
                hashlib.sha256(package).hexdigest() + "  " + self.archive + "\n"
            ).encode(),
        }
        cert, key = self.root / "cert.pem", self.root / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        test = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = test.content[self.path]
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        class Server(http.server.HTTPServer):
            def get_request(self):
                connection, address = super().get_request()
                test.attempts += 1
                if test.failures:
                    test.failures -= 1
                    connection.close()
                    raise ConnectionAbortedError("Simulated reset during TLS handshake")
                return context.wrap_socket(connection, server_side=True), address

        self.attempts = 0
        self.failures = 0
        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        binary = self.root / "bin"
        binary.mkdir()
        curl = binary / "curl"
        # Only the fixture URL and filesystem roots change. Curl itself performs
        # the TLS handshake, retries, output truncation and certificate checking.
        curl.write_text(
            "#!/usr/bin/env python3\nimport os,sys\n"
            "args=sys.argv[1:]\n"
            "args=[os.environ['TEST_ORIGIN']+'/'+a.rsplit('/',1)[1] if a.startswith('https://github.com/') else a for a in args]\n"
            "os.execv(os.environ['TEST_CURL'], [os.environ['TEST_CURL'], *args])\n"
        )
        curl.chmod(0o755)
        script = (Path(__file__).parents[1] / "quickstart/download.sh").read_text()
        self.script = self.root / "download.sh"
        self.script.write_text(
            script.replace("release_root=/release", f"release_root={self.release}")
            .replace("/existing", str(self.existing))
            .replace("/bundled", str(self.root / "bundled"))
        )
        self.env = {
            **os.environ,
            "PATH": str(binary) + ":" + os.environ["PATH"],
            "TEST_CURL": shutil.which("curl"),
            "TEST_ORIGIN": f"https://127.0.0.1:{self.server.server_port}",
            "CURL_CA_BUNDLE": str(cert),
            "NO_PROXY": "127.0.0.1",
            "no_proxy": "127.0.0.1",
            "LAVIK_QUICKSTART_VERSION": "nightly",
        }

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def run_download(self):
        return subprocess.run(
            ["bash", str(self.script)],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_tls_handshake_failure_is_retried_and_verified_release_is_published(self):
        self.failures = 1
        result = self.run_download()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("curl: (35)", result.stderr)
        self.assertEqual(self.attempts, 3)  # failed handshake, checksum, archive
        self.assertEqual(
            (self.release / "current/REVISION").read_text(), "test-revision\n"
        )
        self.assertIn("Verified Lavik nightly", result.stdout)
        result = self.run_download()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Reusing Lavik", result.stdout)
        self.assertEqual(self.attempts, 3, "retained release must not contact GitHub")

    def test_packaged_binaries_install_without_any_network_request(self):
        bundled = self.root / "bundled"
        bundled.mkdir()
        with tarfile.open(
            fileobj=io.BytesIO(self.content["/" + self.archive])
        ) as archive:
            for member in archive:
                target = bundled / member.name.removeprefix("package/")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
                target.chmod(member.mode)
        self.env["LAVIK_QUICKSTART_VERSION"] = "bundled"
        result = self.run_download()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.attempts, 0)
        self.assertIn("Installed bundled Lavik", result.stdout)
        self.assertTrue((self.release / "current/.quickstart-bundle.sha256").is_file())
        result = self.run_download()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Reusing Lavik bundled", result.stdout)
        self.assertEqual(self.attempts, 0)

    def test_persistent_tls_failure_is_bounded_and_does_not_publish(self):
        self.failures = 100
        result = self.run_download()
        self.assertEqual(result.returncode, 35, result.stderr)
        self.assertEqual(self.attempts, 4)
        self.assertIn("Docker Desktop proxy/VPN", result.stderr)
        self.assertFalse((self.release / "current").exists())
        self.assertEqual(list(self.release.glob(".download-*")), [])

    def test_corrupt_download_is_rejected_without_replacing_a_release(self):
        self.content["/" + self.archive] = b"corrupt archive"
        result = self.run_download()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Release verification failed", result.stderr)
        self.assertFalse((self.release / "current").exists())


if __name__ == "__main__":
    unittest.main()
