"""End-to-end: real server processes driven through the command-line client."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.integration.conftest import free_port

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
FAST = ["--gossip-interval", "0.2", "--fail-timeout", "1.5", "--hint-interval", "0.3",
        "--rpc-timeout", "0.5"]




def wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("localhost", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"port {port} never opened")


class ProcessCluster:
    def __init__(self, tmp: Path, size: int) -> None:
        self.tmp = tmp
        self.ports = [free_port() for _ in range(size)]
        self.procs: dict[int, subprocess.Popen] = {}
        for i in range(size):
            self.start(i)
        time.sleep(1.0)

    def start(self, i: int) -> None:
        seeds = [] if i == 0 else ["--seeds", f"localhost:{self.ports[0]}"]
        log = open(self.tmp / f"node-{i + 1}.log", "a")  # noqa: SIM115 - closed by the OS on exit
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-m", "dynamo.server", "--id", f"node-{i + 1}",
             "--port", str(self.ports[i]), "--data-dir", str(self.tmp / "data"), *seeds, *FAST],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, "GRPC_VERBOSITY": "ERROR"},
        )
        wait_port(self.ports[i])

    def kill(self, i: int) -> None:
        self.procs[i].kill()
        self.procs[i].wait(timeout=10)

    def cli(self, *args: str, seed: int = 0) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "dynamo.client", "--seeds", f"localhost:{self.ports[seed]}",
             *args],
            cwd=ROOT, capture_output=True, text=True, timeout=30,
        )

    def shutdown(self) -> list[int]:
        codes = []
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc in self.procs.values():
            codes.append(proc.wait(timeout=10))
        return codes


@pytest.fixture
def procs(tmp_path):
    cluster = ProcessCluster(tmp_path, 3)
    yield cluster
    cluster.shutdown()


def test_cli_roundtrip_and_exit_codes(procs):
    members = procs.cli("members")
    assert members.returncode == 0
    assert all(f"node-{i}" in members.stdout for i in (1, 2, 3))

    assert procs.cli("put", "greeting", "hello world").returncode == 0
    got = procs.cli("get", "greeting", seed=2)
    assert got.returncode == 0 and "'hello world'" in got.stdout

    missing = procs.cli("get", "nope")
    assert missing.returncode == 1 and "not found" in missing.stdout

    assert procs.cli("frobnicate").returncode == 2


def test_survives_sigkill_and_hands_off_after_restart(procs):
    procs.kill(2)
    time.sleep(2.0)                                   # > fail-timeout
    for i in range(5):
        assert procs.cli("put", f"k{i}", f"v{i}").returncode == 0
    procs.start(2)

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        log = (procs.tmp / "node-1.log").read_text() + (procs.tmp / "node-2.log").read_text()
        if "hinted handoff: delivered" in log:
            break
        time.sleep(0.2)
    else:
        pytest.fail("no hinted handoff happened after restart")
    for i in range(5):
        assert f"'v{i}'" in procs.cli("get", f"k{i}", seed=2).stdout


def test_graceful_shutdown_on_sigterm(tmp_path):
    cluster = ProcessCluster(tmp_path, 1)
    assert cluster.shutdown() == [0]


def test_duplicate_port_is_rejected(procs):
    dup = subprocess.run(
        [sys.executable, "-m", "dynamo.server", "--id", "dup", "--port", str(procs.ports[0]),
         "--data-dir", ""],
        cwd=ROOT, capture_output=True, text=True, timeout=20,
    )
    assert dup.returncode == 1
    assert "already in use" in dup.stderr
