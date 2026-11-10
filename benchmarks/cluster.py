"""Launch a cluster of real server processes for experiments.

``impl="new"`` runs this repository's ``dynamo.server``. ``impl="original"``
runs the pre-rewrite ``server.py`` from a git worktree of commit
``ORIGINAL_COMMIT`` so both versions are measured on the same machine with
the same load generator.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORIGINAL_COMMIT = "eebab5b"


def port_open(port: int) -> bool:
    try:
        socket.create_connection(("localhost", port), 0.2).close()
        return True
    except OSError:
        return False


def ensure_original_worktree(path: Path) -> Path:
    if not (path / "server.py").exists():
        subprocess.run(["git", "worktree", "add", "-f", str(path), ORIGINAL_COMMIT],
                       cwd=ROOT, check=True, capture_output=True)
    return path


class ProcessCluster:
    def __init__(self, size: int, workdir: Path, *, impl: str = "new", base_port: int = 26051,
                 server_args: list[str] | None = None, original_dir: Path | None = None) -> None:
        if impl not in ("new", "original"):
            raise ValueError(impl)
        self.size, self.impl, self.base_port = size, impl, base_port
        self.workdir = workdir
        self.server_args = server_args or []
        self.original_dir = original_dir
        self.procs: dict[int, subprocess.Popen] = {}
        workdir.mkdir(parents=True, exist_ok=True)
        if impl == "original":
            assert original_dir is not None
            shutil.rmtree(original_dir / "data", ignore_errors=True)  # it writes to ./data
        else:
            shutil.rmtree(workdir / "data", ignore_errors=True)

    @property
    def addresses(self) -> list[str]:
        return [f"localhost:{self.base_port + i}" for i in range(self.size)]

    def _cmd(self, i: int) -> tuple[list[str], Path]:
        port = self.base_port + i
        seed = f"localhost:{self.base_port}"
        if self.impl == "original":
            cmd = [sys.executable, "server.py", f"node-{i + 1}", str(port)]
            if i:
                cmd += ["--seed", seed]
            return cmd, self.original_dir
        cmd = [sys.executable, "-m", "dynamo.server", "--id", f"node-{i + 1}", "--port", str(port),
               "--data-dir", str(self.workdir / "data"), *self.server_args]
        if i:
            cmd += ["--seeds", seed]
        return cmd, ROOT

    def start(self, i: int) -> None:
        port = self.base_port + i
        if port_open(port):
            raise RuntimeError(f"port {port} already in use")
        cmd, cwd = self._cmd(i)
        log = open(self.workdir / f"node-{i + 1}.log", "a")  # noqa: SIM115
        self.procs[i] = subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                                         env={**os.environ, "GRPC_VERBOSITY": "ERROR",
                                              "PYTHONUNBUFFERED": "1"})
        deadline = time.monotonic() + 15
        while not port_open(port):
            if self.procs[i].poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(f"node-{i + 1} failed to start; see {log.name}")
            time.sleep(0.05)

    def start_all(self, settle: float = 3.0) -> ProcessCluster:
        for i in range(self.size):
            self.start(i)
            if self.impl == "original":
                time.sleep(1.0)   # its join is a single fire-and-forget RPC
        time.sleep(settle)
        return self

    def kill(self, i: int, sig: int = signal.SIGKILL) -> None:
        proc = self.procs.pop(i)
        proc.send_signal(sig)
        proc.wait(timeout=15)

    def stop_all(self) -> None:
        for i in list(self.procs):
            self.kill(i, signal.SIGTERM if self.impl == "new" else signal.SIGKILL)

    def __enter__(self) -> ProcessCluster:
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop_all()
