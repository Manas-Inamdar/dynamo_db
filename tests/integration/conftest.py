"""In-process cluster fixture: real gRPC servers on free localhost ports."""

from __future__ import annotations

import random
import socket
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from dynamo.client import DynamoClient
from dynamo.config import NodeConfig
from dynamo.node import DynamoNode
from dynamo.server import NodeServer

# Aggressive timings so failure detection and handoff happen in well under
# a second; production defaults are an order of magnitude slower.
FAST = {
    "rpc_timeout": 0.5,
    "gossip_interval": 0.1,
    "fail_timeout": 0.8,
    "remove_timeout": 30.0,
    "hint_interval": 0.2,
    "probe_dead_probability": 1.0,
    "vnodes": 32,
    "server_workers": 16,
    "fanout_workers": 16,
}


_HANDED_OUT: set[int] = set()


def free_port() -> int:
    """A currently unused port *below* the kernel's ephemeral range.

    Binding port 0 would return an ephemeral port, which any outgoing
    connection may take as its source port before the server binds it.
    """
    for _ in range(200):
        port = random.randint(20000, 32000)
        if port in _HANDED_OUT:
            # Two nodes drawing the same random port before either binds it
            # was a ~1 % per suite run "birthday" collision.
            continue
        with socket.socket() as s:
            try:
                s.bind(("localhost", port))
            except OSError:
                continue
            _HANDED_OUT.add(port)
            return port
    raise RuntimeError("no free port found")


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0, message: str = "") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout}s waiting for: {message or predicate}")


class Cluster:
    def __init__(self, data_dir: Path, size: int, **overrides: object) -> None:
        self.data_dir = data_dir
        self.overrides = {**FAST, **overrides}
        self.ids = [f"node-{i}" for i in range(1, size + 1)]
        self.ports = {nid: free_port() for nid in self.ids}
        self.servers: dict[str, NodeServer] = {}
        self._clients: list[DynamoClient] = []
        for nid in self.ids:
            self.start(nid)
        self.wait_converged()

    # ----------------------------------------------------------- lifecycle
    def config(self, node_id: str) -> NodeConfig:
        seeds = tuple(f"localhost:{self.ports[n]}" for n in self.ids if n != node_id)[:2]
        return NodeConfig(node_id=node_id, port=self.ports[node_id], seeds=seeds,
                          data_dir=str(self.data_dir), **self.overrides)

    def start(self, node_id: str) -> DynamoNode:
        for attempt in range(20):
            try:
                server = NodeServer(self.config(node_id)).start()
                break
            except OSError:
                # Another process grabbed the port between selection and bind
                # (or a just-stopped server is still releasing it).
                if attempt == 19:
                    raise
                time.sleep(0.1)
        self.servers[node_id] = server
        return server.node

    def stop(self, node_id: str) -> None:
        """Simulate a crash: the node disappears without saying goodbye."""
        self.servers.pop(node_id).stop(grace=0)

    def restart(self, node_id: str) -> DynamoNode:
        self.stop(node_id)
        return self.start(node_id)

    def shutdown(self) -> None:
        for client in self._clients:
            client.close()
        for node_id in list(self.servers):
            self.stop(node_id)

    # ------------------------------------------------------------- helpers
    def node(self, node_id: str) -> DynamoNode:
        return self.servers[node_id].node

    @property
    def running(self) -> list[str]:
        return list(self.servers)

    def address(self, node_id: str) -> str:
        return f"localhost:{self.ports[node_id]}"

    def client(self, *node_ids: str, discover: bool = False) -> DynamoClient:
        """A client pinned to the given coordinators (all running nodes by default)."""
        targets = node_ids or tuple(self.running)
        client = DynamoClient([self.address(n) for n in targets], timeout=3.0, discover=discover)
        self._clients.append(client)
        return client

    def wait_converged(self, timeout: float = 10.0) -> None:
        running = set(self.running)

        def converged() -> bool:
            return all(
                {m.node_id for m in self.node(n).membership.members() if m.healthy} >= running
                and set(self.node(n).ring.nodes()) >= running
                for n in running
            )

        wait_until(converged, timeout, "membership to converge")

    def wait_dead(self, node_id: str, timeout: float = 10.0) -> None:
        wait_until(lambda: all(not self.node(n).membership.is_healthy(node_id)
                               for n in self.running),
                   timeout, f"{node_id} to be detected as dead")

    def holders(self, key: str) -> set[str]:
        """Running nodes whose *primary storage* holds ``key``."""
        return {n for n in self.running if self.node(n).storage.get(key)}

    def values_on(self, node_id: str, key: str) -> list[str]:
        return sorted(v.value.decode() for v in self.node(node_id).storage.get(key))

    def preference(self, key: str, n: int = 3) -> list[str]:
        return self.node(self.running[0]).ring.preference_list(key, n)


@pytest.fixture
def make_cluster(tmp_path: Path) -> Iterator[Callable[..., Cluster]]:
    clusters: list[Cluster] = []

    def factory(size: int = 3, **overrides: object) -> Cluster:
        cluster = Cluster(tmp_path / f"cluster{len(clusters)}", size, **overrides)
        clusters.append(cluster)
        return cluster

    yield factory
    for cluster in clusters:
        cluster.shutdown()


@pytest.fixture
def cluster(make_cluster: Callable[..., Cluster]) -> Cluster:
    return make_cluster(3)
