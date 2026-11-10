"""Node configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NodeConfig:
    """Everything a node needs to start. All durations are in seconds."""

    node_id: str
    port: int
    host: str = "localhost"            # address advertised to peers
    bind: str = "[::]"                 # interface the gRPC server listens on
    seeds: tuple[str, ...] = ()        # host:port of nodes to join through
    data_dir: str | None = "data"      # None => in-memory only

    # Quorum parameters (Dynamo's N, R, W).
    n: int = 3
    r: int = 2
    w: int = 2

    vnodes: int = 64                   # virtual nodes per physical node
    fsync: bool = False                # fsync every write (durability vs latency)

    rpc_timeout: float = 1.0           # per replica RPC
    gossip_interval: float = 1.0
    fail_timeout: float = 5.0          # no heartbeat for this long => dead
    remove_timeout: float = 120.0      # dead for this long => drop from ring
    hint_interval: float = 1.0         # how often to retry hinted handoff
    hint_batch_size: int = 128         # keys per DeliverHints RPC
    probe_dead_probability: float = 0.25  # chance a gossip round probes a dead node

    server_workers: int = 32           # gRPC server thread pool
    fanout_workers: int = 64           # coordinator -> replica RPC pool

    def __post_init__(self) -> None:
        if not self.node_id:
            raise ValueError("node_id must not be empty")
        if not 0 < self.port < 65536:
            raise ValueError(f"invalid port {self.port}")
        if self.n < 1:
            raise ValueError("N must be >= 1")
        if not 1 <= self.r <= self.n:
            raise ValueError(f"R must be in [1, N]; got R={self.r}, N={self.n}")
        if not 1 <= self.w <= self.n:
            raise ValueError(f"W must be in [1, N]; got W={self.w}, N={self.n}")
        if self.vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        for name in ("rpc_timeout", "gossip_interval", "fail_timeout",
                     "remove_timeout", "hint_interval"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.fail_timeout <= self.gossip_interval:
            raise ValueError("fail_timeout must exceed gossip_interval")
        if not 0.0 <= self.probe_dead_probability <= 1.0:
            raise ValueError("probe_dead_probability must be in [0, 1]")

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def strongly_consistent_quorums(self) -> bool:
        """R + W > N guarantees read and write quorums overlap."""
        return self.r + self.w > self.n
