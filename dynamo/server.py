"""Run a Dynamo node as a gRPC server.

Example (3-node cluster on one machine)::

    python -m dynamo.server --id node-1 --port 50051
    python -m dynamo.server --id node-2 --port 50052 --seeds localhost:50051
    python -m dynamo.server --id node-3 --port 50053 --seeds localhost:50051
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from concurrent import futures

import grpc

from dynamo.config import NodeConfig
from dynamo.node import DynamoNode
from dynamo.proto import dynamo_pb2_grpc
from dynamo.rpc import CHANNEL_OPTIONS

log = logging.getLogger("dynamo.server")


class NodeServer:
    """A :class:`DynamoNode` bound to a gRPC server."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.node = DynamoNode(config)
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(config.server_workers,
                                       thread_name_prefix=f"{config.node_id}-srv"),
            # gRPC enables SO_REUSEPORT by default, which lets a second process
            # silently bind the same port and steal half the traffic.
            options=(*CHANNEL_OPTIONS, ("grpc.so_reuseport", 0)),
        )
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(self.node, self.grpc_server)
        endpoint = f"{config.bind}:{config.port}"
        try:
            bound = self.grpc_server.add_insecure_port(endpoint)
        except RuntimeError as exc:
            bound, cause = 0, exc
        else:
            cause = None
        if bound == 0:
            self.node.stop()
            raise OSError(f"could not bind {endpoint} (already in use?)") from cause

    def start(self) -> NodeServer:
        self.grpc_server.start()
        self.node.start()
        log.info("[%s] serving on %s", self.config.node_id, self.config.address)
        return self

    def stop(self, grace: float = 0.5) -> None:
        self.node.stop()
        self.grpc_server.stop(grace).wait()
        log.info("[%s] stopped", self.config.node_id)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="dynamo-server", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--id", dest="node_id", required=True, help="unique node id, e.g. node-1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="localhost", help="address advertised to peers")
    p.add_argument("--bind", default="[::]", help="interface to listen on")
    p.add_argument("--seeds", default="",
                   help="comma-separated host:port of existing nodes to join through")
    p.add_argument("--data-dir", default="data",
                   help="root directory for persistent state ('' for in-memory)")
    p.add_argument("-n", "--replicas", dest="n", type=int, default=3, help="replication factor N")
    p.add_argument("-r", "--read-quorum", dest="r", type=int, default=2, help="read quorum R")
    p.add_argument("-w", "--write-quorum", dest="w", type=int, default=2, help="write quorum W")
    p.add_argument("--vnodes", type=int, default=64, help="virtual nodes per physical node")
    p.add_argument("--fsync", action="store_true", help="fsync every write to disk")
    p.add_argument("--rpc-timeout", type=float, default=1.0)
    p.add_argument("--gossip-interval", type=float, default=1.0)
    p.add_argument("--fail-timeout", type=float, default=5.0)
    p.add_argument("--remove-timeout", type=float, default=120.0)
    p.add_argument("--hint-interval", type=float, default=1.0)
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> NodeConfig:
    return NodeConfig(
        node_id=args.node_id,
        port=args.port,
        host=args.host,
        bind=args.bind,
        seeds=tuple(s.strip() for s in args.seeds.split(",") if s.strip()),
        data_dir=args.data_dir or None,
        n=args.n,
        r=args.r,
        w=args.w,
        vnodes=args.vnodes,
        fsync=args.fsync,
        rpc_timeout=args.rpc_timeout,
        gossip_interval=args.gossip_interval,
        fail_timeout=args.fail_timeout,
        remove_timeout=args.remove_timeout,
        hint_interval=args.hint_interval,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s.%(msecs)03d %(levelname)-5s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        config = config_from_args(args)
    except ValueError as exc:
        print(f"invalid configuration: {exc}", file=sys.stderr)
        return 2
    if not config.strongly_consistent_quorums:
        log.warning("R + W <= N: reads may miss the latest acknowledged write")

    try:
        server = NodeServer(config).start()
    except OSError as exc:
        log.error("%s", exc)
        return 1
    stopped = threading.Event()

    def _shutdown(signum: int, _frame: object) -> None:
        log.info("[%s] received %s, shutting down", config.node_id, signal.Signals(signum).name)
        stopped.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    stopped.wait()
    server.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
