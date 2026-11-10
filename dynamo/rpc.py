"""Shared gRPC channels between nodes.

Opening a channel costs a TCP (and HTTP/2) handshake, so channels are cached
per address and reused for every RPC. gRPC's default reconnect backoff grows
to 120 s, which would hide a recovered node from its peers for minutes; the
options below cap it so recovery is noticed within a couple of seconds.

By default gRPC Python also shares connections ("subchannels") process-wide
between channels with the same target and options, so a brand-new channel
can inherit another channel's reconnect backoff and fail instantly with
"connection refused" against a server that is up. A local subchannel pool
makes every channel own its connections, which is what ``reset`` relies on.
"""

from __future__ import annotations

import threading

import grpc

from dynamo.proto import dynamo_pb2_grpc

CHANNEL_OPTIONS: tuple[tuple[str, int], ...] = (
    ("grpc.use_local_subchannel_pool", 1),
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 1000),
    ("grpc.keepalive_time_ms", 10_000),
    ("grpc.keepalive_timeout_ms", 5_000),
    ("grpc.max_send_message_length", 64 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
)


class ChannelPool:
    """Thread-safe cache of ``address -> (channel, stub)``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[grpc.Channel, dynamo_pb2_grpc.DynamoServiceStub]] = {}
        self._closed = False

    def stub(self, address: str) -> dynamo_pb2_grpc.DynamoServiceStub:
        with self._lock:
            if self._closed:
                raise RuntimeError("channel pool is closed")
            entry = self._entries.get(address)
            if entry is None:
                channel = grpc.insecure_channel(address, options=CHANNEL_OPTIONS)
                entry = (channel, dynamo_pb2_grpc.DynamoServiceStub(channel))
                self._entries[address] = entry
            return entry[1]

    def reset(self, address: str) -> None:
        """Drop the cached channel so the next RPC reconnects immediately.

        Used when a peer comes back: its old channel may still be waiting
        out a reconnect backoff and would fail the first RPCs.
        """
        with self._lock:
            entry = self._entries.pop(address, None)
        if entry is not None:
            entry[0].close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            entries, self._entries = self._entries, {}
        for channel, _ in entries.values():
            channel.close()
