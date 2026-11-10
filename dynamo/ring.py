"""Consistent-hash ring with virtual nodes.

Every physical node owns ``vnodes`` tokens on a 160-bit SHA-1 ring. A key is
stored on the first ``N`` *distinct* physical nodes found by walking the ring
clockwise from the key's hash (its *preference list*).

Virtual nodes smooth out the load imbalance that a single token per node
would cause and spread the load of a failed node across many successors
(see experiments/E5_vnode_balance).
"""

from __future__ import annotations

import bisect
import hashlib
import logging
import threading

log = logging.getLogger(__name__)


def hash_key(data: str) -> int:
    """Position of ``data`` on the ring (160-bit SHA-1 as an integer)."""
    return int.from_bytes(hashlib.sha1(data.encode("utf-8")).digest(), "big")


class HashRing:
    """Thread-safe consistent-hash ring.

    Membership changes (gossip thread) and lookups (request threads) happen
    concurrently, so all access to the token list goes through a lock. The
    token list is rebuilt copy-on-write so readers never see a half-updated
    ring.
    """

    def __init__(self, vnodes: int = 64) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._lock = threading.RLock()
        self._tokens: list[tuple[int, str]] = []   # sorted (token, node_id)
        self._addresses: dict[str, str] = {}        # node_id -> host:port

    # ------------------------------------------------------------- membership
    def add_node(self, node_id: str, address: str) -> bool:
        """Add a node (or update its address). Returns True if it was new."""
        with self._lock:
            if node_id in self._addresses:
                self._addresses[node_id] = address
                return False
            new_tokens = [(hash_key(f"{node_id}#vnode-{i}"), node_id) for i in range(self.vnodes)]
            self._tokens = sorted(self._tokens + new_tokens)
            self._addresses[node_id] = address
        log.debug("ring: added %s (%d vnodes)", node_id, self.vnodes)
        return True

    def remove_node(self, node_id: str) -> bool:
        """Remove a node and its tokens. Returns True if it was present."""
        with self._lock:
            if node_id not in self._addresses:
                return False
            self._tokens = [t for t in self._tokens if t[1] != node_id]
            del self._addresses[node_id]
        log.debug("ring: removed %s", node_id)
        return True

    # ---------------------------------------------------------------- queries
    def __contains__(self, node_id: object) -> bool:
        with self._lock:
            return node_id in self._addresses

    def __len__(self) -> int:
        with self._lock:
            return len(self._addresses)

    def nodes(self) -> dict[str, str]:
        """Snapshot of ``node_id -> address`` for every node on the ring."""
        with self._lock:
            return dict(self._addresses)

    def address_of(self, node_id: str) -> str | None:
        with self._lock:
            return self._addresses.get(node_id)

    def preference_list(self, key: str, n: int | None = None) -> list[str]:
        """Distinct physical nodes responsible for ``key``, in ring order.

        With ``n=None`` every node is returned; the coordinator uses the full
        walk to find stand-ins beyond the first N when replicas are down.
        """
        with self._lock:
            tokens = self._tokens
            limit = len(self._addresses) if n is None else min(n, len(self._addresses))
        if not tokens or limit == 0:
            return []

        start = bisect.bisect_left(tokens, (hash_key(key), ""))
        result: list[str] = []
        seen: set[str] = set()
        size = len(tokens)
        for offset in range(size):
            node_id = tokens[(start + offset) % size][1]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
                if len(result) == limit:
                    break
        return result

    def primary(self, key: str) -> str | None:
        owners = self.preference_list(key, 1)
        return owners[0] if owners else None
