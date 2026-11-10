"""Durable per-node storage of versioned values.

Design
------
* The live versions of each key are kept in memory (``key -> [versions]``)
  and mirrored to disk as **one file per key** containing a
  :class:`StoredRecord` protobuf.
* Every write goes through :func:`dynamo.versioning.reconcile`, so obsolete
  versions are discarded as soon as a newer one arrives. Storage therefore
  grows with the number of keys, not with the number of writes.
* Files are replaced atomically (write to a temp file, then ``os.replace``),
  so a crash mid-write leaves either the old or the new record, never a torn
  one. ``fsync=True`` additionally survives power loss at a latency cost
  (see experiments/E6_fsync).
* Read-modify-write cycles are serialized per key using lock striping, so
  writes to different keys proceed in parallel.

``directory=None`` gives a purely in-memory store (used for tests).
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import urllib.parse
from collections.abc import Iterable, Iterator
from pathlib import Path

from dynamo.proto.dynamo_pb2 import StoredRecord, ValueWithContext
from dynamo.versioning import full_clock, reconcile, same_versions, version_key

log = logging.getLogger(__name__)

_RECORD_SUFFIX = ".rec"
_TMP_SUFFIX = ".tmp"
_LOCK_STRIPES = 64


class Storage:
    """Versioned key-value store backed by one file per key."""

    def __init__(self, directory: str | os.PathLike[str] | None, *, fsync: bool = False) -> None:
        self.directory = Path(directory) if directory is not None else None
        self.fsync = fsync
        self._data: dict[str, list[ValueWithContext]] = {}
        self._locks = [threading.Lock() for _ in range(_LOCK_STRIPES)]
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self._load()

    # ----------------------------------------------------------------- public
    def get(self, key: str) -> list[ValueWithContext]:
        """Live versions of ``key`` (empty list if absent)."""
        # Lists are replaced, never mutated, so this read needs no lock.
        return list(self._data.get(key, ()))

    def put(self, key: str, version: ValueWithContext) -> bool:
        """Merge one version into ``key``. Returns True if state changed."""
        return self.put_many(key, (version,))

    def put_many(self, key: str, versions: Iterable[ValueWithContext]) -> bool:
        """Merge several versions into ``key``. Returns True if state changed."""
        incoming = list(versions)
        if not incoming:
            return False
        with self._lock_for(key):
            current = self._data.get(key, [])
            merged = reconcile([*current, *incoming])
            if same_versions(current, merged):
                return False
            self._persist(key, merged)
            self._data[key] = merged
            return True

    def discard(self, key: str, versions: Iterable[ValueWithContext]) -> None:
        """Remove exactly ``versions`` from ``key`` (used after hint delivery).

        Versions that arrived after ``versions`` were read are kept, which
        makes "deliver, then discard what was delivered" race-free.
        """
        drop = {version_key(v) for v in versions}
        with self._lock_for(key):
            current = self._data.get(key)
            if current is None:
                return
            remaining = [v for v in current if version_key(v) not in drop]
            if len(remaining) == len(current):
                return
            if remaining:
                self._persist(key, remaining)
                self._data[key] = remaining
            else:
                self._delete_file(key)
                del self._data[key]

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def items(self) -> Iterator[tuple[str, list[ValueWithContext]]]:
        for key in self.keys():
            versions = self.get(key)
            if versions:
                yield key, versions

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def max_counter(self, node_id: str) -> int:
        """Highest clock counter for ``node_id`` across all stored versions."""
        best = 0
        for _, versions in self.items():
            for v in versions:
                best = max(best, full_clock(v).get(node_id))
        return best

    # --------------------------------------------------------------- internal
    def _lock_for(self, key: str) -> threading.Lock:
        return self._locks[hash(key) % _LOCK_STRIPES]

    def _path_for(self, key: str) -> Path:
        assert self.directory is not None
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.directory / f"{digest}{_RECORD_SUFFIX}"

    def _persist(self, key: str, versions: list[ValueWithContext]) -> None:
        if self.directory is None:
            return
        path = self._path_for(key)
        tmp = path.with_name(f"{path.name}.{threading.get_ident()}{_TMP_SUFFIX}")
        payload = StoredRecord(key=key, versions=versions).SerializeToString()
        with open(tmp, "wb") as fh:
            fh.write(payload)
            if self.fsync:
                fh.flush()
                os.fsync(fh.fileno())
        os.replace(tmp, path)

    def _delete_file(self, key: str) -> None:
        if self.directory is None:
            return
        try:
            self._path_for(key).unlink()
        except FileNotFoundError:
            pass

    def _load(self) -> None:
        assert self.directory is not None
        loaded = 0
        for entry in self.directory.iterdir():
            if entry.name.endswith(_TMP_SUFFIX):
                # Leftover from a crash between write and rename.
                entry.unlink(missing_ok=True)
                continue
            if not entry.name.endswith(_RECORD_SUFFIX):
                continue
            try:
                record = StoredRecord.FromString(entry.read_bytes())
            except Exception:  # noqa: BLE001 - corrupt file must not stop startup
                log.warning("storage: skipping unreadable record %s", entry)
                continue
            versions = reconcile(record.versions)
            if versions:
                self._data[record.key] = versions
                loaded += 1
        if loaded:
            log.info("storage: loaded %d keys from %s", loaded, self.directory)


class HintStore:
    """Writes held on behalf of unreachable replicas, grouped by target.

    Each target gets its own :class:`Storage`, so hints for the same key are
    reconciled (a newer hint supersedes an older one) and survive a restart
    of the node holding them.
    """

    def __init__(self, directory: str | os.PathLike[str] | None, *, fsync: bool = False) -> None:
        self.directory = Path(directory) if directory is not None else None
        self.fsync = fsync
        self._targets: dict[str, Storage] = {}
        self._lock = threading.Lock()
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            for sub in self.directory.iterdir():
                if sub.is_dir():
                    target = urllib.parse.unquote(sub.name)
                    store = Storage(sub, fsync=fsync)
                    if len(store):
                        self._targets[target] = store

    def _store_for(self, target: str) -> Storage:
        with self._lock:
            store = self._targets.get(target)
            if store is None:
                sub = (
                    self.directory / urllib.parse.quote(target, safe="")
                    if self.directory is not None
                    else None
                )
                store = Storage(sub, fsync=self.fsync)
                self._targets[target] = store
            return store

    def add(self, target: str, key: str, version: ValueWithContext) -> None:
        self._store_for(target).put(key, version)

    def targets(self) -> list[str]:
        """Targets that currently have at least one pending hint."""
        with self._lock:
            return [t for t, s in self._targets.items() if len(s)]

    def pending(self, target: str) -> list[tuple[str, list[ValueWithContext]]]:
        with self._lock:
            store = self._targets.get(target)
        return list(store.items()) if store is not None else []

    def versions_for_key(self, key: str) -> list[ValueWithContext]:
        """All hinted versions of ``key`` regardless of target."""
        with self._lock:
            stores = list(self._targets.values())
        out: list[ValueWithContext] = []
        for store in stores:
            out.extend(store.get(key))
        return out

    def discard(self, target: str, key: str, versions: Iterable[ValueWithContext]) -> None:
        with self._lock:
            store = self._targets.get(target)
        if store is not None:
            store.discard(key, versions)

    def count(self, target: str | None = None) -> int:
        """Number of hinted keys (for one target or in total)."""
        with self._lock:
            stores = [self._targets[target]] if target in self._targets else (
                [] if target is not None else list(self._targets.values())
            )
        return sum(len(s) for s in stores)

    def max_counter(self, node_id: str) -> int:
        with self._lock:
            stores = list(self._targets.values())
        return max((s.max_counter(node_id) for s in stores), default=0)


class DurableCounter:
    """Monotonic counter that never repeats a value, even across restarts.

    A coordinator stamps each write with ``(node_id, counter)``; reusing a
    counter after a restart could give two different values the same vector
    clock. Persisting every increment would add a disk write to every PUT, so
    the counter instead *reserves* values in blocks: the high-water mark is
    written (and fsynced) once per ``block`` increments, and on restart the
    counter resumes from the reserved mark. At most ``block`` values are
    skipped per restart.
    """

    def __init__(self, path: str | os.PathLike[str] | None, *, floor: int = 0, block: int = 1000) -> None:
        self.path = Path(path) if path is not None else None
        self.block = block
        self._lock = threading.Lock()
        persisted = 0
        if self.path is not None and self.path.exists():
            try:
                persisted = int(self.path.read_text().strip() or 0)
            except ValueError:
                log.warning("counter: ignoring corrupt counter file %s", self.path)
        self._value = max(floor, persisted)
        self._reserved = self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def next_after(self, observed: int) -> int:
        """Return a fresh value strictly greater than ``observed`` and every
        value previously returned."""
        with self._lock:
            value = max(self._value, observed) + 1
            if value > self._reserved:
                self._reserve(value + self.block)
            self._value = value
            return value

    def _reserve(self, mark: int) -> None:
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(_TMP_SUFFIX)
            with open(tmp, "w") as fh:
                fh.write(str(mark))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        self._reserved = mark
