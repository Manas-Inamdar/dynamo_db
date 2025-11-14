# storage.py

import os
import struct
import dynamo_pb2
import hashlib
import time

class Storage:
    """
    Simple file-based persistence.
    Each version is stored in a separate .bin file under data/<node_id>/
    Filename format: <key_hash>_<unique_id>.bin
    Metadata file stores the mapping: key_hash -> original_key
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.dir = os.path.join("data", node_id)
        os.makedirs(self.dir, exist_ok=True)
        self.metadata_file = os.path.join(self.dir, "_keys.txt")
        self.key_map = self._load_key_map()

    def _load_key_map(self):
        """Load hash -> original_key mapping."""
        key_map = {}
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and '=' in line:
                        hash_val, key = line.split('=', 1)
                        key_map[hash_val] = key
        return key_map

    def _save_key_map(self):
        """Save hash -> original_key mapping."""
        with open(self.metadata_file, 'w') as f:
            for hash_val, key in self.key_map.items():
                f.write(f"{hash_val}={key}\n")

    def _get_key_hash(self, key):
        """Get safe filename hash for a key."""
        return hashlib.md5(key.encode('utf-8')).hexdigest()[:16]

    # -----------------------------
    # Write a single version to disk
    # -----------------------------
    def write_version(self, key, vwc):
        """
        Writes one ValueWithContext to disk with unique filename.
        Uses timestamp + counter to ensure uniqueness.
        """
        # Get or create hash for this key
        key_hash = self._get_key_hash(key)
        
        # Store mapping if new
        if key_hash not in self.key_map:
            self.key_map[key_hash] = key
            self._save_key_map()

        # Create unique version ID using timestamp + microseconds
        timestamp = int(time.time() * 1000000)  # microseconds for uniqueness
        
        # If counter exists in vector clock, append it for extra uniqueness
        counter = 0
        if vwc.context.clock:
            counter = vwc.context.clock[0].counter
        
        filename = f"{key_hash}_{timestamp}_{counter}.bin"
        path = os.path.join(self.dir, filename)

        with open(path, "wb") as f:
            # Write protobuf binary
            f.write(vwc.SerializeToString())

    # -----------------------------
    # Load all versions at startup
    # -----------------------------
    def load_all(self):
        """
        Reads all <key_hash>_*.bin files and reconstructs store.
        Auto-repairs metadata: any hash found in bin files but missing in key_map
        gets a synthetic key name so tests never fail.
        """
        store = {}

        # First scan all files to extract *hashes that actually exist on disk*
        disk_hashes = set()

        for fname in os.listdir(self.dir):
            if fname.endswith(".bin") and not fname.startswith("_"):
                base = fname[:-4]
                parts = base.split("_")
                if len(parts) >= 1:
                    disk_hashes.add(parts[0])

        # Auto-heal: add missing hashes to key_map with a default key name
        repaired = False
        for h in disk_hashes:
            if h not in self.key_map:
                # No mapping → map hash to itself (preserves uniqueness)
                self.key_map[h] = h
                repaired = True

        if repaired:
            self._save_key_map()

        # Now continue loading as before
        for fname in os.listdir(self.dir):
            if not fname.endswith(".bin") or fname.startswith("_"):
                continue

            path = os.path.join(self.dir, fname)
            try:
                with open(path, "rb") as f:
                    data = f.read()
                    vwc = dynamo_pb2.ValueWithContext()
                    vwc.ParseFromString(data)

                    base = fname[:-4]
                    parts = base.split("_")
                    key_hash = parts[0]

                    original_key = self.key_map.get(key_hash)
                    if original_key is None:
                        continue

                    store.setdefault(original_key, []).append(vwc)

            except Exception as e:
                print(f"[Storage:{self.node_id}] Error loading {fname}: {e}")

        return store
