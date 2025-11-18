# dht_ring.py
import hashlib
import bisect

class DHTRing:

    def __init__(self, vnode_count=10):
        self.vnode_count = vnode_count
        self.ring = []           # list of (token_hash:int, node_id:str)
        self.nodes = {}          # node_id -> address

    # -------------------------------
    # Hash helper — MUST match vnode hashing
    # -------------------------------
    @staticmethod
    def hash_value(data: str) -> int:
        """Generate a 160-bit SHA1 hash as an integer."""
        return int(hashlib.sha1(data.encode()).hexdigest(), 16)

    # -------------------------------
    # Add a physical node + its virtual nodes
    # -------------------------------
    def add_node(self, node_id: str, address: str):
        if node_id in self.nodes:
            print(f"[Ring] Node {node_id} already exists.")
            return
        
        self.nodes[node_id] = address

        # Add vnodes
        for i in range(self.vnode_count):
            token_str = f"{node_id}-vnode-{i}"
            token_hash = self.hash_value(token_str)
            bisect.insort(self.ring, (token_hash, node_id))

        print(f"[Ring] Added node {node_id} with {self.vnode_count} vnodes.")

    # -------------------------------
    # Remove a node (and all its vnodes)
    # -------------------------------
    def remove_node(self, node_id: str):
        before = len(self.ring)
        self.ring = [(tok, nid) for (tok, nid) in self.ring if nid != node_id]
        removed = before - len(self.ring)

        if node_id in self.nodes:
            del self.nodes[node_id]

        print(f"[Ring] Removed node {node_id}, removed {removed} tokens.")

    # ---------------------------------------------------
    # CORRECT DYNAMO PREFERENCE LIST IMPLEMENTATION
    # ---------------------------------------------------
    def get_preference_list(self, key: str, N: int):
        """
        Return up to N distinct physical nodes in clockwise order,
        starting from the first vnode whose token >= hash(key).

        This is the correct Dynamo behavior.
        """
        if not self.ring:
            return []

        key_hash = self.hash_value(key)

        # Find first token >= key_hash (or wrap to 0)
        idx = bisect.bisect_left(self.ring, (key_hash, ""))

        result = []
        seen_nodes = set()

        ring_len = len(self.ring)
        i = idx

        # Walk entire ring at most once
        for _ in range(ring_len):
            token, node_id = self.ring[i % ring_len]

            if node_id not in seen_nodes:
                seen_nodes.add(node_id)
                result.append(node_id)

                if len(result) == N:
                    break

            i += 1

        return result

    # -------------------------------
    # Debug helper
    # -------------------------------
    def print_ring(self):
        print("---- RING STATE ----")
        for token, nid in self.ring[:40]:
            print(f"{token} -> {nid}")
        print("---------------------")


# --------------------------------------------------------
# Basic test cases (run this file directly)
# --------------------------------------------------------
if __name__ == "__main__":
    print("Running basic DHTRing tests...")
    ring = DHTRing(vnode_count=5)

    ring.add_node("node-1", "localhost:50051")
    ring.add_node("node-2", "localhost:50052")
    ring.add_node("node-3", "localhost:50053")

    print("\nPreference list for key 'apple' (N=3):")
    print(ring.get_preference_list("apple", 3))

    print("\nPreference list for key 'project' (N=3):")
    print(ring.get_preference_list("project", 3))

    print("\nRemoving node-2...")
    ring.remove_node("node-2")

    print("\nPreference list for key 'apple' after removal (N=2):")
    print(ring.get_preference_list("apple", 2))

    print("\nAll tests done.")
