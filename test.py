# test_ring_pref_consistency.py
from dht_ring import DHTRing
import hashlib

def main():
    ring = DHTRing(vnode_count=10)
    ring.add_node("node-1", "localhost:50051")
    ring.add_node("node-2", "localhost:50052")
    ring.add_node("node-3", "localhost:50053")
    k = "some:key:for:test"
    pref1 = ring.get_preference_list(k, 3)
    pref2 = ring.get_preference_list(k, 3)
    print("pref1:", pref1)
    print("pref2:", pref2)
    assert pref1 == pref2
    print("OK: preference list deterministic")

if __name__ == "__main__":
    main()
