# server.py
import grpc
from concurrent import futures
import time
import threading
import random

import dynamo_pb2
import dynamo_pb2_grpc

from dht_ring import DHTRing
from vector_clock import VectorClock
from storage import Storage


# ----------------------------------------------------
# GLOBAL CONFIG
# ----------------------------------------------------
N = 3     # replication factor
R = 2     # read quorum
W = 2     # write quorum

GOSSIP_INTERVAL = 1.0        # seconds between gossip rounds
FAIL_TIMEOUT_MS = 5000       # node DOWN after this (5s of silence)
REMOVE_TIMEOUT_MS = 120000   # remove from ring after 2 minutes DOWN


# ====================================================
#   DYNAMO NODE SERVICE IMPLEMENTATION (Coordinator)
# ====================================================
class DynamoNode(dynamo_pb2_grpc.DynamoServiceServicer):

    def __init__(self, node_id, address, ring):
        self.node_id = node_id
        self.address = address
        self.ring = ring

        # storage + hints
        self.store = {}
        self.hints = {}  # target_node_id -> [Hint]

        # membership: node_id -> {generation, last_ts, up, address}
        self.membership = {}
        self.lock = threading.Lock()

        # Initialize membership for all known nodes in ring
                # Initialize membership for all known nodes in ring
        now_ms = int(time.time() * 1000)
        for nid, addr in self.ring.nodes.items():
            is_self = (nid == self.node_id)
            self.membership[nid] = {
                "generation": 0,
                "last_ts": now_ms,
                "up": is_self,
                "address": addr
            }

        # Ensure there's always a local entry for this node even if ring didn't include it
        if self.node_id not in self.membership:
            # Node may be starting as a newcomer (not yet joined). Create local membership entry.
            self.membership[self.node_id] = {
                "generation": 1,
                "last_ts": now_ms,
                "up": True,
                "address": self.address
            }
        else:
            # If entry existed, bump generation and mark UP
            self.membership[self.node_id]["generation"] = max(1, self.membership[self.node_id].get("generation", 0) + 1)
            self.membership[self.node_id]["up"] = True
            self.membership[self.node_id]["last_ts"] = now_ms


        print(f"[{self.node_id}] Node initialized at {self.address}")

        # Storage system
        self.storage = Storage(node_id)
        self.store = self.storage.load_all()
        print(f"[{self.node_id}] Loaded {sum(len(v) for v in self.store.values())} versions from disk")

        # Start gossip thread
        t = threading.Thread(target=self._gossip_loop, daemon=True)
        t.start()

    # ---------------------------
    # Hint storage
    # ---------------------------
    def _store_hint(self, target_node_id, key, vwc):
        hint = dynamo_pb2.Hint(
            key=key,
            data=vwc,
            target_node_id=target_node_id,
            timestamp_ms=int(time.time() * 1000)
        )
        with self.lock:
            self.hints.setdefault(target_node_id, []).append(hint)
        print(f"[{self.node_id}] HINT STORED for key={key} for target={target_node_id}")

    # ---------------------------
    # Local persistent storage
    # ---------------------------
    def _store_local_version(self, key, vwc):
        with self.lock:
            self.store.setdefault(key, []).append(vwc)
        self.storage.write_version(key, vwc)
        print(f"[{self.node_id}] Persisted version of key={key}")

    # ---------------------------
    # Forward PUT
    # ---------------------------
    def _send_forward_put(self, replica_id, key, vwc, ack_list):
        address = self.ring.nodes.get(replica_id)
        if not address:
            print(f"[{self.node_id}] No address for replica {replica_id}, storing hint")
            self._store_hint(replica_id, key, vwc)
            ack_list.append(0)
            return

        try:
            channel = grpc.insecure_channel(address)
            stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
            req = dynamo_pb2.PutRequest(key=key, data=vwc)
            reply = stub.ForwardPut(req, timeout=1.5)
            if reply.success:
                ack_list.append(1)
                print(f"[{self.node_id}] Replica {replica_id} ACK")
                return
            print(f"[{self.node_id}] Replica {replica_id} NACK → hint")
            self._store_hint(replica_id, key, vwc)
            ack_list.append(0)
        except Exception as e:
            print(f"[{self.node_id}] ERROR PUT→{replica_id}: {e}")
            self._store_hint(replica_id, key, vwc)
            ack_list.append(0)

    # ---------------------------
    # Forward GET
    # ---------------------------
    def _send_forward_get(self, replica_id, key, reply_list):
        address = self.ring.nodes.get(replica_id)
        if not address:
            print(f"[{self.node_id}] No address for replica {replica_id}")
            return
        try:
            channel = grpc.insecure_channel(address)
            stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
            reply = stub.ForwardGet(dynamo_pb2.GetRequest(key=key), timeout=1.5)
            reply_list.append((replica_id, reply))
            print(f"[{self.node_id}] GET reply from {replica_id}")
        except Exception as e:
            print(f"[{self.node_id}] ERROR GET→{replica_id}: {e}")

    # ---------------------------
    # Version reconciliation
    # ---------------------------
    def _reconcile_versions(self, versions):
        unique = []
        seen = set()
        for v in versions:
            k = (bytes(v.value), str(v.context))
            if k not in seen:
                seen.add(k)
                unique.append(v)

        if len(unique) <= 1:
            return unique

        vc_list = [VectorClock.from_proto(v.context) for v in unique]
        dominated = [False] * len(unique)

        for i, vc_i in enumerate(vc_list):
            for j, vc_j in enumerate(vc_list):
                if i == j:
                    continue
                if vc_j.compare(vc_i) == 1:  # j dominates i
                    dominated[i] = True
                    break

        return [unique[i] for i in range(len(unique)) if not dominated[i]]

    # ---------------------------
    # Read repair
    # ---------------------------
    def _schedule_read_repair(self, key, reconciled, pref_list, replies):
        def worker():
            for rid, rep in replies:
                returned = set((bytes(v.value), str(v.context)) for v in rep.data)
                for vwc in reconciled:
                    ident = (bytes(vwc.value), str(vwc.context))
                    if ident not in returned:
                        try:
                            addr = self.ring.nodes[rid]
                            ch = grpc.insecure_channel(addr)
                            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
                            stub.ForwardPut(dynamo_pb2.PutRequest(key=key, data=vwc), timeout=1.5)
                            print(f"[{self.node_id}] ReadRepair: updated {rid}")
                        except:
                            pass
        threading.Thread(target=worker, daemon=True).start()

    # ---------------------------
    # RPC: PutHint
    # ---------------------------
    def PutHint(self, request, context):
        with self.lock:
            self.hints.setdefault(request.target_node_id, []).append(request)
        print(f"[{self.node_id}] STORED REMOTE HINT for {request.key}")
        return dynamo_pb2.PutReply(success=True, message="Hint stored")

    # ====================================================
    # CLIENT-FACING PUT
    # ====================================================
    def Put(self, request, context):
        print(f"[{self.node_id}] CLIENT PUT {request.key}")

        key = request.key
        incoming = VectorClock.from_proto(request.data.context)
        incoming.increment(self.node_id)

        vwc = dynamo_pb2.ValueWithContext(value=request.data.value,
                                          context=incoming.to_proto())

        pref = self.ring.get_preference_list(key, N)
        print(f"[{self.node_id}] Preference list: {pref}")

        success = 0

        if self.node_id in pref:
            self._store_local_version(key, vwc)
            success = 1

        acks = []
        threads = []

        for rid in pref:
            if rid == self.node_id:
                continue
            t = threading.Thread(target=self._send_forward_put,
                                 args=(rid, key, vwc, acks))
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=2)

        success += sum(acks)

        if success >= W:
            return dynamo_pb2.PutReply(success=True,
                                       message=f"W quorum satisfied ({success})")
        else:
            return dynamo_pb2.PutReply(success=False,
                                       message=f"Write failed ({success})")

    # ====================================================
    # CLIENT-FACING GET
    # ====================================================
    def Get(self, request, context):
        print(f"[{self.node_id}] CLIENT GET {request.key}")

        key = request.key
        pref = self.ring.get_preference_list(key, N)
        print(f"[{self.node_id}] Preference list: {pref}")

        if not pref:
            return dynamo_pb2.GetReply(found=False, message="Empty ring")

        replies = []
        threads = []

        for rid in pref:
            t = threading.Thread(target=self._send_forward_get,
                                 args=(rid, key, replies))
            t.start()
            threads.append(t)

        waited = 0
        while waited < 2.5 and len(replies) < R:
            time.sleep(0.05)
            waited += 0.05

        for t in threads:
            t.join(timeout=0.05)

        if len(replies) < R:
            return dynamo_pb2.GetReply(found=False,
                                       message=f"R quorum FAIL ({len(replies)})")

        all_versions = []
        for _, rep in replies:
            all_versions.extend(rep.data)

        reconciled = self._reconcile_versions(all_versions)
        self._schedule_read_repair(key, reconciled, pref, replies)

        return dynamo_pb2.GetReply(found=len(reconciled) > 0,
                                   data=reconciled,
                                   message="OK")

    # ====================================================
    # INTERNAL RPC: ForwardPut
    # ====================================================
    def ForwardPut(self, request, context):
        key = request.key
        vwc = request.data

        with self.lock:
            self.store.setdefault(key, []).append(vwc)

        self.storage.write_version(key, vwc)
        print(f"[{self.node_id}] Replica stored {key}")
        return dynamo_pb2.PutReply(success=True, message="Replica stored")

    # ====================================================
    # INTERNAL RPC: ForwardGet
    # ====================================================
    def ForwardGet(self, request, context):
        key = request.key
        with self.lock:
            values = self.store.get(key, [])
        return dynamo_pb2.GetReply(found=len(values)>0,
                                   data=values,
                                   message="OK")

    # ====================================================
    # RPC: DeliverHints
    # ====================================================
    def DeliverHints(self, request, context):
        delivered = 0
        for h in request.hints:
            key = h.key
            vwc = h.data
            with self.lock:
                self.store.setdefault(key, []).append(vwc)
            self.storage.write_version(key, vwc)
            delivered += 1
        print(f"[{self.node_id}] Delivered {delivered} hints")
        return dynamo_pb2.DeliverHintsReply(success=True,
                                            message=f"Delivered {delivered}")

    # ====================================================
    # Hints delivery helper
    # ====================================================
    def deliver_hints_to(self, target):
        with self.lock:
            hints = self.hints.get(target, [])
            if not hints:
                return
            addr = self.membership.get(target, {}).get("address")

        if not addr:
            print(f"[{self.node_id}] No address for {target}, cannot deliver hints")
            return

        try:
            ch = grpc.insecure_channel(addr)
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
            req = dynamo_pb2.DeliverHintsRequest(
                target_node_id=target,
                hints=hints
            )
            rep = stub.DeliverHints(req, timeout=5)
            if rep.success:
                with self.lock:
                    self.hints[target] = []
                print(f"[{self.node_id}] Delivered {len(hints)} hints → {target}")
        except Exception as e:
            print(f"[{self.node_id}] Failed delivering hints → {target}: {e}")

    # ====================================================
    # GOSSIP RPC
    # ====================================================
    def Gossip(self, request, context):
        incoming = {}
        for m in request.members:
            incoming[m.node_id] = {
                "generation": m.generation,
                "last_ts": m.last_updated_time,
                "up": m.up,
                "address": m.address
            }

        changed = False
        to_deliver = []

        # ---------------------------
        # Merge membership
        # ---------------------------
                # ---------------------------
        # Merge membership
        # ---------------------------
        with self.lock:
            for nid, info in incoming.items():
                cur = self.membership.get(nid)

                # New node entirely
                if cur is None:
                    self.membership[nid] = info.copy()
                    print(f"[{self.node_id}] Gossip: NEW node {nid} @ {info['address']}")
                    changed = True

                    # ADD TO RING (skip adding self here)
                    if info.get("address") and nid != self.node_id and nid not in self.ring.nodes:
                        try:
                            self.ring.add_node(nid, info["address"])
                            print(f"[{self.node_id}] Added {nid} to ring via gossip")
                        except Exception as e:
                            print(f"[{self.node_id}] ring.add_node ERROR: {e}")

                    continue

                # Existing node → check update
                if (info.get("generation", 0) > cur.get("generation", 0)) or \
                   (info.get("last_ts", 0) > cur.get("last_ts", 0)):
                    prev_up = cur.get("up", False)

                    cur["generation"] = info.get("generation", cur.get("generation", 0))
                    cur["last_ts"] = info.get("last_ts", cur.get("last_ts", 0))
                    cur["up"] = info.get("up", cur.get("up", False))
                    cur["address"] = info.get("address", cur.get("address"))

                    changed = True

                    # Add to ring IF now known and not present (skip self)
                    if cur.get("address") and nid != self.node_id and nid not in self.ring.nodes:
                        try:
                            self.ring.add_node(nid, cur["address"])
                            print(f"[{self.node_id}] Learned {nid} via gossip; added to ring")
                        except Exception as e:
                            print(f"[{self.node_id}] ring.add_node ERROR: {e}")

                    # If transitioned down→up deliver hints
                    if (not prev_up) and cur.get("up"):
                        if nid in self.hints and self.hints[nid]:
                            to_deliver.append(nid)

            # ---------------------------
            # Mark DOWN by timeout
            # ---------------------------
            now = int(time.time() * 1000)
            for nid, cur in list(self.membership.items()):
                if nid == self.node_id:
                    continue

                # if silent → DOWN
                if cur.get("up", True) and now - cur.get("last_ts", 0) > FAIL_TIMEOUT_MS:
                    cur["up"] = False
                    print(f"[{self.node_id}] TIMEOUT → DOWN: {nid}")
                    changed = True

            # ---------------------------
            # Dynamic ring update
            # ---------------------------
            for nid, info in list(self.membership.items()):
                # Add nodes that are UP and not present in ring (skip self)
                if info.get("up") and nid != self.node_id and nid not in self.ring.nodes and info.get("address"):
                    print(f"[{self.node_id}] Ring update: adding node {nid} ({info['address']})")
                    try:
                        self.ring.add_node(nid, info["address"])
                    except Exception as e:
                        print(f"[{self.node_id}] ring.add_node ERROR (during dynamic add): {e}")

                # remove from RING if long-down
                if (not info.get("up")) and (now - info.get("last_ts", 0) > REMOVE_TIMEOUT_MS):
                    if nid in self.ring.nodes:
                        try:
                            self.ring.remove_node(nid)
                            print(f"[{self.node_id}] REMOVED from ring: {nid}")
                        except Exception as e:
                            print(f"[{self.node_id}] ring.remove_node ERROR: {e}")

        
        # ---------------------------
        # Deliver hints outside lock
        # ---------------------------
        for nid in to_deliver:
            self.deliver_hints_to(nid)

        # ---------------------------
        # Respond with our membership
        # ---------------------------
        resp = dynamo_pb2.MembershipList()
        with self.lock:
            for nid, cur in self.membership.items():
                ns = dynamo_pb2.NodeState(
                    node_id=nid,
                    address=cur["address"],
                    generation=cur["generation"],
                    last_updated_time=cur["last_ts"],
                    up=cur["up"]
                )
                resp.members.append(ns)
        return resp

    # ====================================================
    # GOSSIP LOOP
    # ====================================================
    def _gossip_loop(self):
        time.sleep(0.5)  # small delay
        while True:
            try:
                now = int(time.time() * 1000)
                with self.lock:
                    self.membership[self.node_id]["last_ts"] = now

                # Choose peers with known address
                with self.lock:
                    peers = [nid for nid, info in self.membership.items()
                             if nid != self.node_id and info["address"]]

                if not peers:
                    time.sleep(GOSSIP_INTERVAL)
                    continue

                peer = random.choice(peers)
                peer_addr = self.membership[peer]["address"]

                # Build outgoing membership list
                out = dynamo_pb2.MembershipList()
                with self.lock:
                    for nid, cur in self.membership.items():
                        ns = dynamo_pb2.NodeState(
                            node_id=nid,
                            address=cur["address"],
                            generation=cur["generation"],
                            last_updated_time=cur["last_ts"],
                            up=cur["up"],
                        )
                        out.members.append(ns)

                # Send gossip
                try:
                    ch = grpc.insecure_channel(peer_addr)
                    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
                    resp = stub.Gossip(out, timeout=2)
                    self.Gossip(resp, None)
                except Exception as e:
                    print(f"[{self.node_id}] Gossip→{peer} failed: {e}")

            except Exception as e:
                print(f"[{self.node_id}] Gossip loop error: {e}")

            time.sleep(GOSSIP_INTERVAL)


# ====================================================
#   SERVER STARTUP
# ====================================================
def serve(node_id, port, ring):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=40))
    dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(
        DynamoNode(node_id, f"localhost:{port}", ring), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{node_id}] Server started on port {port}")
    server.wait_for_termination()


# ====================================================
#   MAIN
# ====================================================
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python server.py <node_id> <port>")
        exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2])

    ring = DHTRing(vnode_count=10)
    ring.add_node("node-1", "localhost:50051")
    ring.add_node("node-2", "localhost:50052")
    ring.add_node("node-3", "localhost:50053")

    serve(node_id, port, ring)
