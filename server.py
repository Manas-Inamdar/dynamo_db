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
import argparse


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

        # Ensure our node is present in the ring (so preference lists can include self)
        try:
            if self.node_id not in self.ring.nodes:
                # add self's vnodes to the ring so coordinator may include itself
                self.ring.add_node(self.node_id, self.address)
                print(f"[{self.node_id}] Added self to ring at {self.address}")
        except Exception as e:
            # defensive: don't crash server startup if ring manipulation fails
            print(f"[{self.node_id}] Warning: failed to add self to ring: {e}")

        # storage + hints
        self.store = {}
        self.hints = {}  # target_node_id -> [Hint]

        # membership: node_id -> {generation, last_ts, up, address}
        self.membership = {}
        self.lock = threading.Lock()

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
    # --------------------------
    def _send_forward_put(self, replica_id, key, vwc, ack_list):

        # -----------------------------------------
        # SKIP NETWORK CALL IF REPLICA IS KNOWN DOWN
        # -----------------------------------------
        with self.lock:
            info = self.membership.get(replica_id)
            if info and not info["up"]:
                print(f"[{self.node_id}] SKIP PUT → {replica_id} (DOWN)")
                self._store_hint(replica_id, key, vwc)
                ack_list.append(0)
                return

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

        # -----------------------------------------
        # SKIP NETWORK CALL IF REPLICA IS KNOWN DOWN
        # -----------------------------------------
        with self.lock:
            info = self.membership.get(replica_id)
            if info and not info["up"]:
                # We don't append anything to reply_list because this replica 
                # cannot contribute to read quorum.
                print(f"[{self.node_id}] SKIP GET → {replica_id} (DOWN)")
                return

        address = self.ring.nodes.get(replica_id)
        if not address:
            print(f"[{self.node_id}] No address for replica {replica_id}")
            return

        try:
            channel = grpc.insecure_channel(address)
            stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
            reply = stub.ForwardGet(
                dynamo_pb2.GetRequest(key=key), timeout=1.5
            )
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
                                       message=f"W quorum satisfied ({success})"
            )
        else:
            return dynamo_pb2.PutReply(success=False,
                                       message=f"Write failed ({success})"
            )

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
                                       message=f"R quorum FAIL ({len(replies)})"
            )

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
        # Parse incoming membership
        incoming = {}
        for m in request.members:
            incoming[m.node_id] = {
                "generation": m.generation,
                "last_ts": m.last_updated_time,
                "up": m.up,
                "address": m.address
            }

        to_deliver = []
        now_ms = int(time.time() * 1000)

        with self.lock:
            # ============================================
            # 1. MERGE MEMBERSHIP INFORMATION
            # ============================================
            for nid, info in incoming.items():
                cur = self.membership.get(nid)

                # ------------- NEW NODE DISCOVERED -------------
                if cur is None:
                    self.membership[nid] = info.copy()
                    print(f"[{self.node_id}] Gossip: NEW node {nid} @ {info['address']}")

                    # Add to ring (skip adding self)
                    if nid != self.node_id and info["address"] and nid not in self.ring.nodes:
                        try:
                            self.ring.add_node(nid, info["address"])
                            print(f"[{self.node_id}] Ring add (new): {nid}")
                        except Exception as e:
                            print(f"[{self.node_id}] ring.add_node ERROR: {e}")

                    continue

                # ------------- EXISTING NODE: CHECK UPDATE -------------
                newer = (info["generation"] > cur["generation"]) or \
                        (info["last_ts"] > cur["last_ts"])

                if newer:
                    prev_up = cur["up"]

                    cur["generation"] = info["generation"]
                    cur["last_ts"] = info["last_ts"]
                    cur["up"] = info["up"]
                    cur["address"] = info["address"]

                    # add to ring if newly discovered address
                    if nid != self.node_id and info["address"] and nid not in self.ring.nodes:
                        try:
                            self.ring.add_node(nid, info["address"])
                            print(f"[{self.node_id}] Ring add (learned): {nid}")
                        except:
                            pass

                    # Node RECOVERED → schedule hint delivery
                    # Node RECOVERED → schedule hint delivery + print once
                    if not prev_up and cur["up"]:
                        print(f"[{self.node_id}] {nid} RECOVERED")
                        if nid in self.hints and self.hints[nid]:
                            to_deliver.append(nid)

                    # if not prev_up and cur["up"]:
                    #     if nid in self.hints and self.hints[nid]:
                    #         to_deliver.append(nid)

            # ============================================
            # 2. MARK NODES DOWN BY TIMEOUT
            # ============================================
            for nid, cur in self.membership.items():
                if nid == self.node_id:
                    continue

                if cur["up"] and now_ms - cur["last_ts"] > FAIL_TIMEOUT_MS:
                    cur["up"] = False
                    print(f"[{self.node_id}] TIMEOUT → DOWN: {nid}")

            # ============================================
            # 3. RING UPDATES (BUG FIX: use correct vars)
            # ============================================
            for nid, info in list(self.membership.items()):

                # ---- ADD NODES THAT ARE UP AND MISSING ----
                if info["up"] and nid != self.node_id and info["address"] \
                and nid not in self.ring.nodes:
                    try:
                        self.ring.add_node(nid, info["address"])
                        print(f"[{self.node_id}] Ring add (UP): {nid}")
                    except:
                        pass

                # ---- REMOVE NODES DOWN FOR LONG ----
                if (not info["up"]) and \
                (now_ms - info["last_ts"] > REMOVE_TIMEOUT_MS):
                    if nid in self.ring.nodes:
                        try:
                            self.ring.remove_node(nid)
                            print(f"[{self.node_id}] Ring remove (DOWN too long): {nid}")
                        except Exception as e:
                            print(f"[{self.node_id}] ring.remove_node ERROR: {e}")

        # ============================================
        # 4. DELIVER HINTS (outside lock)
        # ============================================
        for nid in to_deliver:
            self.deliver_hints_to(nid)

        # ============================================
        # 5. RETURN LOCAL MEMBERSHIP STATE
        # ============================================
        resp = dynamo_pb2.MembershipList()
        with self.lock:
            for nid, cur in self.membership.items():
                resp.members.append(
                    dynamo_pb2.NodeState(
                        node_id=nid,
                        address=cur["address"],
                        generation=cur["generation"],
                        last_updated_time=cur["last_ts"],
                        up=cur["up"]
                    )
                )
        return resp
    # ====================================================
    # GOSSIP LOOP
    # ====================================================
    def _gossip_loop(self):
        time.sleep(0.5)
        while True:
            try:
                now = int(time.time() * 1000)
                with self.lock:
                    self.membership[self.node_id]["last_ts"] = now

                    # choose ONLY UP peers
                    peers = [
                        nid for nid, info in self.membership.items()
                        if nid != self.node_id
                        and info["address"]
                        and info["up"]          # <-- skip known DOWN nodes
                    ]

                if not peers:
                    time.sleep(GOSSIP_INTERVAL)
                    continue

                peer = random.choice(peers)

                with self.lock:
                    peer_addr = self.membership[peer]["address"]

                # Build outgoing membership list
                out = dynamo_pb2.MembershipList()
                with self.lock:
                    for nid, cur in self.membership.items():
                        out.members.append(
                            dynamo_pb2.NodeState(
                                node_id=nid,
                                address=cur["address"],
                                generation=cur["generation"],
                                last_updated_time=cur["last_ts"],
                                up=cur["up"],
                            )
                        )

                try:
                    ch = grpc.insecure_channel(peer_addr)
                    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
                    resp = stub.Gossip(out, timeout=2)
                    self.Gossip(resp, None)

                except Exception:
                    # Do NOT print RPC spam
                    # Instead: mark DOWN once
                    with self.lock:
                        info = self.membership.get(peer)
                        if info and info["up"]:
                            info["up"] = False
                            # Mark DOWN only once
                            print(f"[{self.node_id}] Marked {peer} DOWN (RPC failure).")

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
# ====================================================
#   MAIN
# ====================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id")
    parser.add_argument("port")
    parser.add_argument("--seed", required=False,
                        help="Seed node address (host:port) for joining the cluster")
    args = parser.parse_args()

    node_id = args.node_id
    port = int(args.port)

    ring = DHTRing(vnode_count=10)

    # Start server
    threading.Thread(
        target=lambda: serve(node_id, port, ring),
        daemon=True
    ).start()

    # Give the server time to come online
    time.sleep(1.0)

    # ------------------------
    # AUTO-JOIN if --seed used
    # ------------------------
    if args.seed:
        try:
            print(f"[{node_id}] Contacting seed {args.seed} ...")

            ch = grpc.insecure_channel(args.seed)
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)

            ml = dynamo_pb2.MembershipList()
            ns = dynamo_pb2.NodeState(
                node_id=node_id,
                address=f"localhost:{port}",
                generation=1,
                last_updated_time=int(time.time() * 1000),
                up=True
            )
            ml.members.append(ns)

            stub.Gossip(ml, timeout=3)
            print(f"[{node_id}] Successfully joined cluster via seed {args.seed}")

        except Exception as e:
            print(f"[{node_id}] Failed to auto-join via seed: {e}")

    # keep main thread alive
    while True:
        time.sleep(10)
