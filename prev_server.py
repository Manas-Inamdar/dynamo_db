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
FAIL_TIMEOUT_MS = 5000       # consider node DOWN if no update within this ms

# ====================================================
#   DYNAMO NODE SERVICE IMPLEMENTATION (Coordinator)
# ====================================================
class DynamoNode(dynamo_pb2_grpc.DynamoServiceServicer):

    def __init__(self, node_id, address, ring):
        self.node_id = node_id
        self.address = address
        self.ring = ring

        # storage and hints
        self.store = {}          # key -> [ValueWithContext]
        self.hints = {}          # target_node_id -> list of Hint messages

        # membership: node_id -> {generation, last_ts, up(bool), address}
        self.membership = {}
        self.lock = threading.Lock()

        # initialize membership from ring known nodes
        now_ms = int(time.time() * 1000)
        for nid, addr in self.ring.nodes.items():
            # default generation 0, mark UP for self only; others initially assume UP
            is_self = (nid == self.node_id)
            self.membership[nid] = {
                "generation": 0,
                "last_ts": now_ms,
                "up": is_self,  # Only self is confirmed UP initially
                "address": addr
            }

        # ensure our own entry has a generation > 0 (helps detection on restart)
        self.membership[self.node_id]["generation"] = 1
        self.membership[self.node_id]["up"] = True
        self.membership[self.node_id]["last_ts"] = now_ms

        print(f"[{self.node_id}] Node initialized at {self.address}")
        # start gossip thread
        t = threading.Thread(target=self._gossip_loop, daemon=True)
        t.start()
        self.storage = Storage(node_id)

# Load existing data (persistent)
        self.store = self.storage.load_all()
        print(f"[{self.node_id}] Loaded {sum(len(v) for v in self.store.values())} versions from disk")


    # ---------------------------
    # HINT STORAGE
    # ---------------------------
    def _store_hint(self, target_node_id, key, vwc):
        hint = dynamo_pb2.Hint(
            key=key,
            data=vwc,
            target_node_id=target_node_id,
            timestamp_ms=int(time.time() * 1000)
        )

        with self.lock:
            if target_node_id not in self.hints:
                self.hints[target_node_id] = []
            self.hints[target_node_id].append(hint)

        print(f"[{self.node_id}] HINT STORED for key={key} → target={target_node_id}")

    # ---------------------------
    # LOCAL STORAGE
    # ---------------------------
    def _store_local_version(self, key, vwc):
        """Append a new version into local storage."""
        with self.lock:
            if key not in self.store:
                self.store[key] = []
            self.store[key].append(vwc)
        self.storage.write_version(key, vwc)
        print(f"[{self.node_id}] Persisted version of key={key} to disk")
    # ---------------------------
    # Forwarding helpers (PUT/GET)
    # ---------------------------
    def _send_forward_put(self, replica_id, key, vwc, ack_list):
        address = self.ring.nodes[replica_id]

        try:
            ch = grpc.insecure_channel(address)
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)

            req = dynamo_pb2.PutRequest(
                key=key,
                data=vwc
            )

            reply = stub.ForwardPut(req, timeout=1.5)

            if reply.success:
                print(f"[{self.node_id}] Replica {replica_id} ACK")
                ack_list.append(1)
                return

            # If success==False treat as failure → store hint
            print(f"[{self.node_id}] Replica {replica_id} NACK → storing hint")
            self._store_hint(replica_id, key, vwc)
            ack_list.append(0)

        except Exception as e:
            print(f"[{self.node_id}] ERROR contacting {replica_id}: {e}")
            print(f"[{self.node_id}] Storing HINT for {key} intended for {replica_id}")
            self._store_hint(replica_id, key, vwc)
            ack_list.append(0)


    def _send_forward_get(self, replica_id, key, reply_list):
        address = self.ring.nodes[replica_id]
        try:
            channel = grpc.insecure_channel(address)
            stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
            req = dynamo_pb2.GetRequest(key=key)
            reply = stub.ForwardGet(req, timeout=1.5)
            reply_list.append((replica_id, reply))
            print(f"[{self.node_id}] Got GET reply from {replica_id}")
        except Exception as e:
            print(f"[{self.node_id}] ERROR contacting {replica_id} for GET: {e}")

    def _reconcile_versions(self, all_vwcs):
        """
        all_vwcs: list of dynamo_pb2.ValueWithContext
        Returns list of ValueWithContext that are not causally dominated (siblings).
        """
        # Deduplicate by (value bytes + context string) to avoid exact duplicates
        unique = []
        seen = set()
        for v in all_vwcs:
            key = (bytes(v.value), str(v.context))
            if key not in seen:
                seen.add(key)
                unique.append(v)

        # If only one unique, return it
        if len(unique) <= 1:
            return unique

        # Convert contexts to VectorClock for comparison
        vc_list = [VectorClock.from_proto(v.context) for v in unique]

        # Determine dominated versions
        dominated = [False] * len(unique)
        for i, vi in enumerate(vc_list):
            for j, vj in enumerate(vc_list):
                if i == j:
                    continue
                cmp = vj.compare(vi)  # compare(vj, vi): 1 if vj > vi (vj dominates vi)
                if cmp == 1:
                    dominated[i] = True
                    break

        result = [unique[i] for i, dom in enumerate(dominated) if not dom]
        return result

    def _schedule_read_repair(self, key, reconciled_vwcs, replica_ids, replica_replies):
        """
        Async: push reconciled_vwcs to replicas that are stale/missing them.
        replica_replies: list of (replica_id, reply)
        """
        def worker():
            # compute which replicas are missing any of the reconciled versions
            for rid, reply in replica_replies:
                # collect strings of returned versions for easy comparison
                returned_keys = set((bytes(v.value), str(v.context)) for v in reply.data)
                for vwc in reconciled_vwcs:
                    identifier = (bytes(vwc.value), str(vwc.context))
                    if identifier not in returned_keys:
                        # push vwc to replica rid
                        try:
                            addr = self.ring.nodes[rid]
                            ch = grpc.insecure_channel(addr)
                            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
                            req = dynamo_pb2.PutRequest(key=key, data=vwc)
                            stub.ForwardPut(req, timeout=1.5)
                            print(f"[{self.node_id}] Read-repair: updated replica {rid} for key {key}")
                        except Exception as e:
                            print(f"[{self.node_id}] Read-repair failed for {rid}: {e}")

        t = threading.Thread(target=worker, daemon=True)
        t.start()

    # ---------------------------
    # PutHint RPC (store hint locally)
    # ---------------------------
    def PutHint(self, request, context):
        target = request.target_node_id
        key = request.key

        with self.lock:
            if target not in self.hints:
                self.hints[target] = []

            # store raw Hint protobuf message
            self.hints[target].append(request)

        print(f"[{self.node_id}] STORED HINT for key={key} intended_for={target}")

        return dynamo_pb2.PutReply(success=True, message="Hint stored")

    # ------------------------------------------------
    # CLIENT-FACING PUT (Coordinator entry point)
    # ------------------------------------------------
    def Put(self, request, context):
        print(f"[{self.node_id}] CLIENT PUT key={request.key}")

        key = request.key
        value_bytes = request.data.value  # raw bytes
        ctx_proto = request.data.context

        # 1. Load/merge vector clock
        incoming_clock = VectorClock.from_proto(ctx_proto)
        incoming_clock.increment(self.node_id)  # coordinator increments its own entry

        # 2. Prepare ValueWithContext for replicas
        vwc = dynamo_pb2.ValueWithContext(
            value=value_bytes,
            context=incoming_clock.to_proto()
        )

        # 3. Compute preference list
        pref_list = self.ring.get_preference_list(key, N)
        print(f"[{self.node_id}] Preference list for {key}: {pref_list}")

        # 4. Local write ONLY if coordinator is in preference list
        success_count = 0
        if self.node_id in pref_list:
            self._store_local_version(key, vwc)
            success_count = 1  # local write counts as success
            print(f"[{self.node_id}] Coordinator is in preference list, stored locally")

        # 5. Forward to replicas in parallel
        acks = []
        threads = []

        for replica_id in pref_list:
            if replica_id == self.node_id:
                continue  # skip self, already stored (or not in pref list)

            t = threading.Thread(
                target=self._send_forward_put,
                args=(replica_id, key, vwc, acks)
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=2)

        success_count += sum(acks)

        # 6. Check W quorum
        if success_count >= W:
            return dynamo_pb2.PutReply(
                success=True,
                message=f"W quorum satisfied with {success_count} acks."
            )
        else:
            return dynamo_pb2.PutReply(
                success=False,
                message=f"Write failed: only {success_count} replicas acked."
            )


    # ------------------------------------------------
    # CLIENT-FACING GET (Coordinator entry point)
    # ------------------------------------------------
    def Get(self, request, context):
        print(f"[{self.node_id}] CLIENT GET key={request.key}")

        key = request.key
        pref_list = self.ring.get_preference_list(key, N)
        print(f"[{self.node_id}] Preference list for {key}: {pref_list}")

        # If no nodes (empty ring)
        if not pref_list:
            return dynamo_pb2.GetReply(found=False, message="Empty ring")

        # Parallel ForwardGet to preference list
        reply_list = []  # will hold tuples (replica_id, GetReply)
        threads = []
        for replica_id in pref_list:
            t = threading.Thread(target=self._send_forward_get, args=(replica_id, key, reply_list))
            t.start()
            threads.append(t)

        # Wait until we get R replies or all threads finish (timeout handled in helper)
        waited = 0.0
        poll_interval = 0.05
        timeout = 2.5
        while waited < timeout and len(reply_list) < R:
            time.sleep(poll_interval)
            waited += poll_interval

        # join threads briefly to clean up
        for t in threads:
            t.join(timeout=0.01)

        if len(reply_list) < R:
            return dynamo_pb2.GetReply(found=False, message=f"Failed to achieve read quorum (got {len(reply_list)})")

        # Flatten collected ValueWithContext from the R replies
        all_vwcs = []
        for rid, rep in reply_list:
            for v in rep.data:
                all_vwcs.append(v)

        # Reconcile versions using vector clocks
        reconciled = self._reconcile_versions(all_vwcs)

        # Schedule read-repair asynchronously
        self._schedule_read_repair(key, reconciled, pref_list, reply_list)

        return dynamo_pb2.GetReply(found=len(reconciled) > 0, data=reconciled, message="OK")


    # ------------------------------------------------
    # INTERNAL RPC TO REPLICAS: PUT
    # ------------------------------------------------
    def ForwardPut(self, request, context):
        key = request.key
        vwc = request.data

        # store version in memory
        with self.lock:
            if key not in self.store:
                self.store[key] = []
            self.store[key].append(vwc)

        # CRITICAL FIX: Persist to disk!
        self.storage.write_version(key, vwc)

        print(f"[{self.node_id}] Stored replica PUT key={key}")
        return dynamo_pb2.PutReply(success=True, message="Replica stored")


    # ------------------------------------------------
    # INTERNAL RPC TO REPLICAS: GET
    # ------------------------------------------------
    def ForwardGet(self, request, context):
        print(f"[{self.node_id}] FORWARD GET received for {request.key}")

        with self.lock:
            values = self.store.get(request.key, [])

        return dynamo_pb2.GetReply(
            found=len(values) > 0,
            data=values,
            message="OK"
        )

    # ---------------------------
    # DeliverHints RPC (incoming hints delivered to this node)
    # ---------------------------
    def DeliverHints(self, request, context):
        # request.target_node_id is intended to be this node's id
        target = request.target_node_id
        delivered = 0
        for h in request.hints:
            key = h.key
            vwc = h.data
            with self.lock:
                if key not in self.store:
                    self.store[key] = []
                # append delivered version
                self.store[key].append(vwc)
            delivered += 1
            self.storage.write_version(key, vwc)
            print(f"[{self.node_id}] Delivered hint for key={key} from remote")

        return dynamo_pb2.DeliverHintsReply(success=True, message=f"Delivered {delivered} hints")

    # ---------------------------
    # Gossip RPC handler
    # ---------------------------
    def Gossip(self, request, context):
        # request is MembershipList
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

        with self.lock:
            for nid, info in incoming.items():
                cur = self.membership.get(nid)
                if cur is None:
                    # new node discovered
                    self.membership[nid] = {
                        "generation": info["generation"],
                        "last_ts": info["last_ts"],
                        "up": info["up"],
                        "address": info["address"]
                    }
                    print(f"[{self.node_id}] Gossip: discovered new node {nid} addr={info['address']}")
                    changed = True
                    if info["up"] and nid in self.hints and self.hints[nid]:
                        to_deliver.append(nid)
                else:
                    # merge by generation/last_ts
                    if info["generation"] > cur["generation"] or info["last_ts"] > cur["last_ts"]:
                        prev_up = cur["up"]
                        cur["generation"] = info["generation"]
                        cur["last_ts"] = info["last_ts"]
                        cur["address"] = info["address"]
                        cur["up"] = info["up"]
                        changed = True
                        # if transitioned from DOWN->UP, schedule hint delivery
                        if (not prev_up) and cur["up"] and nid in self.hints and self.hints[nid]:
                            to_deliver.append(nid)

            # also mark nodes DOWN if their last_ts is too old
            now_ms = int(time.time() * 1000)
            for nid, cur in list(self.membership.items()):
                if nid == self.node_id:
                    continue
                if now_ms - cur["last_ts"] > FAIL_TIMEOUT_MS and cur["up"]:
                    cur["up"] = False
                    print(f"[{self.node_id}] Marking node {nid} DOWN by timeout")
                    changed = True

        # attempt to deliver hints for recovered nodes (outside lock)
        for recovered in to_deliver:
            print(f"[{self.node_id}] Detected RECOVERY for {recovered}, delivering hints if present")
            self.deliver_hints_to(recovered)

        # respond with our current membership snapshot
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

    # ---------------------------
    # Deliver hints to recovered node (caller)
    # ---------------------------
    def deliver_hints_to(self, target_node_id):
        with self.lock:
            if target_node_id not in self.hints or not self.hints[target_node_id]:
                print(f"[{self.node_id}] No hints for {target_node_id}")
                return

            hints_to_send = list(self.hints[target_node_id])  # copy
        # get address (may have been updated)
        with self.lock:
            addr = self.membership.get(target_node_id, {}).get("address", None)
        if not addr:
            print(f"[{self.node_id}] No address known for {target_node_id}, cannot deliver hints")
            return

        try:
            ch = grpc.insecure_channel(addr)
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)

            req = dynamo_pb2.DeliverHintsRequest(target_node_id=target_node_id, hints=hints_to_send)
            reply = stub.DeliverHints(req, timeout=5.0)
            if reply.success:
                # remove delivered hints
                with self.lock:
                    self.hints[target_node_id] = []
                print(f"[{self.node_id}] Delivered {len(hints_to_send)} hints to {target_node_id}")
            else:
                print(f"[{self.node_id}] DeliverHints NACK: {reply.message}")
        except Exception as e:
            print(f"[{self.node_id}] Failed to deliver hints to {target_node_id}: {e}")

    # ---------------------------
    # Gossip background loop
    # ---------------------------
    def _gossip_loop(self):
        time.sleep(0.5)  # brief startup delay
        while True:
            try:
                # Update our own timestamp periodically
                now_ms = int(time.time() * 1000)
                with self.lock:
                    self.membership[self.node_id]["last_ts"] = now_ms
                
                # pick a random peer (not self) that we know an address for
                with self.lock:
                    peers = [nid for nid, info in self.membership.items() 
                            if nid != self.node_id and info.get("address")]
                
                if not peers:
                    time.sleep(GOSSIP_INTERVAL)
                    continue

                peer = random.choice(peers)
                peer_addr = None
                with self.lock:
                    peer_addr = self.membership[peer]["address"]

                # build MembershipList message
                out = dynamo_pb2.MembershipList()
                with self.lock:
                    for nid, cur in self.membership.items():
                        ns = dynamo_pb2.NodeState(
                            node_id=nid,
                            address=cur["address"],
                            generation=cur["generation"],
                            last_updated_time=cur["last_ts"],
                            up=cur["up"]
                        )
                        out.members.append(ns)

                # send gossip
                try:
                    ch = grpc.insecure_channel(peer_addr)
                    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
                    resp = stub.Gossip(out, timeout=2.0)
                    # merge response
                    self.Gossip(resp, None)
                except Exception as e:
                    # Log gossip failure but don't crash
                    print(f"[{self.node_id}] Gossip to {peer} failed: {e}")

            except Exception as e:
                print(f"[{self.node_id}] Exception in gossip loop: {e}")

            time.sleep(GOSSIP_INTERVAL)

# ====================================================
#   SERVER STARTER
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

    # Each server loads/creates its own ring (static for now)
    ring = DHTRing(vnode_count=10)
    ring.add_node("node-1", "localhost:50051")
    ring.add_node("node-2", "localhost:50052")
    ring.add_node("node-3", "localhost:50053")

    serve(node_id, port, ring)
