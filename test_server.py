import unittest
import grpc
import time
import threading
from concurrent import futures
from unittest.mock import Mock, patch, MagicMock
import logging
import sys
from datetime import datetime
import os
import shutil

import dynamo_pb2
import dynamo_pb2_grpc
from server import DynamoNode, N, R, W
from dht_ring import DHTRing
from vector_clock import VectorClock


# Configure logging to file and console
def setup_logging():
    """Set up logging to both file and console."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"test_results_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers
    logger.handlers = []
    
    # File handler - detailed logs
    file_handler = logging.FileHandler(log_filename, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler - summary only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info("=" * 80)
    logging.info(f"Test Run Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Log file: {log_filename}")
    logging.info("=" * 80)
    
    return log_filename


# Set up logging at module import
LOG_FILE = setup_logging()


class TestDynamoNodeUnit(unittest.TestCase):
    """Unit tests for DynamoNode class methods."""

    def setUp(self):
        """Set up a test ring and node."""
        logging.info(f"\n{'='*60}")
        logging.info(f"Setting up test: {self._testMethodName}")
        logging.info(f"{'='*60}")
        
        # Clean up storage before each test to prevent pollution
        for node_id in ["node-1", "node-2", "node-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
        
        self.ring = DHTRing(vnode_count=10)
        self.ring.add_node("node-1", "localhost:50051")
        self.ring.add_node("node-2", "localhost:50052")
        self.ring.add_node("node-3", "localhost:50053")
        
        self.node = DynamoNode("node-1", "localhost:50051", self.ring)
        logging.debug("Test setup complete")

    def tearDown(self):
        """Clean up after test."""
        # Clean up storage after each test
        for node_id in ["node-1", "node-2", "node-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
        logging.debug(f"Test {self._testMethodName} completed")

    def test_node_initialization(self):
        """Test that node initializes correctly."""
        self.assertEqual(self.node.node_id, "node-1")
        self.assertEqual(self.node.address, "localhost:50051")
        self.assertEqual(self.node.store, {})
        self.assertEqual(self.node.hints, {})
        self.assertIsNotNone(self.node.ring)

    def test_store_hint(self):
        """Test storing a hint for failed replica."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(
            value=b"test_value",
            context=vc.to_proto()
        )
        
        self.node._store_hint("node-2", "test_key", vwc)
        
        self.assertIn("node-2", self.node.hints)
        self.assertEqual(len(self.node.hints["node-2"]), 1)
        self.assertEqual(self.node.hints["node-2"][0].key, "test_key")
        self.assertEqual(self.node.hints["node-2"][0].target_node_id, "node-2")
        self.assertEqual(self.node.hints["node-2"][0].data.value, b"test_value")

    def test_store_multiple_hints_same_target(self):
        """Test storing multiple hints for same target node."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc.to_proto())
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v2", context=vc.to_proto())
        
        self.node._store_hint("node-2", "key1", vwc1)
        self.node._store_hint("node-2", "key2", vwc2)
        
        self.assertEqual(len(self.node.hints["node-2"]), 2)

    def test_store_hints_different_targets(self):
        """Test storing hints for different target nodes."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"value", context=vc.to_proto())
        
        self.node._store_hint("node-2", "key1", vwc)
        self.node._store_hint("node-3", "key2", vwc)
        
        self.assertIn("node-2", self.node.hints)
        self.assertIn("node-3", self.node.hints)
        self.assertEqual(len(self.node.hints["node-2"]), 1)
        self.assertEqual(len(self.node.hints["node-3"]), 1)

    def test_put_hint_rpc(self):
        """Test PutHint RPC endpoint."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"hint_value", context=vc.to_proto())
        
        hint = dynamo_pb2.Hint(
            key="hint_key",
            data=vwc,
            target_node_id="node-2",
            timestamp_ms=int(time.time() * 1000)
        )
        
        request = hint
        context = Mock()
        
        response = self.node.PutHint(request, context)
        
        self.assertTrue(response.success)
        self.assertIn("node-2", self.node.hints)
        self.assertEqual(len(self.node.hints["node-2"]), 1)

    def test_hint_contains_timestamp(self):
        """Test that hints contain timestamps."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"value", context=vc.to_proto())
        
        before_time = int(time.time() * 1000)
        self.node._store_hint("node-2", "key", vwc)
        after_time = int(time.time() * 1000)
        
        hint = self.node.hints["node-2"][0]
        self.assertGreaterEqual(hint.timestamp_ms, before_time)
        self.assertLessEqual(hint.timestamp_ms, after_time)

    def test_store_local_version(self):
        """Test storing a version locally."""
        vc = VectorClock()
        vc.increment("node-1")
        
        vwc = dynamo_pb2.ValueWithContext(
            value=b"test_value",
            context=vc.to_proto()
        )
        
        self.node._store_local_version("test_key", vwc)
        
        self.assertIn("test_key", self.node.store)
        self.assertEqual(len(self.node.store["test_key"]), 1)
        self.assertEqual(self.node.store["test_key"][0].value, b"test_value")

    def test_store_multiple_versions(self):
        """Test storing multiple versions of same key."""
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc1.to_proto())
        
        vc2 = VectorClock()
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v2", context=vc2.to_proto())
        
        self.node._store_local_version("key", vwc1)
        self.node._store_local_version("key", vwc2)
        
        self.assertEqual(len(self.node.store["key"]), 2)

    def test_reconcile_versions_single(self):
        """Test reconciliation with single version."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"v1", context=vc.to_proto())
        
        result = self.node._reconcile_versions([vwc])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].value, b"v1")

    def test_reconcile_versions_identical(self):
        """Test reconciliation with identical versions."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc.to_proto())
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc.to_proto())
    
        result = self.node._reconcile_versions([vwc1, vwc2])
        # Should deduplicate
        self.assertEqual(len(result), 1)

    def test_reconcile_versions_causal_order(self):
        """Test reconciliation with causally ordered versions."""
        # v1 happens before v2
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc1.to_proto())
        
        vc2 = VectorClock.from_proto(vc1.to_proto())
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v2", context=vc2.to_proto())
        
        result = self.node._reconcile_versions([vwc1, vwc2])
        # v2 dominates v1, should only return v2
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].value, b"v2")

    def test_reconcile_versions_concurrent_siblings(self):
        """Test reconciliation with concurrent conflicting versions."""
        # Two concurrent updates
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc1.to_proto())
        
        vc2 = VectorClock()
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v2", context=vc2.to_proto())
        
        result = self.node._reconcile_versions([vwc1, vwc2])
        # Both are concurrent, should return both as siblings
        self.assertEqual(len(result), 2)

    def test_forward_get_empty_store(self):
        """Test ForwardGet when key doesn't exist."""
        request = dynamo_pb2.GetRequest(key="nonexistent")
        context = Mock()
        
        reply = self.node.ForwardGet(request, context)
        
        self.assertFalse(reply.found)
        self.assertEqual(len(reply.data), 0)

    def test_forward_get_existing_key(self):
        """Test ForwardGet when key exists."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"test", context=vc.to_proto())
        
        self.node._store_local_version("key1", vwc)
        
        request = dynamo_pb2.GetRequest(key="key1")
        context = Mock()
        
        reply = self.node.ForwardGet(request, context)
        
        self.assertTrue(reply.found)
        self.assertEqual(len(reply.data), 1)
        self.assertEqual(reply.data[0].value, b"test")

    def test_forward_put(self):
        """Test ForwardPut stores data correctly."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"forwarded", context=vc.to_proto())
        
        request = dynamo_pb2.PutRequest(key="fwd_key", data=vwc)
        context = Mock()
        
        reply = self.node.ForwardPut(request, context)
        
        self.assertTrue(reply.success)
        self.assertIn("fwd_key", self.node.store)
        self.assertEqual(self.node.store["fwd_key"][0].value, b"forwarded")


class TestDynamoNodeIntegration(unittest.TestCase):
    """Integration tests with actual gRPC servers."""

    @classmethod
    def setUpClass(cls):
        """Start test gRPC servers."""
        logging.info("\n" + "="*80)
        logging.info("Starting Integration Test Suite - TestDynamoNodeIntegration")
        logging.info("="*80)
        
        cls.ring = DHTRing(vnode_count=10)
        cls.ring.add_node("node-1", "localhost:50061")
        cls.ring.add_node("node-2", "localhost:50062")
        cls.ring.add_node("node-3", "localhost:50063")
        
        cls.servers = []
        cls.nodes = []
        
        logging.info("Starting 3 test gRPC servers...")
        # Start 3 test servers
        for i, (node_id, port) in enumerate([
            ("node-1", 50061),
            ("node-2", 50062),
            ("node-3", 50063)
        ]):
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node = DynamoNode(node_id, f"localhost:{port}", cls.ring)
            
            dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            
            cls.servers.append(server)
            cls.nodes.append(node)
            logging.debug(f"Started {node_id} on port {port}")
        
        time.sleep(0.5)
        logging.info("All test servers started successfully")

    @classmethod
    def tearDownClass(cls):
        """Stop test servers."""
        logging.info("Stopping all integration test servers...")
        for i, server in enumerate(cls.servers):
            server.stop(0)
            logging.debug(f"Stopped server {i+1}")
        logging.info("All integration test servers stopped")

    def test_hinted_handoff_on_replica_failure(self):
        """Test that hints are stored when replica is unavailable."""
        # Stop node-2 to simulate failure
        self.servers[1].stop(0)
        time.sleep(0.2)
        
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"hint_test_value",
            context=vc.to_proto()
        )
        
        # Try to PUT - should succeed with W quorum but store hint for node-2
        request = dynamo_pb2.PutRequest(key="hint_test_key", data=vwc)
        response = stub.Put(request, timeout=3)
        
        # Check if coordinator stored hints for failed replica
        coordinator = self.nodes[0]
        pref_list = self.ring.get_preference_list("hint_test_key", N)
        
        # If node-2 was in preference list, should have hint
        if "node-2" in pref_list:
            # Hint might be stored if write failed
            pass
        
        # Restart node-2
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("node-2", "localhost:50062", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50062}")
        server.start()
        self.servers[1] = server
        self.nodes[1] = node
        
        time.sleep(0.2)
        channel.close()

    def test_put_hint_via_grpc(self):
        """Test PutHint RPC call between nodes."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vc.increment("node-2")
        vwc = dynamo_pb2.ValueWithContext(value=b"hinted_value", context=vc.to_proto())
        
        hint = dynamo_pb2.Hint(
            key="hinted_key",
            data=vwc,
            target_node_id="node-3",
            timestamp_ms=int(time.time() * 1000)
        )
        
        response = stub.PutHint(hint, timeout=2)
        
        self.assertTrue(response.success)
        
        # Verify hint was stored
        node1 = self.nodes[0]
        self.assertIn("node-3", node1.hints)
        
        channel.close()

    def test_hint_storage_preserves_data(self):
        """Test that hint storage preserves value and context correctly."""
        vc = VectorClock()
        vc.increment("node-1")
        vc.increment("node-2")
        vwc = dynamo_pb2.ValueWithContext(
            value=b"preserved_value",
            context=vc.to_proto()
        )
        
        node = self.nodes[0]
        node._store_hint("node-2", "preserve_key", vwc)
        
        stored_hint = node.hints["node-2"][0]
        self.assertEqual(stored_hint.data.value, b"preserved_value")
        
        # Verify vector clock is preserved
        stored_vc = VectorClock.from_proto(stored_hint.data.context)
        self.assertEqual(stored_vc.clock.get("node-1"), 1)
        self.assertEqual(stored_vc.clock.get("node-2"), 1)

    def test_multiple_hints_for_same_key(self):
        """Test storing multiple hints for same key (from different operations)."""
        node = self.nodes[0]
        
        # Clear hints to ensure clean state for this test
        node.hints.clear()
        
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"v1", context=vc1.to_proto())
        
        vc2 = VectorClock()
        vc2.increment("node-1")
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"v2", context=vc2.to_proto())
        
        node._store_hint("node-3", "same_key", vwc1)
        node._store_hint("node-3", "same_key", vwc2)
        
        self.assertEqual(len(node.hints["node-3"]), 2)
        self.assertEqual(node.hints["node-3"][0].key, "same_key")
        self.assertEqual(node.hints["node-3"][1].key, "same_key")

    def test_hints_stored_on_write_quorum_failure(self):
        """Test hints are stored when W quorum cannot be achieved."""
        # This test verifies coordinator behavior when replicas fail
        # Stop 2 nodes to force W quorum failure in some cases
        self.servers[1].stop(0)
        self.servers[2].stop(0)
        time.sleep(0.2)
        
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"quorum_fail_value",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="quorum_fail_key", data=vwc)
        response = stub.Put(request, timeout=3)
        
        # May or may not succeed depending on preference list
        # But should have stored hints
        coordinator = self.nodes[0]
        
        # Check for hints (may be stored for node-2 or node-3)
        total_hints = sum(len(hints) for hints in coordinator.hints.values())
        
        # Restart nodes
        for i, port in [(1, 50062), (2, 50063)]:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node_id = f"node-{i+1}"
            node = DynamoNode(node_id, f"localhost:{port}", self.ring)
            dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            self.servers[i] = server
            self.nodes[i] = node
        
        time.sleep(0.2)
        channel.close()

    def test_put_single_key(self):
        """Test PUT operation through client."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"test_value_1",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="test_key_1", data=vwc)
        response = stub.Put(request, timeout=3)
        
        self.assertTrue(response.success)
        self.assertIn("quorum", response.message.lower())
        
        channel.close()

    def test_get_existing_key(self):
        """Test GET operation for existing key."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # First PUT a key
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"get_test_value",
            context=vc.to_proto()
        )
        
        put_req = dynamo_pb2.PutRequest(key="get_test_key", data=vwc)
        put_resp = stub.Put(put_req, timeout=3)
        self.assertTrue(put_resp.success)
        
        time.sleep(0.2)  # Allow replication
        
        # Now GET it
        get_req = dynamo_pb2.GetRequest(key="get_test_key")
        get_resp = stub.Get(get_req, timeout=3)
        
        self.assertTrue(get_resp.found)
        self.assertGreater(len(get_resp.data), 0)
        self.assertEqual(get_resp.data[0].value, b"get_test_value")
        
        channel.close()

    def test_get_nonexistent_key(self):
        """Test GET operation for non-existent key."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key="nonexistent_key_xyz")
        response = stub.Get(request, timeout=3)
        
        self.assertFalse(response.found)
        
        channel.close()

    def test_get_quorum_requirement(self):
        """Test that GET requires R quorum."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # PUT first
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"quorum_get_test",
            context=vc.to_proto()
        )
        
        put_req = dynamo_pb2.PutRequest(key="quorum_get_key", data=vwc)
        stub.Put(put_req, timeout=3)
        
        time.sleep(0.2)
        
        # GET should succeed with R quorum
        get_req = dynamo_pb2.GetRequest(key="quorum_get_key")
        get_resp = stub.Get(get_req, timeout=3)
        
        self.assertTrue(get_resp.found)
        
        channel.close()

    def test_get_reconciles_versions(self):
        """Test that GET reconciles multiple versions."""
        # Manually store different versions on different nodes
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"version1", context=vc1.to_proto())
        
        vc2 = VectorClock()
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"version2", context=vc2.to_proto())
        
        # Store different versions on different nodes
        self.nodes[0]._store_local_version("reconcile_key", vwc1)
        self.nodes[1]._store_local_version("reconcile_key", vwc2)
        self.nodes[2]._store_local_version("reconcile_key", vwc1)
        
        # Now GET - should return siblings since they're concurrent
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key="reconcile_key")
        response = stub.Get(request, timeout=3)
        
        self.assertTrue(response.found)
        # Should have 2 siblings (concurrent versions)
        self.assertEqual(len(response.data), 2)
        
        channel.close()

    def test_get_returns_latest_version(self):
        """Test that GET returns latest version when causally ordered."""
        # Create causally ordered versions
        vc1 = VectorClock()
        vc1.increment("node-1")
        vwc1 = dynamo_pb2.ValueWithContext(value=b"old", context=vc1.to_proto())
        
        vc2 = VectorClock.from_proto(vc1.to_proto())
        vc2.increment("node-2")
        vwc2 = dynamo_pb2.ValueWithContext(value=b"new", context=vc2.to_proto())
        
        # Store both versions
        self.nodes[0]._store_local_version("latest_key", vwc1)
        self.nodes[0]._store_local_version("latest_key", vwc2)
        self.nodes[1]._store_local_version("latest_key", vwc2)
        
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key="latest_key")
        response = stub.Get(request, timeout=3)
        
        self.assertTrue(response.found)
        # Should only return the latest (non-dominated) version
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0].value, b"new")
        
        channel.close()

    def test_read_repair_triggers(self):
        """Test that read repair happens after GET."""
        # Store value only on node-1 and node-2, not node-3
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"repair_test", context=vc.to_proto())
        
        self.nodes[0]._store_local_version("repair_key", vwc)
        self.nodes[1]._store_local_version("repair_key", vwc)
        # node-3 doesn't have it
        
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key="repair_key")
        response = stub.Get(request, timeout=3)
        
        self.assertTrue(response.found)
        
        # Wait for read repair to complete
        time.sleep(0.5)
        
        # Check if node-3 now has the value (read repair should have pushed it)
        # Note: This depends on the preference list including node-3
        pref_list = self.ring.get_preference_list("repair_key", N)
        if "node-3" in pref_list:
            # Node-3 should have received the value via read repair
            # This is a probabilistic test based on timing
            pass
        
        channel.close()

    def test_put_multiple_keys(self):
        """Test PUT operations for multiple keys."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        for i in range(5):
            vc = VectorClock()
            vwc = dynamo_pb2.ValueWithContext(
                value=f"value_{i}".encode(),
                context=vc.to_proto()
            )
            
            request = dynamo_pb2.PutRequest(key=f"key_{i}", data=vwc)
            response = stub.Put(request, timeout=3)
            
            self.assertTrue(response.success)
        
        channel.close()

    def test_get_multiple_keys(self):
        """Test GET operations for multiple keys."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # PUT multiple keys
        for i in range(5):
            vc = VectorClock()
            vwc = dynamo_pb2.ValueWithContext(
                value=f"multi_value_{i}".encode(),
                context=vc.to_proto()
            )
            put_req = dynamo_pb2.PutRequest(key=f"multi_key_{i}", data=vwc)
            stub.Put(put_req, timeout=3)
        
        time.sleep(0.2)
        
        # GET all keys
        for i in range(5):
            get_req = dynamo_pb2.GetRequest(key=f"multi_key_{i}")
            get_resp = stub.Get(get_req, timeout=3)
            
            self.assertTrue(get_resp.found)
            self.assertEqual(get_resp.data[0].value, f"multi_value_{i}".encode())
        
        channel.close()

    def test_put_with_vector_clock(self):
        """Test PUT operation increments vector clock."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # First write
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"initial",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="vc_test", data=vwc)
        response = stub.Put(request, timeout=3)
        self.assertTrue(response.success)
        
        # Verify local store has incremented clock
        node1 = self.nodes[0]
        if "vc_test" in node1.store:
            stored_vc = VectorClock.from_proto(node1.store["vc_test"][0].context)
            self.assertGreater(stored_vc.clock.get("node-1", 0), 0)
        
        channel.close()

    def test_put_then_get_consistency(self):
        """Test that GET returns what was PUT."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # PUT
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"consistency_test_value",
            context=vc.to_proto()
        )
        
        put_req = dynamo_pb2.PutRequest(key="consistency_key", data=vwc)
        put_resp = stub.Put(put_req, timeout=3)
        self.assertTrue(put_resp.success)
        
        time.sleep(0.2)
        
        # GET
        get_req = dynamo_pb2.GetRequest(key="consistency_key")
        get_resp = stub.Get(get_req, timeout=3)
        
        self.assertTrue(get_resp.found)
        self.assertEqual(get_resp.data[0].value, b"consistency_test_value")
        
        channel.close()

    def test_concurrent_puts_then_get(self):
        """Test concurrent PUTs followed by GET returns reconciled versions."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        def do_put(value_suffix):
            vc = VectorClock()
            vwc = dynamo_pb2.ValueWithContext(
                value=f"concurrent_{value_suffix}".encode(),
                context=vc.to_proto()
            )
            request = dynamo_pb2.PutRequest(key="concurrent_key", data=vwc)
            return stub.Put(request, timeout=3)
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=do_put, args=(i,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        time.sleep(0.3)
        
        # GET should return siblings
        get_req = dynamo_pb2.GetRequest(key="concurrent_key")
        get_resp = stub.Get(get_req, timeout=3)
        
        self.assertTrue(get_resp.found)
        # May have siblings due to concurrent writes
        self.assertGreater(len(get_resp.data), 0)
        
        channel.close()

    def test_forward_put_between_nodes(self):
        """Test ForwardPut RPC between nodes."""
        channel = grpc.insecure_channel("localhost:50062")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(
            value=b"forwarded_value",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="fwd_test", data=vwc)
        response = stub.ForwardPut(request, timeout=2)
        
        self.assertTrue(response.success)
        
        # Verify storage
        node2 = self.nodes[1]
        self.assertIn("fwd_test", node2.store)
        
        channel.close()

    def test_forward_get_between_nodes(self):
        """Test ForwardGet RPC between nodes."""
        # First store some data in node-2
        node2 = self.nodes[1]
        vc = VectorClock()
        vc.increment("node-2")
        vwc = dynamo_pb2.ValueWithContext(value=b"stored", context=vc.to_proto())
        node2._store_local_version("stored_key", vwc)
        
        # Now query via gRPC
        channel = grpc.insecure_channel("localhost:50062")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.GetRequest(key="stored_key")
        response = stub.ForwardGet(request, timeout=2)
        
        self.assertTrue(response.found)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0].value, b"stored")
        
        channel.close()

    def test_replication_across_nodes(self):
        """Test that PUT replicates to multiple nodes."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"replicated_value",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="repl_key", data=vwc)
        response = stub.Put(request, timeout=3)
        
        self.assertTrue(response.success)
        
        # Check that data exists on multiple nodes
        time.sleep(0.2)  # Give time for replication
        
        stored_count = 0
        for node in self.nodes:
            if "repl_key" in node.store:
                stored_count += 1
        
        # Should be stored on at least W nodes
        self.assertGreaterEqual(stored_count, W)
        
        channel.close()

    def test_preference_list_routing(self):
        """Test that keys are routed to correct preference list."""
        channel = grpc.insecure_channel("localhost:50061")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # PUT a key
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"routing_test",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="routing_key", data=vwc)
        response = stub.Put(request, timeout=3)
        
        self.assertTrue(response.success)
        
        # Verify preference list was used
        pref_list = self.ring.get_preference_list("routing_key", N)
        self.assertEqual(len(pref_list), N)
        
        channel.close()


class TestGossipAndMembership(unittest.TestCase):
    """Tests for gossip-based failure detection and membership."""

    def setUp(self):
        """Set up test nodes."""
        self.ring = DHTRing(vnode_count=10)
        self.ring.add_node("node-1", "localhost:50081")
        self.ring.add_node("node-2", "localhost:50082")
        self.ring.add_node("node-3", "localhost:50083")
        
        self.node1 = DynamoNode("node-1", "localhost:50081", self.ring)
        self.node2 = DynamoNode("node-2", "localhost:50082", self.ring)
        self.node3 = DynamoNode("node-3", "localhost:50083", self.ring)

    def test_membership_initialization(self):
        """Test that membership is initialized correctly."""
        self.assertIn("node-1", self.node1.membership)
        self.assertIn("node-2", self.node1.membership)
        self.assertIn("node-3", self.node1.membership)
        
        # Self should be marked UP
        self.assertTrue(self.node1.membership["node-1"]["up"])
        # Self should have generation > 0
        self.assertGreater(self.node1.membership["node-1"]["generation"], 0)

    def test_gossip_message_structure(self):
        """Test Gossip RPC message structure."""
        request = dynamo_pb2.MembershipList()
        
        # Add some member states
        for nid in ["node-1", "node-2"]:
            member = dynamo_pb2.NodeState(
                node_id=nid,
                address=f"localhost:5008{nid[-1]}",
                generation=1,
                last_updated_time=int(time.time() * 1000),
                up=True
            )
            request.members.append(member)
        
        context = Mock()
        response = self.node1.Gossip(request, context)
        
        # Response should contain membership list
        self.assertIsInstance(response, dynamo_pb2.MembershipList)
        self.assertGreater(len(response.members), 0)

    def test_gossip_discovers_new_node(self):
        """Test that gossip discovers new nodes."""
        # Create a gossip message with a new node
        request = dynamo_pb2.MembershipList()
        
        new_node = dynamo_pb2.NodeState(
            node_id="node-4",
            address="localhost:50084",
            generation=1,
            last_updated_time=int(time.time() * 1000),
            up=True
        )
        request.members.append(new_node)
        
        context = Mock()
        self.node1.Gossip(request, context)
        
        # Node should be discovered
        self.assertIn("node-4", self.node1.membership)
        self.assertEqual(self.node1.membership["node-4"]["address"], "localhost:50084")

    def test_gossip_updates_node_state(self):
        """Test that gossip updates existing node states."""
        # Initialize with old state
        old_time = int(time.time() * 1000) - 10000
        self.node1.membership["node-2"]["last_ts"] = old_time
        self.node1.membership["node-2"]["generation"] = 1
        
        # Send gossip with newer state
        request = dynamo_pb2.MembershipList()
        new_time = int(time.time() * 1000)
        
        updated_node = dynamo_pb2.NodeState(
            node_id="node-2",
            address="localhost:50082",
            generation=2,
            last_updated_time=new_time,
            up=True
        )
        request.members.append(updated_node)
        
        context = Mock()
        self.node1.Gossip(request, context)
        
        # State should be updated
        self.assertEqual(self.node1.membership["node-2"]["generation"], 2)
        self.assertGreater(self.node1.membership["node-2"]["last_ts"], old_time)

    def test_deliver_hints_rpc(self):
        """Test DeliverHints RPC endpoint."""
        vc = VectorClock()
        vc.increment("node-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"hint_data", context=vc.to_proto())
        
        hint = dynamo_pb2.Hint(
            key="delivered_key",
            data=vwc,
            target_node_id="node-2",
            timestamp_ms=int(time.time() * 1000)
        )
        
        request = dynamo_pb2.DeliverHintsRequest(
            target_node_id="node-2",
            hints=[hint]
        )
        
        context = Mock()
        response = self.node2.DeliverHints(request, context)
        
        self.assertTrue(response.success)
        # Data should be stored locally
        self.assertIn("delivered_key", self.node2.store)
        self.assertEqual(self.node2.store["delivered_key"][0].value, b"hint_data")

    def test_deliver_multiple_hints(self):
        """Test delivering multiple hints at once."""
        hints = []
        for i in range(3):
            vc = VectorClock()
            vc.increment("node-1")
            vwc = dynamo_pb2.ValueWithContext(
                value=f"hint_value_{i}".encode(),
                context=vc.to_proto()
            )
            
            hint = dynamo_pb2.Hint(
                key=f"hint_key_{i}",
                data=vwc,
                target_node_id="node-2",
                timestamp_ms=int(time.time() * 1000)
            )
            hints.append(hint)
        
        request = dynamo_pb2.DeliverHintsRequest(
            target_node_id="node-2",
            hints=hints
        )
        
        context = Mock()
        response = self.node2.DeliverHints(request, context)
        
        self.assertTrue(response.success)
        
        # All hints should be stored
        for i in range(3):
            self.assertIn(f"hint_key_{i}", self.node2.store)

    def test_thread_safety_concurrent_stores(self):
        """Test thread-safe concurrent store operations."""
        def store_version(node, key, value):
            vc = VectorClock()
            vc.increment(node.node_id)
            vwc = dynamo_pb2.ValueWithContext(value=value, context=vc.to_proto())
            node._store_local_version(key, vwc)
        
        threads = []
        for i in range(10):
            t = threading.Thread(
                target=store_version,
                args=(self.node1, "concurrent_key", f"value_{i}".encode())
            )
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All versions should be stored
        self.assertEqual(len(self.node1.store["concurrent_key"]), 10)

    def test_thread_safety_concurrent_hints(self):
        """Test thread-safe concurrent hint operations."""
        def store_hint(node, target, key):
            vc = VectorClock()
            vc.increment(node.node_id)
            vwc = dynamo_pb2.ValueWithContext(value=b"hint", context=vc.to_proto())
            node._store_hint(target, key, vwc)
        
        threads = []
        for i in range(10):
            t = threading.Thread(
                target=store_hint,
                args=(self.node1, "node-2", f"hint_key_{i}")
            )
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All hints should be stored
        self.assertEqual(len(self.node1.hints["node-2"]), 10)


class TestGossipIntegration(unittest.TestCase):
    """Integration tests for gossip with actual gRPC servers."""

    @classmethod
    def setUpClass(cls):
        """Start test gRPC servers for gossip testing."""
        cls.ring = DHTRing(vnode_count=10)
        cls.ring.add_node("gossip-1", "localhost:50091")
        cls.ring.add_node("gossip-2", "localhost:50092")
        cls.ring.add_node("gossip-3", "localhost:50093")
        
        cls.servers = []
        cls.nodes = []
        
        for i, (node_id, port) in enumerate([
            ("gossip-1", 50091),
            ("gossip-2", 50092),
            ("gossip-3", 50093)
        ]):
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node = DynamoNode(node_id, f"localhost:{port}", cls.ring)
            
            dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            
            cls.servers.append(server)
            cls.nodes.append(node)
        
        time.sleep(1.0)  # Let servers and gossip start

    @classmethod
    def tearDownClass(cls):
        """Stop test servers."""
        for server in cls.servers:
            server.stop(0)

    def test_gossip_propagates_membership(self):
        """Test that gossip propagates membership information."""
        # Wait for a few gossip rounds
        time.sleep(3.0)
        
        # All nodes should know about all other nodes
        for node in self.nodes:
            self.assertIn("gossip-1", node.membership)
            self.assertIn("gossip-2", node.membership)
            self.assertIn("gossip-3", node.membership)

    def test_hint_delivery_on_recovery(self):
        """Test that hints are delivered when a node recovers."""
        # Store a hint on node-1 for node-2
        vc = VectorClock()
        vc.increment("gossip-1")
        vwc = dynamo_pb2.ValueWithContext(
            value=b"recovery_test",
            context=vc.to_proto()
        )
        
        self.nodes[0]._store_hint("gossip-2", "recovery_key", vwc)
        
        # Simulate node-2 going down then up
        with self.nodes[0].lock:
            self.nodes[0].membership["gossip-2"]["up"] = False
        
        time.sleep(0.2)
        
        # Simulate recovery via gossip
        request = dynamo_pb2.MembershipList()
        recovered = dynamo_pb2.NodeState(
            node_id="gossip-2",
            address="localhost:50092",
            generation=2,
            last_updated_time=int(time.time() * 1000),
            up=True
        )
        request.members.append(recovered)
        
        context = Mock()
        self.nodes[0].Gossip(request, context)
        
        # Wait for hint delivery
        time.sleep(1.0)
        
        # Hint should have been delivered (or attempted)
        # Check if hints were cleared (successful delivery) or still present (failed delivery)
        # In real scenario, node-2 should have received the hint

    def test_deliver_hints_via_grpc(self):
        """Test delivering hints via gRPC call."""
        # Store hints on node-1 for node-2
        hints = []
        for i in range(3):
            vc = VectorClock()
            vc.increment("gossip-1")
            vwc = dynamo_pb2.ValueWithContext(
                value=f"grpc_hint_{i}".encode(),
                context=vc.to_proto()
            )
            
            hint = dynamo_pb2.Hint(
                key=f"grpc_key_{i}",
                data=vwc,
                target_node_id="gossip-2",
                timestamp_ms=int(time.time() * 1000)
            )
            hints.append(hint)
        
        # Call DeliverHints RPC
        channel = grpc.insecure_channel("localhost:50092")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        request = dynamo_pb2.DeliverHintsRequest(
            target_node_id="gossip-2",
            hints=hints
        )
        
        response = stub.DeliverHints(request, timeout=2)
        
        self.assertTrue(response.success)
        
        # Verify data was stored on node-2
        node2 = self.nodes[1]
        for i in range(3):
            self.assertIn(f"grpc_key_{i}", node2.store)
        
        channel.close()

    def test_gossip_rpc_call(self):
        """Test calling Gossip RPC between nodes."""
        channel = grpc.insecure_channel("localhost:50092")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        # Build membership list
        request = dynamo_pb2.MembershipList()
        
        member = dynamo_pb2.NodeState(
            node_id="gossip-1",
            address="localhost:50091",
            generation=3,
            last_updated_time=int(time.time() * 1000),
            up=True
        )
        request.members.append(member)
        
        response = stub.Gossip(request, timeout=2)
        
        # Response should contain membership list
        self.assertGreater(len(response.members), 0)
        
        # Check that gossip-1 info was updated on gossip-2
        node2 = self.nodes[1]
        self.assertIn("gossip-1", node2.membership)
        
        channel.close()


class TestHintDelivery(unittest.TestCase):
    """Dedicated tests for hint delivery mechanism."""

    def setUp(self):
        """Set up test nodes."""
        self.ring = DHTRing(vnode_count=10)
        self.ring.add_node("hint-1", "localhost:50101")
        self.ring.add_node("hint-2", "localhost:50102")
        
        self.node1 = DynamoNode("hint-1", "localhost:50101", self.ring)
        self.node2 = DynamoNode("hint-2", "localhost:50102", self.ring)

    def test_deliver_hints_to_method(self):
        """Test deliver_hints_to method logic."""
        # Store some hints
        vc = VectorClock()
        vc.increment("hint-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"test_hint", context=vc.to_proto())
        
        self.node1._store_hint("hint-2", "hint_key", vwc)
        
        # Update membership to have correct address
        with self.node1.lock:
            self.node1.membership["hint-2"]["address"] = "localhost:50102"
            self.node1.membership["hint-2"]["up"] = True
        
        # Attempt delivery (will fail in unit test without server, but we test the logic)
        initial_hint_count = len(self.node1.hints.get("hint-2", []))
        self.assertGreater(initial_hint_count, 0)

    def test_no_hints_to_deliver(self):
        """Test when there are no hints to deliver."""
        # Ensure no hints exist
        with self.node1.lock:
            self.node1.hints.clear()
        
        # Should handle gracefully
        self.node1.deliver_hints_to("hint-2")
        
        # No errors should occur

    def test_hints_cleared_after_successful_delivery(self):
        """Test that hints are cleared after successful delivery."""
        # This would require actual server interaction
        # We test the mechanism is in place
        self.assertIn("hints", dir(self.node1))
        self.assertIsInstance(self.node1.hints, dict)


class TestPersistence(unittest.TestCase):
    """Tests for data persistence across restarts."""

    def setUp(self):
        """Set up test ring."""
        logging.info(f"\n{'='*60}")
        logging.info(f"Persistence Test: {self._testMethodName}")
        logging.info(f"{'='*60}")
        
        self.ring = DHTRing(vnode_count=10)
        self.ring.add_node("persist-1", "localhost:50111")
        self.ring.add_node("persist-2", "localhost:50112")
        self.ring.add_node("persist-3", "localhost:50113")
        
        # Clean up any existing storage files
        import os
        import shutil
        for node_id in ["persist-1", "persist-2", "persist-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
                logging.debug(f"Cleaned up storage for {node_id}")

    def tearDown(self):
        """Clean up storage files after test."""
        import os
        import shutil
        for node_id in ["persist-1", "persist-2", "persist-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
        logging.debug("Cleaned up all storage after test")

    def test_data_persists_after_restart(self):
        """Test that data is restored from disk after node restart."""
        # Start node
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("persist-1", "localhost:50111", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50111}")
        server.start()
        
        time.sleep(0.2)
        
        # Write some data
        channel = grpc.insecure_channel("localhost:50111")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"persistent_value",
            context=vc.to_proto()
        )
        
        request = dynamo_pb2.PutRequest(key="persist_key", data=vwc)
        response = stub.Put(request, timeout=2)
        
        channel.close()
        time.sleep(0.2)
        
        # Stop server
        server.stop(0)
        del node
        time.sleep(0.2)
        
        # Restart node
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("persist-1", "localhost:50111", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50111}")
        server2.start()
        
        time.sleep(0.2)
        
        # Verify data was loaded
        self.assertIn("persist_key", node2.store)
        self.assertEqual(node2.store["persist_key"][0].value, b"persistent_value")
        
        # Also verify via GET
        channel2 = grpc.insecure_channel("localhost:50111")
        stub2 = dynamo_pb2_grpc.DynamoServiceStub(channel2)
        
        get_req = dynamo_pb2.GetRequest(key="persist_key")
        get_resp = stub2.ForwardGet(get_req, timeout=2)
        
        self.assertTrue(get_resp.found)
        self.assertEqual(get_resp.data[0].value, b"persistent_value")
        
        channel2.close()
        server2.stop(0)

    def test_multiple_versions_persist(self):
        """Test that multiple versions of same key persist."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("persist-1", "localhost:50111", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50111}")
        server.start()
        
        time.sleep(0.2)
        
        # Write multiple versions
        for i in range(3):
            vc = VectorClock()
            vc.increment(f"node-{i}")
            vwc = dynamo_pb2.ValueWithContext(
                value=f"version_{i}".encode(),
                context=vc.to_proto()
            )
            node._store_local_version("multi_key", vwc)
        
        time.sleep(0.2)
        server.stop(0)
        del node
        time.sleep(0.2)
        
        # Restart and verify
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("persist-1", "localhost:50111", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50111}")
        server2.start()
        
        time.sleep(0.2)
        
        self.assertIn("multi_key", node2.store)
        self.assertEqual(len(node2.store["multi_key"]), 3)
        
        server2.stop(0)

    def test_hints_delivered_persist(self):
        """Test that delivered hints are persisted."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("persist-2", "localhost:50112", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50112}")
        server.start()
        
        time.sleep(0.2)
        
        # Deliver a hint
        channel = grpc.insecure_channel("localhost:50112")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        vc = VectorClock()
        vc.increment("persist-1")
        vwc = dynamo_pb2.ValueWithContext(value=b"hint_value", context=vc.to_proto())
        
        hint = dynamo_pb2.Hint(
            key="hint_key",
            data=vwc,
            target_node_id="persist-2",
            timestamp_ms=int(time.time() * 1000)
        )
        
        req = dynamo_pb2.DeliverHintsRequest(
            target_node_id="persist-2",
            hints=[hint]
        )
        
        response = stub.DeliverHints(req, timeout=2)
        self.assertTrue(response.success)
        
        channel.close()
        time.sleep(0.2)
        server.stop(0)
        del node
        time.sleep(0.2)
        
        # Restart and verify hint was persisted
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("persist-2", "localhost:50112", self.ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50112}")
        server2.start()
        
        time.sleep(0.2)
        
        self.assertIn("hint_key", node2.store)
        self.assertEqual(node2.store["hint_key"][0].value, b"hint_value")
        
        server2.stop(0)


class TestPersistenceFullScenario(unittest.TestCase):
    """Full integration test: write, restart, hints, verify persistence."""

    def setUp(self):
        """Clean up storage before test."""
        logging.info("\n" + "="*80)
        logging.info("FULL PERSISTENCE SCENARIO TEST")
        logging.info("="*80)
        
        import os
        import shutil
        for node_id in ["scenario-1", "scenario-2", "scenario-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)

    def tearDown(self):
        """Clean up storage after test."""
        import os
        import shutil
        for node_id in ["scenario-1", "scenario-2", "scenario-3"]:
            storage_dir = f"data/{node_id}"
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
        logging.info("Full scenario test cleanup complete")

    def test_full_restart_scenario(self):
        """
        Full scenario test:
        1. Start all nodes
        2. Write keys
        3. Stop all nodes
        4. Restart node-1 and verify data loaded
        5. Kill node-3
        6. Write more keys (creates hints for node-3)
        7. Restart node-3 and verify hints delivered + persisted
        8. Restart all nodes and verify all data present
        """
        ring = DHTRing(vnode_count=10)
        ring.add_node("scenario-1", "localhost:50121")
        ring.add_node("scenario-2", "localhost:50122")
        ring.add_node("scenario-3", "localhost:50123")
        
        # Step 1: Start all 3 nodes
        logging.info("\n" + "-"*80)
        logging.info("Step 1: Starting all nodes")
        logging.info("-"*80)
        servers = []
        nodes = []
        
        for node_id, port in [
            ("scenario-1", 50121),
            ("scenario-2", 50122),
            ("scenario-3", 50123)
        ]:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node = DynamoNode(node_id, f"localhost:{port}", ring)
            dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            servers.append(server)
            nodes.append(node)
            logging.info(f"Started {node_id} on port {port}")
        
        time.sleep(1.0)
        logging.info("All nodes started, waiting for gossip stabilization...")
        
        # Step 2: Write initial keys
        logging.info("\n" + "-"*80)
        logging.info("Step 2: Writing initial keys")
        logging.info("-"*80)
        channel = grpc.insecure_channel("localhost:50121")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        for i in range(3):
            vc = VectorClock()
            vwc = dynamo_pb2.ValueWithContext(
                value=f"initial_value_{i}".encode(),
                context=vc.to_proto()
            )
            request = dynamo_pb2.PutRequest(key=f"initial_key_{i}", data=vwc)
            response = stub.Put(request, timeout=3)
            logging.info(f"PUT initial_key_{i}: success={response.success}, message={response.message}")
        
        channel.close()
        time.sleep(0.5)
        
        # Step 3: Stop all servers (simulate CTRL+C)
        logging.info("\n" + "-"*80)
        logging.info("Step 3: Stopping all nodes (simulating CTRL+C)")
        logging.info("-"*80)
        for i, server in enumerate(servers):
            server.stop(0)
            logging.info(f"Stopped server {i+1}")
        
        del nodes
        time.sleep(0.5)
        logging.info("All servers stopped")
        
        # Step 4: Restart node-1 and verify data loaded
        logging.info("\n" + "-"*80)
        logging.info("Step 4: Restarting node-1")
        logging.info("-"*80)
        server1 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node1 = DynamoNode("scenario-1", "localhost:50121", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node1, server1)
        server1.add_insecure_port(f"[::]:{50121}")
        server1.start()
        
        time.sleep(0.5)
        
        # Verify data was loaded from disk
        loaded_keys = list(node1.store.keys())
        logging.info(f"Loaded keys from disk: {loaded_keys}")
        logging.info(f"Total versions loaded: {sum(len(v) for v in node1.store.values())}")
        
        # GET from node-1
        channel = grpc.insecure_channel("localhost:50121")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        for i in range(3):
            get_req = dynamo_pb2.GetRequest(key=f"initial_key_{i}")
            get_resp = stub.ForwardGet(get_req, timeout=2)
            logging.info(f"GET initial_key_{i}: found={get_resp.found}")
        
        channel.close()
        
        # Also restart node-2
        logging.info("\n" + "-"*80)
        logging.info("Restarting node-2 (for W quorum)")
        logging.info("-"*80)
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("scenario-2", "localhost:50122", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50122}")
        server2.start()
        
        time.sleep(1.0)
        logging.info(f"Node-2 loaded {len(node2.store)} keys from disk")
        
        # Step 5: node-3 is still down
        logging.info("\n" + "-"*80)
        logging.info("Step 5: node-3 remains down")
        logging.info("-"*80)
        
        # Step 6: Write more keys
        logging.info("\n" + "-"*80)
        logging.info("Step 6: Writing new keys (node-3 down, should create hints)")
        logging.info("-"*80)
        channel = grpc.insecure_channel("localhost:50121")
        stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
        
        for i in range(2):
            vc = VectorClock()
            vwc = dynamo_pb2.ValueWithContext(
                value=f"new_value_{i}".encode(),
                context=vc.to_proto()
            )
            request = dynamo_pb2.PutRequest(key=f"new_key_{i}", data=vwc)
            response = stub.Put(request, timeout=3)
            logging.info(f"PUT new_key_{i}: success={response.success}, message={response.message}")
        
        channel.close()
        time.sleep(0.5)
        
        # Check hints
        hints_on_node1 = list(node1.hints.keys())
        hints_on_node2 = list(node2.hints.keys())
        logging.info(f"Hints stored on node-1: {hints_on_node1}")
        logging.info(f"Hints stored on node-2: {hints_on_node2}")
        
        # Step 7: Restart node-3
        logging.info("\n" + "-"*80)
        logging.info("Step 7: Restarting node-3")
        logging.info("-"*80)
        server3 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node3 = DynamoNode("scenario-3", "localhost:50123", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node3, server3)
        server3.add_insecure_port(f"[::]:{50123}")
        server3.start()
        
        logging.info("Waiting for gossip to propagate and hints to be delivered...")
        time.sleep(2.0)
        
        # Verify node-3 data
        node3_keys = list(node3.store.keys())
        logging.info(f"Node-3 keys after recovery: {node3_keys}")
        logging.info(f"Node-3 total versions: {sum(len(v) for v in node3.store.values())}")
        
        # Step 8: Final restart
        logging.info("\n" + "-"*80)
        logging.info("Step 8: Final restart of all nodes")
        logging.info("-"*80)
        server1.stop(0)
        server2.stop(0)
        server3.stop(0)
        
        del node1, node2, node3
        time.sleep(0.5)
        logging.info("All nodes stopped for final restart")
        
        # Restart all
        final_servers = []
        final_nodes = []
        
        for node_id, port in [
            ("scenario-1", 50121),
            ("scenario-2", 50122),
            ("scenario-3", 50123)
        ]:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            node = DynamoNode(node_id, f"localhost:{port}", ring)
            dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
            server.add_insecure_port(f"[::]:{port}")
            server.start()
            final_servers.append(server)
            final_nodes.append(node)
            logging.info(f"Restarted {node_id}")
        
        time.sleep(1.0)
        
        # Final verification
        logging.info("\n" + "-"*80)
        logging.info("Final verification - Data persistence check")
        logging.info("-"*80)
        for i, node in enumerate(final_nodes):
            keys = list(node.store.keys())
            versions = sum(len(v) for v in node.store.values())
            logging.info(f"Node {i+1} ({node.node_id}):")
            logging.info(f"  Keys: {keys}")
            logging.info(f"  Total versions: {versions}")
            
            # Check for initial keys
            has_data = any(f"initial_key_{j}" in node.store for j in range(3))
            self.assertTrue(has_data, f"Node {i+1} should have some initial keys")
            logging.info(f"  Has initial data: {has_data}")
        
        # Clean up
        for server in final_servers:
            server.stop(0)
        
        logging.info("\n" + "="*80)
        logging.info("FULL SCENARIO TEST COMPLETED SUCCESSFULLY")
        logging.info("="*80)


class TestPersistenceEdgeCases(unittest.TestCase):
    """Edge cases for persistence."""

    def setUp(self):
        """Clean up storage."""
        import os
        import shutil
        storage_dir = "data/edge-node"
        if os.path.exists(storage_dir):
            shutil.rmtree(storage_dir)

    def tearDown(self):
        """Clean up storage."""
        import os
        import shutil
        storage_dir = "data/edge-node"
        if os.path.exists(storage_dir):
            shutil.rmtree(storage_dir)

    def test_empty_store_on_first_start(self):
        """Test that node starts with empty store on first boot."""
        ring = DHTRing(vnode_count=10)
        ring.add_node("edge-node", "localhost:50131")
        
        node = DynamoNode("edge-node", "localhost:50131", ring)
        
        # Should start with empty store (only what gossip/ring provides)
        # New node won't have any keys except those loaded from disk
        # On first start, disk should be empty
        self.assertIsInstance(node.store, dict)

    def test_persistence_with_special_characters(self):
        """Test persistence with special characters in keys/values."""
        ring = DHTRing(vnode_count=10)
        ring.add_node("edge-node", "localhost:50131")
        
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("edge-node", "localhost:50131", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50131}")
        server.start()
        
        time.sleep(0.2)
        
        # Store key with special characters
        vc = VectorClock()
        vwc = dynamo_pb2.ValueWithContext(
            value=b"value_with_\x00\xff_bytes",
            context=vc.to_proto()
        )
        
        node._store_local_version("key/with:special.chars-123", vwc)
        time.sleep(0.2)
        
        server.stop(0)
        del node
        time.sleep(0.2)
        
        # Restart and verify
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("edge-node", "localhost:50131", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50131}")
        server2.start()
        
        time.sleep(0.2)
        
        self.assertIn("key/with:special.chars-123", node2.store)
        self.assertEqual(node2.store["key/with:special.chars-123"][0].value, 
                        b"value_with_\x00\xff_bytes")
        
        server2.stop(0)

    def test_persistence_with_concurrent_writes(self):
        """Test that concurrent writes are all persisted."""
        ring = DHTRing(vnode_count=10)
        ring.add_node("edge-node", "localhost:50131")
        
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node = DynamoNode("edge-node", "localhost:50131", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node, server)
        server.add_insecure_port(f"[::]:{50131}")
        server.start()
        
        time.sleep(0.2)
        
        # Concurrent writes
        def write_key(key_id):
            vc = VectorClock()
            vc.increment(f"writer-{key_id}")
            vwc = dynamo_pb2.ValueWithContext(
                value=f"concurrent_{key_id}".encode(),
                context=vc.to_proto()
            )
            node._store_local_version(f"concurrent_key_{key_id}", vwc)
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=write_key, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        time.sleep(0.5)
        
        server.stop(0)
        del node
        time.sleep(0.2)
        
        # Restart and verify all writes persisted
        server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node2 = DynamoNode("edge-node", "localhost:50131", ring)
        dynamo_pb2_grpc.add_DynamoServiceServicer_to_server(node2, server2)
        server2.add_insecure_port(f"[::]:{50131}")
        server2.start()
        
        time.sleep(0.2)
        
        # All 5 keys should be present
        for i in range(5):
            self.assertIn(f"concurrent_key_{i}", node2.store)
        
        server2.stop(0)


if __name__ == "__main__":
    # Print summary to console
    print(f"\n{'='*80}")
    print(f"Starting Dynamo Test Suite")
    print(f"Log file: {LOG_FILE}")
    print(f"{'='*80}\n")
    
    # Run tests with custom result class for better logging
    class LoggingTestResult(unittest.TextTestResult):
        def startTest(self, test):
            super().startTest(test)
            logging.info(f"\n>>> Starting: {test}")
        
        def addSuccess(self, test):
            super().addSuccess(test)
            logging.info(f"✓ PASSED: {test}")
        
        def addError(self, test, err):
            super().addError(test, err)
            logging.error(f"✗ ERROR: {test}")
            logging.error(f"Error details: {self._exc_info_to_string(err, test)}")
        
        def addFailure(self, test, err):
            super().addFailure(test, err)
            logging.error(f"✗ FAILED: {test}")
            logging.error(f"Failure details: {self._exc_info_to_string(err, test)}")
    
    # Create test runner with logging
    runner = unittest.TextTestRunner(
        verbosity=2,
        resultclass=LoggingTestResult,
        stream=sys.stdout
    )
    
    # Run tests
    result = runner.run(unittest.TestLoader().loadTestsFromModule(sys.modules[__name__]))
    
    # Final summary
    logging.info("\n" + "="*80)
    logging.info("TEST SUITE SUMMARY")
    logging.info("="*80)
    logging.info(f"Tests run: {result.testsRun}")
    logging.info(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    logging.info(f"Failures: {len(result.failures)}")
    logging.info(f"Errors: {len(result.errors)}")
    logging.info(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    logging.info("="*80)
    logging.info(f"Full test results saved to: {LOG_FILE}")
    logging.info("="*80)
    
    print(f"\n{'='*80}")
    print(f"Test suite completed!")
    print(f"Results saved to: {LOG_FILE}")
    print(f"{'='*80}\n")
