# Dynamo-Style Distributed Key-Value Store

A fault-tolerant, eventually consistent distributed key-value store inspired by Amazon's Dynamo paper. Implements consistent hashing, vector clocks, hinted handoff, read repair, gossip-based failure detection, and persistent storage.

---

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Setup](#setup)
- [Running the System](#running-the-system)
- [Testing Features](#testing-features)
- [Client Usage](#client-usage)
- [Project Structure](#project-structure)
- [Configuration](#configuration)

---

## ✨ Features

### Core Features
1. **Consistent Hashing with Virtual Nodes**
   - Distributes keys evenly across nodes
   - 10 virtual nodes per physical node
   - Automatic rebalancing on node join/leave

2. **Vector Clocks for Conflict Detection**
   - Tracks causality between versions
   - Detects concurrent writes (siblings)
   - Enables proper version reconciliation

3. **Quorum-Based Replication**
   - N=3 (replication factor)
   - W=2 (write quorum)
   - R=2 (read quorum)
   - Tunable consistency vs availability

4. **Hinted Handoff**
   - Stores hints when replica is unavailable
   - Automatically delivers hints when node recovers
   - Ensures eventual consistency

5. **Read Repair**
   - Detects stale replicas during reads
   - Asynchronously pushes latest version
   - Maintains consistency over time

6. **Gossip-Based Failure Detection**
   - Periodic membership exchange (1s interval)
   - Timeout-based failure detection (5s)
   - Automatic removal of long-dead nodes (2min)

7. **Persistent Storage**
   - File-based persistence (data/<node_id>/)
   - Hash-based key storage with metadata
   - Survives restarts and crashes

8. **Dynamic Membership**
   - Nodes can join/leave at runtime
   - Gossip propagates membership changes
   - Ring automatically updates

---

## 🏗️ Architecture

![Architecture Diagram](docs/architecture.png)

- **Client**: Interacts with the key-value store using the provided API.
- **Coordinator**: Manages client requests, coordinates between replicas, and ensures consistency.
- **Replica**: Stores the actual key-value data. Multiple replicas per key for redundancy.
- **Gossip**: Protocol used by nodes to discover each other and share state information.

---

## 🚀 Setup

1. **Prerequisites**
   - Go 1.16 or later
   - Git

2. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/dynamo-style-kvstore.git
   cd dynamo-style-kvstore
   ```

3. **Build the Project**
   ```bash
   go build -o kvstore ./cmd/kvstore
   ```

4. **Configuration**
   - Copy `config/example.yaml` to `config/config.yaml`
   - Edit `config/config.yaml` to set your desired configuration

---

## 🏃 Running the System

1. **Start a Single Node**
   ```bash
   ./kvstore -config config/config.yaml
   ```

2. **Join an Existing Cluster**
   - Start the node with the `-join` flag:
   ```bash
   ./kvstore -config config/config.yaml -join <existing-node-ip>
   ```

3. **Check Logs**
   - Logs are stored in the `logs/` directory by default.
   - Monitor logs for debugging and information.

---

## 🔍 Testing Features

1. **Run Unit Tests**
   ```bash
   go test ./... -v
   ```

2. **Run Integration Tests**
   ```bash
   go test -tags=integration ./... -v
   ```

3. **Benchmarking**
   - Use the built-in benchmarking tools in Go.
   - Example: `go test -bench=.`

---

## 💻 Client Usage

### HTTP API
- Base URL: `http://<node_ip>:<node_port>/v1/`

1. **Put a Key-Value Pair**
   ```bash
   curl -X POST "<node_ip>:<node_port>/v1/kv/put" -H "Content-Type: application/json" -d '{"key": "foo", "value": "bar"}'
   ```

2. **Get a Value by Key**
   ```bash
   curl -X GET "<node_ip>:<node_port>/v1/kv/get/foo"
   ```

3. **Delete a Key-Value Pair**
   ```bash
   curl -X DELETE "<node_ip>:<node_port>/v1/kv/delete/foo"
   ```

### gRPC API
- Import the protobuf definition in `proto/kvstore.proto`.
- Use the generated client code to interact with the kvstore.

---

## 📂 Project Structure

```
dynamo-style-kvstore/
├── cmd/                  # Command-line applications
│   └── kvstore/          # Key-Value store application
├── config/               # Configuration files
│   └── example.yaml      # Example configuration
├── docs/                 # Documentation files
│   └── architecture.png  # Architecture diagram
├── internal/             # Internal packages
│   ├── cluster/          # Cluster management
│   ├── config/           # Configuration handling
│   ├── storage/          # Storage engine
│   └── transport/        # Networking and transport layer
├── proto/                # Protocol buffers
│   └── kvstore.proto     # gRPC service definition
├── scripts/              # Helper scripts
└── tests/                # Test files
    ├── integration/      # Integration tests
    └── unit/             # Unit tests
```

---

## ⚙️ Configuration

- Configuration is done via YAML files.
- Main configuration file: `config/config.yaml`
- Example configuration: `config/example.yaml`

### Sample Configuration
```yaml
server:
  port: 8080
  data_dir: "data/"
  log_level: "info"

cluster:
  replication_factor: 3
  num_virtual_nodes: 10

gossip:
  interval: 1s
  timeout: 5s

storage:
  engine: "file"
  path: "data/{node_id}/"
```

### Configuration Options
- `server.port`: Port for the HTTP API
- `server.data_dir`: Directory for persistent data
- `server.log_level`: Log level (`debug`, `info`, `warn`, `error`)
- `cluster.replication_factor`: Number of replicas per key
- `cluster.num_virtual_nodes`: Number of virtual nodes per physical node
- `gossip.interval`: Interval for gossip protocol
- `gossip.timeout`: Timeout for detecting failed nodes
- `storage.engine`: Storage engine to use (`file` or `memory`)
- `storage.path`: Path for storage (with `{node_id}` placeholder)

