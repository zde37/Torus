# Testing Guide: Torus Chord DHT

This guide will walk you through testing the implemented Chord DHT functionality.

---

## Prerequisites

```bash
cd /Users/zde37/go_projects/ubuntu/chord-p2p/torus/backend
```

Make sure the binary is built:
```bash
make proto  # If not already done
go build -o bin/torus ./cmd/torus
```

---

## Test 1: Single Node Ring

### Start Node 1 (Bootstrap)
```bash
./bin/torus -port 8440 -http-port 8080 -log-level debug
```

**Expected output:**
- "Creating new Chord ring"
- "Chord ring created successfully"
- "Torus node is ready"

**What to verify:**
- Node creates a ring where it's its own successor
- Background tasks start (stabilization, fix fingers)
- No errors in logs

---

## Test 2: Two-Node Ring

### Terminal 1: Start Bootstrap Node
```bash
./bin/torus -port 8440 -http-port 8080
```

### Terminal 2: Join Second Node
```bash
./bin/torus -port 8441 -http-port 8081 -bootstrap "127.0.0.1:8440"
```

**Expected output (Terminal 2):**
- "Joining existing Chord ring"
- "Fetching bootstrap node information"
- "Retrieved bootstrap node information"
- "Asking bootstrap node for successor"
- "Found successor"
- "Joined Chord ring successfully"

**What to verify:**
- Node 2 successfully contacts Node 1
- Node 2 finds its position in the ring
- Both nodes show stabilization messages in debug mode

**To stop:** Press `Ctrl+C` in each terminal

---

## Test 3: Three-Node Ring

### Terminal 1: Bootstrap
```bash
./bin/torus -port 8440 -http-port 8080 -log-level info
```

### Terminal 2: Node 2
```bash
./bin/torus -port 8441 -http-port 8081 -bootstrap "127.0.0.1:8440" -log-level info
```

### Terminal 3: Node 3
```bash
./bin/torus -port 8442 -http-port 8082 -bootstrap "127.0.0.1:8440" -log-level info
```

**What to verify:**
- All 3 nodes join successfully
- Check logs for "Stabilize completed" messages
- No RPC errors

---

## Test 4: DHT Operations (Using grpcurl)

Since we don't have an HTTP API yet, we'll test using `grpcurl` directly with the gRPC server.

### Install grpcurl (if not installed)
```bash
# macOS
brew install grpcurl

# Or download from: https://github.com/fullstorydev/grpcurl/releases
```

### Setup Test Environment

**Terminal 1: Start Node 1**
```bash
./bin/torus -port 8440 -http-port 8080 -log-level debug
```

**Terminal 2: Start Node 2**
```bash
./bin/torus -port 8441 -http-port 8081 -bootstrap "127.0.0.1:8440" -log-level debug
```

Wait ~2 seconds for nodes to stabilize.

---

### Test 4.1: Check Available RPCs

**Terminal 3: List available services**
```bash
grpcurl -plaintext localhost:8440 list
```

**Expected output:**
```
protogen.ChordService
grpc.reflection.v1alpha.ServerReflection
```

**List ChordService methods:**
```bash
grpcurl -plaintext localhost:8440 list protogen.ChordService
```

**Expected output:**
```
protogen.ChordService.ClosestPrecedingFinger
protogen.ChordService.Delete
protogen.ChordService.FindSuccessor
protogen.ChordService.Get
protogen.ChordService.GetNodeInfo
protogen.ChordService.GetPredecessor
protogen.ChordService.GetSuccessorList
protogen.ChordService.Notify
protogen.ChordService.Ping
protogen.ChordService.Set
protogen.ChordService.TransferKeys
```

---

### Test 4.2: Ping Test

**Ping Node 1:**
```bash
grpcurl -emit-defaults -plaintext -d '{"message": "hello"}' localhost:8440 protogen.ChordService.Ping
```

**Expected output:**
```json
{
  "message": "pong",
  "timestamp": "1699638000"
}
```

**Ping Node 2:**
```bash
grpcurl -emit-defaults -plaintext -d '{"message": "hello"}' localhost:8441 protogen.ChordService.Ping
```

---

### Test 4.3: Get Node Information

**Get Node 1 info:**
```bash
grpcurl -emit-defaults -plaintext -d '{}' localhost:8440 protogen.ChordService.GetNodeInfo
```

**Expected output:**
```json
{
  "node": {
    "id": "<hex_bytes>",
    "host": "127.0.0.1",
    "port": 8440
  }
}
```

**Get Node 2 info:**
```bash
grpcurl -emit-defaults -plaintext -d '{}' localhost:8441 protogen.ChordService.GetNodeInfo
```

---

### Test 4.4: Set Key-Value (Store Data)

**Store on Node 1:**
```bash
grpcurl -emit-defaults -plaintext -d '{
  "key": "user:1",
  "value": "'"$(echo -n 'John Doe' | base64)"'"
}' localhost:8440 protogen.ChordService.Set
```

**Expected output:**
```json
{
  "success": true
}
```

**Check logs:** You should see either:
- "Stored key locally" (if Node 1 is responsible)
- "Forwarding Set request to responsible node" (if Node 2 is responsible)

---

### Test 4.5: Get Key-Value (Retrieve Data)

**Retrieve from Node 1 (where we stored it):**
```bash
grpcurl -plaintext -d '{"key": "user:1"}' localhost:8440 protogen.ChordService.Get
```

**Expected output:**
```json
{
  "value": "<base64_encoded_value>",
  "found": true
}
```

**Retrieve from Node 2 (test routing):**
```bash
grpcurl -plaintext -d '{"key": "user:1"}' localhost:8441 protogen.ChordService.Get
```

**Expected behavior:**
- If Node 2 is responsible: Returns value from local storage
- If Node 1 is responsible: Node 2 forwards request to Node 1, returns value

**Check logs:** Look for "Forwarding Get request to responsible node"

---

### Test 4.6: Delete Key

**Delete from Node 1:**
```bash
grpcurl -emit-defaults -plaintext -d '{"key": "user:1"}' localhost:8440 protogen.ChordService.Delete
```

**Expected output:**
```json
{
  "success": true
}
```

**Verify deletion - try to Get:**
```bash
grpcurl -emit-defaults -plaintext -d '{"key": "user:1"}' localhost:8440 protogen.ChordService.Get
```

**Expected output:**
```json
{
  "value": null,
  "found": false
}
```

---

### Test 4.7: Test Routing Across Nodes

**Store multiple keys to see routing in action:**

```bash
# Store key1
grpcurl -emit-defaults -plaintext -d '{
  "key": "test:key1",
  "value": "'"$(echo -n 'value1' | base64)"'"
}' localhost:8440 protogen.ChordService.Set

# Store key2
grpcurl -emit-defaults -plaintext -d '{
  "key": "test:key2",
  "value": "'"$(echo -n 'value2' | base64)"'"
}' localhost:8440 protogen.ChordService.Set

# Store key3
grpcurl -emit-defaults -plaintext -d '{
  "key": "test:key3",
  "value": "'"$(echo -n 'value3' | base64)"'"
}' localhost:8440 protogen.ChordService.Set
```

**Then retrieve from Node 2:**
```bash
grpcurl -emit-defaults -plaintext -d '{"key": "test:key1"}' localhost:8441 protogen.ChordService.Get
grpcurl -emit-defaults -plaintext -d '{"key": "test:key2"}' localhost:8441 protogen.ChordService.Get
grpcurl -emit-defaults -plaintext -d '{"key": "test:key3"}' localhost:8441 protogen.ChordService.Get
```

**Watch the debug logs** to see:
- Which node is responsible for each key (based on hash)
- Routing/forwarding messages between nodes

---

## Test 5: FindSuccessor (Core Chord Operation)

**Find successor for a specific ID:**

First, get Node 1's ID to use as reference:
```bash
grpcurl -emit-defaults -plaintext -d '{}' localhost:8440 protogen.ChordService.GetNodeInfo
```

Then find successor:
```bash
# Example with a test ID (you can use any hex value)
grpcurl -emit-defaults -plaintext -d '{
  "id": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo="
}' localhost:8440 protogen.ChordService.FindSuccessor
```

**Expected output:**
```json
{
  "successor": {
    "id": "<hex_bytes>",
    "host": "127.0.0.1",
    "port": 8440 or 8441
  }
}
```

---

## Test 5.5: Data Migration (TransferKeys)

This tests the automatic data migration that happens when a new node joins the ring.

### Setup: Two Nodes with Data

**Terminal 1: Start Node 1**
```bash
./bin/torus -port 8440 -log-level debug
```

**Terminal 2: Store some data**
```bash
# Store multiple keys on Node 1
grpcurl -emit-defaults -plaintext -d '{
  "key": "user:alice",
  "value": "'"$(echo -n 'Alice Data' | base64)"'"
}' localhost:8440 protogen.ChordService.Set

grpcurl -emit-defaults -plaintext -d '{
  "key": "user:bob",
  "value": "'"$(echo -n 'Bob Data' | base64)"'"
}' localhost:8440 protogen.ChordService.Set

grpcurl -emit-defaults -plaintext -d '{
  "key": "user:charlie",
  "value": "'"$(echo -n 'Charlie Data' | base64)"'"
}' localhost:8440 protogen.ChordService.Set
```

**Expected:** All keys stored successfully on Node 1 (it's the only node)

### Test Migration: Join Node 2

**Terminal 3: Start Node 2 with debug logging**
```bash
./bin/torus -port 8441 -log-level debug -bootstrap "127.0.0.1:8440"
```

**Watch Terminal 3 logs for:**
```
"Requesting key transfer from successor"
"Received keys from successor"
"Stored transferred keys locally"
"Requesting successor to delete transferred keys"
"Key migration completed successfully"
```

**Watch Terminal 1 logs for:**
```
"TransferKeys called"
"Transferred keys"
"DeleteTransferredKeys called"
"Deleted transferred keys"
```

### Verify Keys Were Migrated

**Check which node is responsible for each key:**

```bash
# Try retrieving from Node 2 (some keys may have migrated to it)
grpcurl -emit-defaults -plaintext -d '{"key": "user:alice"}' localhost:8441 protogen.ChordService.Get
grpcurl -emit-defaults -plaintext -d '{"key": "user:bob"}' localhost:8441 protogen.ChordService.Get
grpcurl -emit-defaults -plaintext -d '{"key": "user:charlie"}' localhost:8441 protogen.ChordService.Get
```

**Expected behavior:**
- Keys that hash to Node 2's responsibility range: Retrieved from local storage
- Keys that hash to Node 1's responsibility range: Forwarded to Node 1

**Check logs:** You should see either:
- "Stored key locally" (key is on this node)
- "Forwarding Get request to responsible node" (key is on another node)

### Test TransferKeys RPC Directly

You can also test the TransferKeys RPC directly (though it's mainly used internally):

```bash
# Example: Request keys in a specific range
# Note: You need actual ID bytes in base64
grpcurl -emit-defaults -plaintext -d '{
  "start_id": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=",
  "end_id": "////////////////////////////////////=="
}' localhost:8440 protogen.ChordService.TransferKeys
```

**Expected output:**
```json
{
  "keys": [
    {
      "key": "hexhash1",
      "value": "base64value1",
      "ttlSeconds": "0"
    }
  ],
  "count": 1
}
```

### Test DeleteTransferredKeys RPC

Test the cleanup RPC (normally called automatically after successful transfer):

```bash
grpcurl -emit-defaults -plaintext -d '{
  "start_id": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=",
  "end_id": "AQMFCA0VIjU="
}' localhost:8440 protogen.ChordService.DeleteTransferredKeys
```

**Expected output:**
```json
{
  "success": true,
  "count": 0
}
```

**Note:** Count will be 0 if no keys exist in that range or were already deleted.

### What to Verify:

1. ✅ **No data loss:** All keys stored before Node 2 joined are still accessible
2. ✅ **Correct distribution:** Keys are on the responsible node (based on hash)
3. ✅ **No duplicates:** Keys that migrated to Node 2 were deleted from Node 1
4. ✅ **Automatic cleanup:** DeleteTransferredKeys was called automatically
5. ✅ **Routing works:** Can retrieve keys from any node via forwarding

---

## Test 6: Stabilization

**Watch nodes stabilize over time:**

Start with debug logging:
```bash
# Terminal 1
./bin/torus -port 8440 -log-level debug

# Terminal 2
./bin/torus -port 8441 -bootstrap "127.0.0.1:8440" -log-level debug
```

**Watch for these log messages:**
- "Stabilize completed"
- "Asking successor for its predecessor"
- "Notify successor"
- "Fix fingers completed"

These should appear every 1-3 seconds (based on intervals in config).

---

## Test 7: Node Failure Scenario

### Setup
**Terminal 1: Node 1**
```bash
./bin/torus -port 8440 -log-level debug
```

**Terminal 2: Node 2**
```bash
./bin/torus -port 8441 -bootstrap "127.0.0.1:8440" -log-level debug
```

### Store data
```bash
grpcurl -emit-defaults -plaintext -d '{
  "key": "persistent:data",
  "value": "'"$(echo -n 'important' | base64)"'"
}' localhost:8440 protogen.ChordService.Set
```

### Kill Node 1
Press `Ctrl+C` in Terminal 1

### Try to access from Node 2
```bash
grpcurl -emit-defaults -plaintext -d '{"key": "persistent:data"}' localhost:8441 protogen.ChordService.Get
```

**Expected behavior:**
- If Node 1 stored the data: Request will fail (Node 1 is down)
- If Node 2 stored the data: Request succeeds (data is on Node 2)

**Note:** This demonstrates why we need replication - single point of failure without it!

---

## Expected Observations

### ✅ What Should Work:
1. ✅ Nodes can create and join rings
2. ✅ DHT Set/Get/Delete work on any node
3. ✅ Requests automatically route to responsible nodes
4. ✅ Stabilization runs in background
5. ✅ FindSuccessor correctly locates nodes
6. ✅ **Data migration** - Keys automatically transfer when nodes join
7. ✅ **Cleanup after migration** - Transferred keys are deleted from source node

### ⚠️ Current Limitations (To be fixed):
1. **No replication** - If a node fails, its data is lost (successor list exists but not used for replication)
2. **No HTTP API** - Must use grpcurl (API Gateway coming in Step 7)
3. **Basic stabilization** - Successor list not fully utilized for fault tolerance

---

## Cleanup

To stop all nodes:
```bash
# Press Ctrl+C in each terminal
# Or use:
pkill -f "torus -port"
```

---

## Troubleshooting

**Problem: "Failed to dial: connection refused"**
- Make sure the bootstrap node is running
- Check the port number is correct

**Problem: "remote client not set"**
- This shouldn't happen in the binary, but check logs

**Problem: grpcurl not working**
- Install: `brew install grpcurl`
- Make sure using `-plaintext` flag (no TLS)

**Problem: Can't decode base64 values**
```bash
echo "<base64_value>" | base64 -d
```

---

## What to Test

### Priority 1 (Core functionality):
- ✅ Single node ring creation
- ✅ Two nodes joining
- ✅ DHT Set/Get/Delete
- ✅ Routing between nodes

### Priority 2 (Verify correctness):
- ✅ Three+ nodes
- ✅ FindSuccessor with various IDs
- ✅ Stabilization logs

### Priority 3 (Edge cases):
- ✅ Non-existent key retrieval
- ✅ Deleting non-existent key
- ⚠️ Node failure (shows limitation)

---

## Quick Test Script

Here's a quick script to run basic tests:

```bash
#!/bin/bash

echo "Starting Node 1..."
./bin/torus -port 8440 -log-level info > /tmp/node1.log 2>&1 &
NODE1_PID=$!

sleep 2

echo "Starting Node 2..."
./bin/torus -port 8441 -bootstrap "127.0.0.1:8440" -log-level info > /tmp/node2.log 2>&1 &
NODE2_PID=$!

sleep 2

echo "Testing Ping..."
grpcurl -emit-defaults -plaintext -d '{"message": "hello"}' localhost:8440 protogen.ChordService.Ping

echo "Storing key..."
grpcurl -emit-defaults -plaintext -d '{"key": "test", "value": "dGVzdA=="}' localhost:8440 protogen.ChordService.Set

echo "Retrieving key from Node 1..."
grpcurl -emit-defaults -plaintext -d '{"key": "test"}' localhost:8440 protogen.ChordService.Get

echo "Retrieving key from Node 2 (test routing)..."
grpcurl -emit-defaults -plaintext -d '{"key": "test"}' localhost:8441 protogen.ChordService.Get

echo "Cleaning up..."
kill $NODE1_PID $NODE2_PID

echo "Check logs: /tmp/node1.log and /tmp/node2.log"
```

---

## Next Steps

Once you've tested everything and are satisfied, we'll proceed with:
1. ✅ ~~**TransferKeys** implementation (data migration)~~ - COMPLETED!
2. **Integration tests** (automated multi-node testing)
3. **API Gateway** (Step 7 - HTTP/WebSocket for frontend)

**Happy Testing! 🚀**
