package transport

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/zde37/torus/backend/pkg"
	"github.com/zde37/torus/backend/pkg/chord"
	pb "github.com/zde37/torus/backend/protobuf/protogen"
)

// Compile-time check to ensure GRPCClient implements chord.RemoteClient
var _ chord.RemoteClient = (*GRPCClient)(nil)

// GRPCClient manages connections to remote Chord nodes.
type GRPCClient struct {
	logger    *pkg.Logger
	authToken string // Authentication token for node-to-node communication

	// Connection pool
	connections map[string]*grpc.ClientConn
	connMu      sync.RWMutex

	// Default timeout for RPC calls
	timeout time.Duration
}

// NewGRPCClient creates a new gRPC client.
func NewGRPCClient(logger *pkg.Logger, authToken string, timeout time.Duration) *GRPCClient {
	return &GRPCClient{
		logger:      logger,
		authToken:   authToken,
		connections: make(map[string]*grpc.ClientConn),
		timeout:     timeout,
	}
}

// withAuthMetadata attaches the auth token to the context metadata if configured.
func (c *GRPCClient) withAuthMetadata(ctx context.Context) context.Context {
	if c.authToken == "" {
		return ctx
	}
	md := metadata.Pairs(AuthTokenHeader, c.authToken)
	return metadata.NewOutgoingContext(ctx, md)
}

// getConnection returns a connection to the given address, creating one if needed.
func (c *GRPCClient) getConnection(address string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, exists := c.connections[address]
	c.connMu.RUnlock()

	if exists && conn.GetState().String() != "SHUTDOWN" {
		return conn, nil
	}

	// Need to create new connection
	c.connMu.Lock()
	defer c.connMu.Unlock()

	// Double-check after acquiring write lock
	conn, exists = c.connections[address]
	if exists && conn.GetState().String() != "SHUTDOWN" {
		return conn, nil
	}

	// Create new connection
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(c.timeout),
	}

	newConn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", address, err)
	}

	c.connections[address] = newConn
	c.logger.Debug("created new gRPC connection", pkg.Fields{
		"address": address,
	})

	return newConn, nil
}

// FindSuccessor calls the FindSuccessor RPC on a remote node.
func (c *GRPCClient) FindSuccessor(address string, id *big.Int) (*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.FindSuccessorRequest{
		Id: id.Bytes(),
	}

	resp, err := client.FindSuccessor(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("FindSuccessor RPC failed: %w", err)
	}

	return protoToNodeAddress(resp.Successor), nil
}

// FindSuccessorWithPath calls the FindSuccessorWithPath RPC on a remote node.
// This is used for recursive path tracking - the remote node returns its routing path.
func (c *GRPCClient) FindSuccessorWithPath(address string, id *big.Int) (*chord.NodeAddress, []*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.FindSuccessorWithPathRequest{
		Id: id.Bytes(),
	}

	resp, err := client.FindSuccessorWithPath(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("FindSuccessorWithPath RPC failed: %w", err)
	}

	// Convert path from protobuf
	path := make([]*chord.NodeAddress, len(resp.Path))
	for i, node := range resp.Path {
		path[i] = protoToNodeAddress(node)
	}

	return protoToNodeAddress(resp.Successor), path, nil
}

// GetPredecessor calls the GetPredecessor RPC on a remote node.
func (c *GRPCClient) GetPredecessor(address string) (*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.GetPredecessorRequest{}

	resp, err := client.GetPredecessor(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetPredecessor RPC failed: %w", err)
	}

	return protoToNodeAddress(resp.Predecessor), nil
}

// Notify calls the Notify RPC on a remote node.
func (c *GRPCClient) Notify(address string, node *chord.NodeAddress) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.NotifyRequest{
		Node: nodeAddressToProto(node),
	}

	_, err = client.Notify(ctx, req)
	if err != nil {
		return fmt.Errorf("Notify RPC failed: %w", err)
	}

	return nil
}

// GetSuccessorList calls the GetSuccessorList RPC on a remote node.
func (c *GRPCClient) GetSuccessorList(address string) ([]*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.GetSuccessorListRequest{}

	resp, err := client.GetSuccessorList(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetSuccessorList RPC failed: %w", err)
	}

	// Convert protobuf nodes to NodeAddress
	successors := make([]*chord.NodeAddress, len(resp.Successors))
	for i, pbNode := range resp.Successors {
		successors[i] = protoToNodeAddress(pbNode)
	}

	return successors, nil
}

// Ping calls the Ping RPC on a remote node.
func (c *GRPCClient) Ping(address string, message string) (string, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return "", err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.PingRequest{
		Message: message,
	}

	resp, err := client.Ping(ctx, req)
	if err != nil {
		return "", fmt.Errorf("Ping RPC failed: %w", err)
	}

	return resp.Message, nil
}

// GetNodeInfo calls the GetNodeInfo RPC on a remote node.
func (c *GRPCClient) GetNodeInfo(address string) (*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.GetNodeInfoRequest{}

	resp, err := client.GetNodeInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetNodeInfo RPC failed: %w", err)
	}

	return protoToNodeAddress(resp.Node), nil
}

// ClosestPrecedingFinger calls the ClosestPrecedingFinger RPC on a remote node.
func (c *GRPCClient) ClosestPrecedingFinger(address string, id *big.Int) (*chord.NodeAddress, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	req := &pb.ClosestPrecedingFingerRequest{
		Id: id.Bytes(),
	}

	resp, err := client.ClosestPrecedingFinger(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ClosestPrecedingFinger RPC failed: %w", err)
	}

	return protoToNodeAddress(resp.Node), nil
}

// Get calls the Get RPC on a remote node.
func (c *GRPCClient) Get(ctx context.Context, address string, key string) ([]byte, bool, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, false, err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.GetRequest{Key: key}

	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("Get RPC failed: %w", err)
	}

	return resp.Value, resp.Found, nil
}

// Set calls the Set RPC on a remote node.
func (c *GRPCClient) Set(ctx context.Context, address string, key string, value []byte) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	// Note: TTL support is available in the proto definition (ttl_seconds field)
	// but not exposed in the current interface to keep it simple.
	// Add ttl_seconds to the request if TTL support is needed.
	req := &pb.SetRequest{
		Key:   key,
		Value: value,
	}

	resp, err := client.Set(ctx, req)
	if err != nil {
		return fmt.Errorf("Set RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote node failed to set key")
	}

	return nil
}

// Delete calls the Delete RPC on a remote node.
func (c *GRPCClient) Delete(ctx context.Context, address string, key string) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.DeleteRequest{Key: key}

	resp, err := client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("Delete RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote node failed to delete key")
	}

	return nil
}

// TransferKeys calls the TransferKeys RPC on a remote node.
func (c *GRPCClient) TransferKeys(ctx context.Context, address string, startID, endID *big.Int) (map[string][]byte, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.TransferKeysRequest{
		StartId: startID.Bytes(),
		EndId:   endID.Bytes(),
	}

	resp, err := client.TransferKeys(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("TransferKeys RPC failed: %w", err)
	}

	// Convert protobuf response to map
	keys := make(map[string][]byte)
	for _, kv := range resp.Keys {
		keys[kv.Key] = kv.Value
	}

	c.logger.Debug("received keys from remote node", pkg.Fields{
		"key_count": len(keys),
		"address":   address,
	})

	return keys, nil
}

// DeleteTransferredKeys calls the DeleteTransferredKeys RPC on a remote node.
func (c *GRPCClient) DeleteTransferredKeys(ctx context.Context, address string, startID, endID *big.Int) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.DeleteTransferredKeysRequest{
		StartId: startID.Bytes(),
		EndId:   endID.Bytes(),
	}

	resp, err := client.DeleteTransferredKeys(ctx, req)
	if err != nil {
		return fmt.Errorf("DeleteTransferredKeys RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote node failed to delete transferred keys")
	}

	c.logger.Debug("deleted transferred keys on remote node", pkg.Fields{
		"key_count": int(resp.Count),
		"address":   address,
	})

	return nil
}

// SetReplica calls the SetReplica RPC on a remote node.
func (c *GRPCClient) SetReplica(ctx context.Context, address string, key string, value []byte, ttl time.Duration) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.SetReplicaRequest{
		Key:        key,
		Value:      value,
		TtlSeconds: int64(ttl.Seconds()),
	}

	resp, err := client.SetReplica(ctx, req)
	if err != nil {
		return fmt.Errorf("SetReplica RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote node failed to set replica")
	}

	return nil
}

// GetReplica calls the GetReplica RPC on a remote node.
func (c *GRPCClient) GetReplica(ctx context.Context, address string, key string) ([]byte, bool, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, false, err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.GetReplicaRequest{Key: key}

	resp, err := client.GetReplica(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("GetReplica RPC failed: %w", err)
	}

	return resp.Value, resp.Found, nil
}

// DeleteReplica calls the DeleteReplica RPC on a remote node.
func (c *GRPCClient) DeleteReplica(ctx context.Context, address string, key string) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.DeleteReplicaRequest{Key: key}

	resp, err := client.DeleteReplica(ctx, req)
	if err != nil {
		return fmt.Errorf("DeleteReplica RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote node failed to delete replica")
	}

	return nil
}

// Close closes all connections.
func (c *GRPCClient) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	c.logger.Info("closing all gRPC connections", pkg.Fields{
		"connections": len(c.connections),
	})

	for address, conn := range c.connections {
		if err := conn.Close(); err != nil {
			c.logger.Error("failed to close connection", pkg.Fields{
				"error":   err.Error(),
				"address": address,
			})
		}
	}

	c.connections = make(map[string]*grpc.ClientConn)
	return nil
}

// BulkStore stores multiple key-value pairs on a remote node.
func (c *GRPCClient) BulkStore(ctx context.Context, address string, items map[string][]byte) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.BulkStoreRequest{
		Items: items,
	}

	resp, err := client.BulkStore(ctx, req)
	if err != nil {
		return fmt.Errorf("BulkStore RPC failed: %w", err)
	}

	if !resp.Success {
		if resp.Error != "" {
			return fmt.Errorf("bulk store failed: %s", resp.Error)
		}
		return fmt.Errorf("bulk store failed on remote node")
	}

	c.logger.Debug("bulk stored items on remote node", pkg.Fields{
		"count":   resp.Count,
		"address": address,
	})

	return nil
}

// StoreReplica stores a replica of a key-value pair on a remote node.
func (c *GRPCClient) StoreReplica(ctx context.Context, address string, key string, value []byte) error {
	// Use SetReplica with no TTL for replica storage
	return c.SetReplica(ctx, address, key, value, 0)
}

// NotifyPredecessorLeaving notifies a predecessor that we're leaving.
func (c *GRPCClient) NotifyPredecessorLeaving(ctx context.Context, address string, newSuccessor *chord.NodeAddress) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.NotifyPredecessorLeavingRequest{
		NewSuccessor: &pb.Node{
			Id:       newSuccessor.ID.Bytes(),
			Host:     newSuccessor.Host,
			Port:     int32(newSuccessor.Port),
			HttpPort: int32(newSuccessor.HTTPPort),
		},
	}

	resp, err := client.NotifyPredecessorLeaving(ctx, req)
	if err != nil {
		return fmt.Errorf("NotifyPredecessorLeaving RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("predecessor failed to process leaving notification")
	}

	c.logger.Debug("notified predecessor about leaving", pkg.Fields{
		"address":       address,
		"new_successor": newSuccessor.Address(),
	})

	return nil
}

// NotifySuccessorLeaving notifies a successor that we're leaving.
func (c *GRPCClient) NotifySuccessorLeaving(ctx context.Context, address string, newPredecessor *chord.NodeAddress) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.NotifySuccessorLeavingRequest{
		NewPredecessor: &pb.Node{
			Id:       newPredecessor.ID.Bytes(),
			Host:     newPredecessor.Host,
			Port:     int32(newPredecessor.Port),
			HttpPort: int32(newPredecessor.HTTPPort),
		},
	}

	resp, err := client.NotifySuccessorLeaving(ctx, req)
	if err != nil {
		return fmt.Errorf("NotifySuccessorLeaving RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("successor failed to process leaving notification")
	}

	c.logger.Debug("notified successor about leaving", pkg.Fields{
		"address":         address,
		"new_predecessor": newPredecessor.Address(),
	})

	return nil
}

// NotifyNodeLeaving notifies a node in the successor list about our departure.
func (c *GRPCClient) NotifyNodeLeaving(ctx context.Context, address string, leavingNode *chord.NodeAddress) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

	// Add auth metadata
	ctx = c.withAuthMetadata(ctx)

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	req := &pb.NotifyNodeLeavingRequest{
		LeavingNode: &pb.Node{
			Id:       leavingNode.ID.Bytes(),
			Host:     leavingNode.Host,
			Port:     int32(leavingNode.Port),
			HttpPort: int32(leavingNode.HTTPPort),
		},
	}

	resp, err := client.NotifyNodeLeaving(ctx, req)
	if err != nil {
		return fmt.Errorf("NotifyNodeLeaving RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("node failed to process leaving notification")
	}

	c.logger.Debug("notified node about departure", pkg.Fields{
		"address":      address,
		"leaving_node": leavingNode.Address(),
	})

	return nil
}
