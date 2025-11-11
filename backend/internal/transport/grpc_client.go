package transport

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zde37/torus/internal/chord"
	"github.com/zde37/torus/pkg"
	pb "github.com/zde37/torus/protobuf/protogen"
)

// Compile-time check to ensure GRPCClient implements chord.RemoteClient
var _ chord.RemoteClient = (*GRPCClient)(nil)

// GRPCClient manages connections to remote Chord nodes.
type GRPCClient struct {
	logger *pkg.Logger

	// Connection pool
	connections map[string]*grpc.ClientConn
	connMu      sync.RWMutex

	// Default timeout for RPC calls
	timeout time.Duration
}

// NewGRPCClient creates a new gRPC client.
func NewGRPCClient(logger *pkg.Logger, timeout time.Duration) *GRPCClient {
	if logger == nil {
		logger, _ = pkg.New(pkg.DefaultConfig())
	}

	return &GRPCClient{
		logger:      logger.WithFields(pkg.Fields{"component": "grpc_client"}),
		connections: make(map[string]*grpc.ClientConn),
		timeout:     timeout,
	}
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
	c.logger.Debug().Str("address", address).Msg("Created new gRPC connection")

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

	// Use the provided context with a timeout fallback
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

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

	c.logger.Debug().
		Int("key_count", len(keys)).
		Str("address", address).
		Msg("Received keys from remote node")

	return keys, nil
}

// DeleteTransferredKeys calls the DeleteTransferredKeys RPC on a remote node.
func (c *GRPCClient) DeleteTransferredKeys(ctx context.Context, address string, startID, endID *big.Int) error {
	conn, err := c.getConnection(address)
	if err != nil {
		return err
	}

	client := pb.NewChordServiceClient(conn)

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

	c.logger.Debug().
		Int("key_count", int(resp.Count)).
		Str("address", address).
		Msg("Deleted transferred keys on remote node")

	return nil
}

// Close closes all connections.
func (c *GRPCClient) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	c.logger.Info().
		Int("connections", len(c.connections)).
		Msg("Closing all gRPC connections")

	for address, conn := range c.connections {
		if err := conn.Close(); err != nil {
			c.logger.Error().
				Err(err).
				Str("address", address).
				Msg("Failed to close connection")
		}
	}

	c.connections = make(map[string]*grpc.ClientConn)
	return nil
}
