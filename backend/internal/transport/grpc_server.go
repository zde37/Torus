package transport

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/zde37/torus/internal/chord"
	"github.com/zde37/torus/pkg"
	pb "github.com/zde37/torus/protobuf/protogen"
	"google.golang.org/grpc/reflection"
)

// GRPCServer wraps a ChordNode and implements the gRPC ChordService.
type GRPCServer struct {
	pb.UnimplementedChordServiceServer

	node      *chord.ChordNode
	server    *grpc.Server
	logger    *pkg.Logger
	authToken string // Authentication token for node-to-node communication

	// Server address
	address  string
	listener net.Listener
}

// NewGRPCServer creates a new gRPC server for the given ChordNode.
func NewGRPCServer(node *chord.ChordNode, address string, authToken string, logger *pkg.Logger) (*GRPCServer, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	s := &GRPCServer{
		node:      node,
		address:   address,
		authToken: authToken,
		logger:    logger.WithFields(pkg.Fields{"component": "grpc_server"}),
	}

	return s, nil
}

// Start starts the gRPC server.
func (s *GRPCServer) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4MB
		grpc.MaxSendMsgSize(4 * 1024 * 1024), // 4MB
		grpc.UnaryInterceptor(AuthInterceptor(s.authToken)), // Add auth interceptor
	}

	s.server = grpc.NewServer(opts...)
	pb.RegisterChordServiceServer(s.server, s)
	reflection.Register(s.server) // self-documentation for the server

	s.logger.Info().
		Str("address", s.address).
		Msg("Starting gRPC server")

	// Start serving in a goroutine
	go func() {
		if err := s.server.Serve(listener); err != nil {
			s.logger.Error().Err(err).Msg("gRPC server error")
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *GRPCServer) Stop() error {
	s.logger.Info().Msg("Stopping gRPC server")

	if s.server != nil {
		s.server.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	return nil
}

// FindSuccessor implements the FindSuccessor RPC.
func (s *GRPCServer) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	s.logger.Debug().Msg("FindSuccessor called")

	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id cannot be empty")
	}

	// Convert bytes to big.Int
	id := new(big.Int).SetBytes(req.Id)

	// Find successor
	successor, err := s.node.FindSuccessor(id)
	if err != nil {
		return nil, fmt.Errorf("find successor failed: %w", err)
	}

	// Convert to protobuf
	return &pb.FindSuccessorResponse{
		Successor: nodeAddressToProto(successor),
	}, nil
}

// FindSuccessorWithPath implements the FindSuccessorWithPath RPC.
// This is used for recursive path tracking between nodes.
func (s *GRPCServer) FindSuccessorWithPath(ctx context.Context, req *pb.FindSuccessorWithPathRequest) (*pb.FindSuccessorWithPathResponse, error) {
	s.logger.Debug().Msg("FindSuccessorWithPath called")

	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id cannot be empty")
	}

	// Convert bytes to big.Int
	id := new(big.Int).SetBytes(req.Id)

	// Find successor with path
	successor, path, err := s.node.FindSuccessorWithPath(id)
	if err != nil {
		return nil, fmt.Errorf("find successor with path failed: %w", err)
	}

	// Convert path to protobuf
	pbPath := make([]*pb.Node, len(path))
	for i, node := range path {
		pbPath[i] = nodeAddressToProto(node)
	}

	return &pb.FindSuccessorWithPathResponse{
		Successor: nodeAddressToProto(successor),
		Path:      pbPath,
	}, nil
}

// GetPredecessor implements the GetPredecessor RPC.
func (s *GRPCServer) GetPredecessor(ctx context.Context, req *pb.GetPredecessorRequest) (*pb.GetPredecessorResponse, error) {
	s.logger.Debug().Msg("GetPredecessor called")

	predecessor := s.node.GetPredecessor()

	return &pb.GetPredecessorResponse{
		Predecessor: nodeAddressToProto(predecessor),
	}, nil
}

// Notify implements the Notify RPC.
func (s *GRPCServer) Notify(ctx context.Context, req *pb.NotifyRequest) (*pb.NotifyResponse, error) {
	s.logger.Debug().Msg("Notify called")

	if req.Node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}

	// Convert from protobuf
	node := protoToNodeAddress(req.Node)

	// Call node's notify
	s.node.Notify(node)

	return &pb.NotifyResponse{Success: true}, nil
}

// GetSuccessorList implements the GetSuccessorList RPC.
func (s *GRPCServer) GetSuccessorList(ctx context.Context, req *pb.GetSuccessorListRequest) (*pb.GetSuccessorListResponse, error) {
	s.logger.Debug().Msg("GetSuccessorList called")

	successors := s.node.GetSuccessorList()

	// Filter out dead nodes before sending to frontend
	aliveSuccessors := s.filterAliveNodes(successors)

	// Convert to protobuf
	pbSuccessors := make([]*pb.Node, len(aliveSuccessors))
	for i, succ := range aliveSuccessors {
		pbSuccessors[i] = nodeAddressToProto(succ)
	}

	return &pb.GetSuccessorListResponse{
		Successors: pbSuccessors,
	}, nil
}

// Ping implements the Ping RPC.
func (s *GRPCServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	s.logger.Debug().Str("message", req.Message).Msg("Ping called")

	return &pb.PingResponse{
		Message:   "pong",
		Timestamp: time.Now().Unix(),
	}, nil
}

// GetNodeInfo implements the GetNodeInfo RPC.
func (s *GRPCServer) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	s.logger.Debug().Msg("GetNodeInfo called")

	nodeAddr := s.node.Address()

	// Get key count (user keys only, excluding Chord metadata)
	keyCount, err := s.node.GetKeyCount(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Failed to get key count")
		keyCount = 0 // Default to 0 on error
	}

	// Get replica count
	replicaCount, err := s.node.GetReplicaCount(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Failed to get replica count")
		replicaCount = 0 // Default to 0 on error
	}

	nodeProto := nodeAddressToProto(nodeAddr)
	nodeProto.KeyCount = int32(keyCount)
	nodeProto.ReplicaCount = int32(replicaCount)

	return &pb.GetNodeInfoResponse{
		Node: nodeProto,
	}, nil
}

// ClosestPrecedingFinger implements the ClosestPrecedingFinger RPC.
func (s *GRPCServer) ClosestPrecedingFinger(ctx context.Context, req *pb.ClosestPrecedingFingerRequest) (*pb.ClosestPrecedingFingerResponse, error) {
	s.logger.Debug().Msg("ClosestPrecedingFinger called")

	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id cannot be empty")
	}

	// Convert bytes to big.Int
	id := new(big.Int).SetBytes(req.Id)

	// Get closest preceding node
	node := s.node.ClosestPrecedingNode(id)

	return &pb.ClosestPrecedingFingerResponse{
		Node: nodeAddressToProto(node),
	}, nil
}

// TransferKeys implements the TransferKeys RPC.
func (s *GRPCServer) TransferKeys(ctx context.Context, req *pb.TransferKeysRequest) (*pb.TransferKeysResponse, error) {
	s.logger.Debug().Msg("TransferKeys called")

	if len(req.StartId) == 0 {
		return nil, fmt.Errorf("start_id cannot be empty")
	}
	if len(req.EndId) == 0 {
		return nil, fmt.Errorf("end_id cannot be empty")
	}

	// Convert bytes to big.Int
	startID := new(big.Int).SetBytes(req.StartId)
	endID := new(big.Int).SetBytes(req.EndId)

	// Get keys in the range from the node
	keys, err := s.node.TransferKeys(ctx, startID, endID)
	if err != nil {
		return nil, fmt.Errorf("failed to transfer keys: %w", err)
	}

	// Convert to protobuf format
	pbKeys := make([]*pb.KeyValuePair, 0, len(keys))
	for key, value := range keys {
		pbKeys = append(pbKeys, &pb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	s.logger.Info().
		Int("key_count", len(pbKeys)).
		Msg("Transferred keys")

	return &pb.TransferKeysResponse{
		Keys:  pbKeys,
		Count: int32(len(pbKeys)),
	}, nil
}

// DeleteTransferredKeys implements the DeleteTransferredKeys RPC.
func (s *GRPCServer) DeleteTransferredKeys(ctx context.Context, req *pb.DeleteTransferredKeysRequest) (*pb.DeleteTransferredKeysResponse, error) {
	s.logger.Debug().Msg("DeleteTransferredKeys called")

	if len(req.StartId) == 0 {
		return nil, fmt.Errorf("start_id cannot be empty")
	}
	if len(req.EndId) == 0 {
		return nil, fmt.Errorf("end_id cannot be empty")
	}

	// Convert bytes to big.Int
	startID := new(big.Int).SetBytes(req.StartId)
	endID := new(big.Int).SetBytes(req.EndId)

	// Delete keys in the range
	count, err := s.node.DeleteTransferredKeys(ctx, startID, endID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete transferred keys: %w", err)
	}

	s.logger.Info().
		Int("key_count", count).
		Str("start_id", startID.Text(16)[:8]).
		Str("end_id", endID.Text(16)[:8]).
		Msg("Deleted transferred keys")

	return &pb.DeleteTransferredKeysResponse{
		Success: true,
		Count:   int32(count),
	}, nil
}

// Get implements the Get RPC for DHT operations.
func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("Get called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Get value from DHT
	value, found, err := s.node.Get(ctx, req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	return &pb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

// Set implements the Set RPC for DHT operations.
func (s *GRPCServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("Set called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Store value in DHT (no TTL for MVP)
	if err := s.node.Set(ctx, req.Key, req.Value, 0); err != nil {
		return nil, fmt.Errorf("failed to set key: %w", err)
	}

	return &pb.SetResponse{
		Success: true,
	}, nil
}

// Delete implements the Delete RPC for DHT operations.
func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("Delete called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Delete key from DHT
	if err := s.node.Delete(ctx, req.Key); err != nil {
		return nil, fmt.Errorf("failed to delete key: %w", err)
	}

	return &pb.DeleteResponse{
		Success: true,
	}, nil
}

// LookupPath implements the LookupPath RPC.
func (s *GRPCServer) LookupPath(ctx context.Context, req *pb.LookupPathRequest) (*pb.LookupPathResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("LookupPath called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Hash the key
	keyHash := s.node.HashKey(req.Key)

	// Find successor with path tracking
	responsibleNode, path, err := s.node.FindSuccessorWithPath(keyHash)
	if err != nil {
		return nil, fmt.Errorf("failed to find successor with path: %w", err)
	}

	// Convert path to protobuf
	pbPath := make([]*pb.Node, len(path))
	for i, node := range path {
		pbPath[i] = nodeAddressToProto(node)
	}

	return &pb.LookupPathResponse{
		Key:             req.Key,
		KeyHash:         keyHash.Bytes(),
		ResponsibleNode: nodeAddressToProto(responsibleNode),
		Path:            pbPath,
		Hops:            int32(len(path) - 1), // Hops = path length - 1
	}, nil
}

// GetFingerTable implements the GetFingerTable RPC.
func (s *GRPCServer) GetFingerTable(ctx context.Context, req *pb.GetFingerTableRequest) (*pb.GetFingerTableResponse, error) {
	s.logger.Debug().Msg("GetFingerTable called")

	fingerTable := s.node.GetFingerTable()

	// Convert to protobuf, filtering out entries with dead nodes
	pbEntries := make([]*pb.FingerTableEntry, 0, len(fingerTable))
	for i, entry := range fingerTable {
		// Check if the node is alive (or nil)
		if entry.Node != nil && !s.node.IsNodeAlive(entry.Node) {
			// Skip dead nodes in finger table
			continue
		}

		pbEntries = append(pbEntries, &pb.FingerTableEntry{
			Start: entry.Start.Bytes(),
			Node:  nodeAddressToProto(entry.Node),
			Index: int32(i),
		})
	}

	return &pb.GetFingerTableResponse{
		Entries: pbEntries,
	}, nil
}

// SetReplica implements the SetReplica RPC for replication.
func (s *GRPCServer) SetReplica(ctx context.Context, req *pb.SetReplicaRequest) (*pb.SetReplicaResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("SetReplica called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Store replica in local storage
	if err := s.node.SetReplica(ctx, req.Key, req.Value, time.Duration(req.TtlSeconds)*time.Second); err != nil {
		return nil, fmt.Errorf("failed to set replica: %w", err)
	}

	return &pb.SetReplicaResponse{
		Success: true,
	}, nil
}

// GetReplica implements the GetReplica RPC for retrieving replicas.
func (s *GRPCServer) GetReplica(ctx context.Context, req *pb.GetReplicaRequest) (*pb.GetReplicaResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("GetReplica called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Get replica from local storage
	value, found, err := s.node.GetReplica(ctx, req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica: %w", err)
	}

	return &pb.GetReplicaResponse{
		Value: value,
		Found: found,
	}, nil
}

// DeleteReplica implements the DeleteReplica RPC for deleting replicas.
func (s *GRPCServer) DeleteReplica(ctx context.Context, req *pb.DeleteReplicaRequest) (*pb.DeleteReplicaResponse, error) {
	s.logger.Debug().Str("key", req.Key).Msg("DeleteReplica called")

	if req.Key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// Delete replica from local storage
	if err := s.node.DeleteReplica(ctx, req.Key); err != nil {
		return nil, fmt.Errorf("failed to delete replica: %w", err)
	}

	return &pb.DeleteReplicaResponse{
		Success: true,
	}, nil
}

// BulkStore stores multiple key-value pairs efficiently.
func (s *GRPCServer) BulkStore(ctx context.Context, req *pb.BulkStoreRequest) (*pb.BulkStoreResponse, error) {
	s.logger.Debug().Int("item_count", len(req.Items)).Msg("BulkStore called")

	if len(req.Items) == 0 {
		return &pb.BulkStoreResponse{
			Success: true,
			Count:   0,
		}, nil
	}

	// Store each item
	stored := int32(0)
	for key, value := range req.Items {
		// Store with the raw key (it's already hashed from the sender)
		if err := s.node.SetRaw(ctx, key, value); err != nil {
			s.logger.Error().Err(err).Str("key", key).Msg("Failed to store item in bulk operation")
			// Continue with other items even if one fails
		} else {
			stored++
		}
	}

	return &pb.BulkStoreResponse{
		Success: stored > 0,
		Count:   stored,
		Error:   "",
	}, nil
}

// NotifyPredecessorLeaving handles notification that our successor is leaving.
func (s *GRPCServer) NotifyPredecessorLeaving(ctx context.Context, req *pb.NotifyPredecessorLeavingRequest) (*pb.NotifyPredecessorLeavingResponse, error) {
	s.logger.Debug().Msg("NotifyPredecessorLeaving called")

	if req.NewSuccessor == nil {
		return nil, fmt.Errorf("new successor cannot be nil")
	}

	// Convert protobuf node to NodeAddress
	newSuccessor := protoToNodeAddress(req.NewSuccessor)
	if newSuccessor == nil {
		return nil, fmt.Errorf("invalid new successor node")
	}

	// Update our successor to the leaving node's successor
	s.node.SetSuccessor(newSuccessor)

	s.logger.Info().
		Str("new_successor", newSuccessor.Address()).
		Msg("Updated successor due to predecessor leaving")

	return &pb.NotifyPredecessorLeavingResponse{
		Success: true,
	}, nil
}

// NotifySuccessorLeaving handles notification that our predecessor is leaving.
func (s *GRPCServer) NotifySuccessorLeaving(ctx context.Context, req *pb.NotifySuccessorLeavingRequest) (*pb.NotifySuccessorLeavingResponse, error) {
	s.logger.Debug().Msg("NotifySuccessorLeaving called")

	if req.NewPredecessor == nil {
		return nil, fmt.Errorf("new predecessor cannot be nil")
	}

	// Convert protobuf node to NodeAddress
	newPredecessor := protoToNodeAddress(req.NewPredecessor)
	if newPredecessor == nil {
		return nil, fmt.Errorf("invalid new predecessor node")
	}

	// Update our predecessor to the leaving node's predecessor
	s.node.SetPredecessor(newPredecessor)

	s.logger.Info().
		Str("new_predecessor", newPredecessor.Address()).
		Msg("Updated predecessor due to successor leaving")

	return &pb.NotifySuccessorLeavingResponse{
		Success: true,
	}, nil
}

// NotifyNodeLeaving handles notification that a node in the ring is leaving.
func (s *GRPCServer) NotifyNodeLeaving(ctx context.Context, req *pb.NotifyNodeLeavingRequest) (*pb.NotifyNodeLeavingResponse, error) {
	s.logger.Debug().Msg("NotifyNodeLeaving called")

	if req.LeavingNode == nil {
		return nil, fmt.Errorf("leaving node cannot be nil")
	}

	// Convert protobuf node to NodeAddress
	leavingNode := protoToNodeAddress(req.LeavingNode)
	if leavingNode == nil {
		return nil, fmt.Errorf("invalid leaving node")
	}

	// Remove the leaving node from our successor list if present
	s.node.RemoveFromSuccessorList(leavingNode)

	s.logger.Info().
		Str("leaving_node", leavingNode.Address()).
		Msg("Processed node leaving notification")

	return &pb.NotifyNodeLeavingResponse{
		Success: true,
	}, nil
}

// Helper functions for type conversion

// nodeAddressToProto converts a NodeAddress to protobuf Node.
func nodeAddressToProto(addr *chord.NodeAddress) *pb.Node {
	if addr == nil {
		return nil
	}

	return &pb.Node{
		Id:       addr.ID.Bytes(),
		Host:     addr.Host,
		Port:     int32(addr.Port),
		HttpPort: int32(addr.HTTPPort),
	}
}

// protoToNodeAddress converts a protobuf Node to NodeAddress.
func protoToNodeAddress(node *pb.Node) *chord.NodeAddress {
	if node == nil {
		return nil
	}

	id := new(big.Int).SetBytes(node.Id)
	return chord.NewNodeAddress(id, node.Host, int(node.Port), int(node.HttpPort))
}

// filterAliveNodes filters a list of NodeAddresses to only include alive nodes.
// This is used to clean dead nodes from successor lists before sending to frontend.
func (s *GRPCServer) filterAliveNodes(nodes []*chord.NodeAddress) []*chord.NodeAddress {
	if len(nodes) == 0 {
		return nodes
	}

	alive := make([]*chord.NodeAddress, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}

		// Check if node is alive
		if s.node.IsNodeAlive(node) {
			alive = append(alive, node)
		}
	}

	return alive
}
