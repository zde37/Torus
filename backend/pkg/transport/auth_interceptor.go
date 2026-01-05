package transport

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// AuthTokenHeader is the metadata key for authentication tokens
	AuthTokenHeader = "x-auth-token"
)

// AuthInterceptor creates a gRPC unary interceptor that validates auth tokens.
// If expectedToken is empty, authentication is disabled (allows anyone to join).
func AuthInterceptor(expectedToken string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// If no auth token configured, allow all requests
		if expectedToken == "" {
			return handler(ctx, req)
		}

		// Extract metadata from context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get auth token from metadata
		tokens := md.Get(AuthTokenHeader)
		if len(tokens) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing auth token")
		}

		// Verify token
		if tokens[0] != expectedToken {
			return nil, status.Error(codes.Unauthenticated, "invalid auth token")
		}

		// Token is valid, proceed with the request
		return handler(ctx, req)
	}
}
