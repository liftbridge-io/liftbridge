package health

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestRegister(t *testing.T) {
	srv := grpc.NewServer()
	Register(srv)

	// Verify the health service was registered by checking service info
	info := srv.GetServiceInfo()
	_, ok := info["grpc.health.v1.Health"]
	require.True(t, ok, "health service should be registered")
}

func TestSetServing(t *testing.T) {
	// Create a gRPC server with health service
	srv := grpc.NewServer()
	Register(srv)

	// Start the server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// Connect a client
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)

	// Set serving and check status
	SetServing()

	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}

func TestSetNotServing(t *testing.T) {
	// Create a gRPC server with health service
	srv := grpc.NewServer()
	Register(srv)

	// Start the server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// Connect a client
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)

	// Set not serving and check status
	SetNotServing()

	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp.Status)
}

func TestSetServingThenNotServing(t *testing.T) {
	// Create a gRPC server with health service
	srv := grpc.NewServer()
	Register(srv)

	// Start the server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// Connect a client
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)

	// Start serving
	SetServing()
	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	// Then stop serving
	SetNotServing()
	resp, err = client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp.Status)

	// Then start serving again
	SetServing()
	resp, err = client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: serviceName,
	})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}

func TestServiceName(t *testing.T) {
	// Verify the service name constant is as expected
	require.Equal(t, "proto.API", serviceName)
}
