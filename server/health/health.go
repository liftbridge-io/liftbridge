package health

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const serviceName = "proto.API" // taken from compiled protobuf file api.go.pb (line 793)

var server = health.NewServer()

// Register the health service with a gRPC server.
func Register(srv *grpc.Server) {
	grpc_health_v1.RegisterHealthServer(srv, server)
}

// SetServing marks the service as healthy.
func SetServing() {
	server.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_SERVING)
}

// SetNotServing marks the service as unhealthy.
func SetNotServing() {
	server.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}
