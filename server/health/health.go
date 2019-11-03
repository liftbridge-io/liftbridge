package health

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	ServiceName = "proto.API" // taken from compiled protobuf file api.go.pb (line 793)
)

var server = health.NewServer()

func Register(srv *grpc.Server) {
	grpc_health_v1.RegisterHealthServer(srv, server)
}

func SetServing() {
	server.SetServingStatus(ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
}

func SetNotServing() {
	server.SetServingStatus(ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}
