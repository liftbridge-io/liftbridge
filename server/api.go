package server

import (
	log "github.com/sirupsen/logrus"
	"github.com/tylertreat/go-jetbridge/proto"
	"golang.org/x/net/context"
)

type apiServer struct {
	logger *log.Logger
}

func (a *apiServer) CreateStream(ctx context.Context, req *proto.CreateStreamRequest) (*proto.CreateStreamResponse, error) {
	// TODO
	a.logger.Infof("CreateStream[subject=%s, name=%s, replicationFactor=%d]",
		req.Subject, req.Name, req.ReplicationFactor)
	resp := &proto.CreateStreamResponse{Success: true}
	return resp, nil
}
