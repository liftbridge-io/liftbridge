//go:generate protoc -I=. -I=$GOPATH/src --gofast_out=. ./server/proto/internal.proto

package main

import (
	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"

	"github.com/tylertreat/jetbridge/server"
)

func main() {
	config := server.Config{
		Logger:   log.New(),
		NATSOpts: nats.GetDefaultOptions(),
		Addr:     ":9292",
	}
	config.Logger.SetLevel(log.DebugLevel)
	//config.Log.Compact = true
	config.Log.MaxSegmentBytes = 60
	config.Clustering.NodeID = "test-node"
	config.Clustering.RaftSnapshots = 2
	config.Clustering.RaftCacheSize = 512
	config.Clustering.Bootstrap = true
	config.Clustering.RaftLogging = true
	server := server.New(config)
	if err := server.Start(); err != nil {
		panic(err)
	}
}
