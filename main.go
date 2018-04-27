//go:generate protoc -I=. --gofast_out=. ./server/proto/internal.proto

package main

import (
	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"

	"github.com/tylertreat/jetbridge/server"
)

func main() {
	config := server.Config{
		Logger:        log.New(),
		NATSOpts:      nats.GetDefaultOptions(),
		Addr:          ":9292",
		RaftSnapshots: 2,
		RaftCacheSize: 512,
	}
	server := server.New(config)
	if err := server.Start(); err != nil {
		panic(err)
	}
}
