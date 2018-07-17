//go:generate protoc -I=. -I=$GOPATH/src --gofast_out=. ./server/proto/internal.proto

package main

import (
	"os"
	"runtime"

	"github.com/nats-io/go-nats"
	"github.com/urfave/cli"

	"github.com/liftbridge-io/liftbridge/server"
)

const version = "0.0.1"

func main() {
	app := cli.NewApp()
	app.Name = "liftbridge"
	app.Usage = "Lightweight, fault-tolerant message streams"
	app.Version = version
	app.Flags = getFlags()
	app.Action = func(c *cli.Context) error {
		config, err := server.NewConfig(c.String("config"))
		if err != nil {
			return err
		}
		if id := c.String("id"); id != "" {
			config.Clustering.ServerID = id
		}
		config.Clustering.Namespace = c.String("namespace")
		config.DataDir = c.String("data-dir")
		config.Port = c.Int("port")
		level, err := server.GetLogLevel(c.String("level"))
		if err != nil {
			return err
		}
		config.LogLevel = level
		config.Clustering.RaftBootstrap = c.Bool("raft-bootstrap-seed")
		config.Clustering.RaftBootstrapPeers = c.StringSlice("raft-bootstrap-peers")

		server := server.New(config)
		if err := server.Start(); err != nil {
			return err
		}
		runtime.Goexit()
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}

func getFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "load configuration from `FILE`",
		},
		cli.StringFlag{
			Name:  "server-id, id",
			Usage: "ID of the server in the cluster if there is no stored ID",
		},
		cli.StringFlag{
			Name:  "namespace, ns",
			Usage: "cluster namespace",
			Value: server.DefaultNamespace,
		},
		cli.StringFlag{
			Name:  "nats-server, n",
			Usage: "connect to NATS server at `ADDR`",
			Value: nats.DefaultURL,
		},
		cli.StringFlag{
			Name:  "data-dir, d",
			Usage: "store data in `DIR` (default: \"/tmp/liftbridge/<namespace>\")",
		},
		cli.IntFlag{
			Name:  "port, p",
			Usage: "port to bind to",
			Value: server.DefaultPort,
		},
		cli.StringFlag{
			Name:  "level, l",
			Usage: "logging level [debug|info|warn|error]",
			Value: "info",
		},
		cli.BoolFlag{
			Name:  "raft-bootstrap-seed",
			Usage: "bootstrap the Raft cluster by electing self as leader if there is no existing state",
		},
		cli.StringSliceFlag{
			Name:  "raft-bootstrap-peers",
			Usage: "bootstrap the Raft cluster with the provided list of peer IDs if there is no existing state",
		},
	}
}
