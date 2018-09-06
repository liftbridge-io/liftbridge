//go:generate protoc -I=. -I=$GOPATH/src --gofast_out=. ./server/proto/internal.proto

package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

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
		// Read config from file if present.
		config, err := server.NewConfig(c.String("config"))
		if err != nil {
			return err
		}

		// Override with flags.
		if c.IsSet("id") {
			config.Clustering.ServerID = c.String("id")
		}
		if c.IsSet("namespace") {
			config.Clustering.Namespace = c.String("namespace")
		}
		if c.IsSet("port") {
			config.Port = c.Int("port")
		}
		if c.IsSet("level") {
			level, err := server.GetLogLevel(c.String("level"))
			if err != nil {
				return err
			}
			config.LogLevel = level
		}
		if c.IsSet("raft-bootstrap-seed") {
			config.Clustering.RaftBootstrapSeed = c.Bool("raft-bootstrap-seed")
		}
		if c.IsSet("raft-bootstrap-peers") {
			config.Clustering.RaftBootstrapPeers = c.StringSlice("raft-bootstrap-peers")
		}
		if c.IsSet("data-dir") {
			config.DataDir = c.String("data-dir")
		}
		if c.IsSet("tls-cert") {
			config.TLSCert = c.String("tls-cert")
		}
		if c.IsSet("tls-key") {
			config.TLSKey = c.String("tls-key")
		}
		if c.IsSet("nats-servers") {
			natsServers, err := normalizeNatsServers(c.StringSlice("nats-servers"))
			if err != nil {
				return err
			}
			config.NATS.Servers = natsServers
		}

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
			Usage: "ID of the server in the cluster if there is no stored ID (default: random ID)",
		},
		cli.StringFlag{
			Name:  "namespace, ns",
			Usage: "cluster namespace",
			Value: server.DefaultNamespace,
		},
		cli.StringSliceFlag{
			Name:  "nats-servers, n",
			Usage: fmt.Sprintf("connect to NATS cluster at `ADDR[,ADDR]` (default: %q)", nats.DefaultURL),
			// NOTE: cannot use Value here as urfave/cli has another bug
			// where it does not replace this value with the specified values but appends them:-(
			// Value: &cli.StringSlice{nats.DefaultURL},
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
			Name:  "tls-cert",
			Usage: "server certificate file",
		},
		cli.StringFlag{
			Name:  "tls-key",
			Usage: "private key for server certificate",
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

func normalizeNatsServers(natsServers []string) ([]string, error) {
	if natsServers != nil {
		// urlfave.cli has issues with *Slice flags - it doesn't yet parse
		// command-line entries the same way as env vars, see
		// https://github.com/urfave/cli/pull/605
		// It has been around since Mar 2017 so don't hold your breath for a fix!
		// ... so we are manually splitting here for now.
		// We also need to handle possible multiple --nats-servers on the cli as this is supported.
		allNatsServers := make([]string, 0)
		for _, natsServersString := range natsServers {
			currNatsServers := strings.Split(natsServersString, ",")
			for i := range currNatsServers {
				if trimmedNatsServer := strings.TrimSpace(currNatsServers[i]); trimmedNatsServer != "" {
					// TODO: validate the server URL and return error?
					allNatsServers = append(allNatsServers, trimmedNatsServer)
				}
			}
		}
		return allNatsServers, nil
	}
	return nil, nil
}
