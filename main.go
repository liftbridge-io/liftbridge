//go:generate protoc -I=. -I=$GOPATH/src --gofast_out=. ./server/proto/internal.proto

package main

import (
	"os"
	"runtime"

	"github.com/urfave/cli"

	"github.com/tylertreat/liftbridge/server"
)

const version = "0.0.1"

func main() {
	app := cli.NewApp()
	app.Name = "liftbridge"
	app.Usage = "Durable stream augmentation for NATS"
	app.Version = version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "Load configuration from `FILE`",
		},
	}
	app.Action = func(c *cli.Context) error {
		config, err := server.NewConfig(c.String("config"))
		if err != nil {
			return err
		}
		server := server.New(config)
		return server.Start()
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
	runtime.Goexit()
}
