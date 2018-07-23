# go-liftbridge [![CircleCI](https://circleci.com/gh/liftbridge-io/go-liftbridge.svg?style=svg)](https://circleci.com/gh/liftbridge-io/go-liftbridge) [![GoDoc](https://godoc.org/github.com/liftbridge-io/go-liftbridge?status.svg)](https://godoc.org/github.com/liftbridge-io/go-liftbridge)

Go client for [Liftbridge](https://github.com/liftbridge-io/liftbridge).

## Installation

```
$ go get github.com/liftbridge-io/go-liftbridge
```

## Example

```go
package main

import (
	"fmt"

	"github.com/liftbridge-io/go-liftbridge"
	lift "github.com/liftbridge-io/go-liftbridge/liftbridge-grpc"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := liftbridge.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	stream := liftbridge.StreamInfo{
		Subject:           "foo",
		Name:              "foo-stream",
		ReplicationFactor: 3,
	}
	if err := client.CreateStream(context.Background(), stream); err != nil {
		if err != liftbridge.ErrStreamExists {
			panic(err)
		}
	}

	// Subscribe to the stream.
	ctx := context.Background()
	if err := client.Subscribe(ctx, stream.Subject, stream.Name, func(msg *lift.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset, string(msg.Value))
	}); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
```
