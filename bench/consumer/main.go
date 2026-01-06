package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/urfave/cli"

	"github.com/liftbridge-io/liftbridge/bench/common"
)

func main() {
	app := cli.NewApp()
	app.Name = "liftbridge-bench-consumer"
	app.Usage = "Benchmark tool for Liftbridge message consumption"
	app.Version = "1.0.0"
	app.Flags = getFlags()
	app.Action = run
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringSliceFlag{
			Name:   "servers, s",
			Usage:  "Liftbridge server addresses",
			EnvVar: "LIFTBRIDGE_SERVERS",
		},
		cli.StringFlag{
			Name:  "stream",
			Usage: "Stream name to consume from",
			Value: "bench-stream",
		},
		cli.IntFlag{
			Name:  "partition",
			Usage: "Partition to consume from",
			Value: 0,
		},
		cli.IntFlag{
			Name:  "expected, n",
			Usage: "Expected number of messages to consume (0 = unlimited)",
			Value: 0,
		},
		cli.DurationFlag{
			Name:  "duration, d",
			Usage: "Maximum duration to run the consumer benchmark",
			Value: 30 * time.Second,
		},
		cli.StringFlag{
			Name:  "start",
			Usage: "Start position: earliest, latest, offset:<n>",
			Value: "earliest",
		},
		cli.StringFlag{
			Name:  "output, o",
			Usage: "Output format: text, json",
			Value: "text",
		},
	}
}

func run(c *cli.Context) error {
	// Parse servers
	servers := c.StringSlice("servers")
	if len(servers) == 0 {
		servers = []string{"localhost:9292"}
	}
	servers = normalizeServers(servers)

	streamName := c.String("stream")
	partition := c.Int("partition")
	expected := c.Int("expected")
	duration := c.Duration("duration")
	startPos := c.String("start")
	outputFormat := c.String("output")

	// Connect to Liftbridge
	fmt.Printf("Connecting to Liftbridge: %v\n", servers)
	client, err := lift.Connect(servers)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	// Parse start option
	startOpt, err := parseStartOption(startPos)
	if err != nil {
		return err
	}

	// Setup stats
	stats := common.NewStats()

	// Setup cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Message counter
	var messageCount int64
	var closeOnce sync.Once
	done := make(chan struct{})

	fmt.Printf("Subscribing to stream '%s' partition %d...\n", streamName, partition)
	if expected > 0 {
		fmt.Printf("Will consume until %d messages received or %s timeout\n", expected, duration)
	} else {
		fmt.Printf("Will consume for %s\n", duration)
	}
	fmt.Println("---")

	// Start benchmark
	stats.Start()

	// Subscribe
	err = client.Subscribe(ctx, streamName, func(msg *lift.Message, err error) {
		if err != nil {
			stats.RecordError()
			return
		}

		stats.RecordReceived(len(msg.Value()))
		count := atomic.AddInt64(&messageCount, 1)

		if expected > 0 && count >= int64(expected) {
			closeOnce.Do(func() { close(done) })
		}
	}, startOpt, lift.Partition(int32(partition)))

	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Wait for completion
	select {
	case <-done:
		fmt.Println("Received expected number of messages")
	case <-time.After(duration):
		fmt.Println("Duration timeout reached")
	}

	stats.Stop()
	cancel()

	// Print results
	common.PrintConsumerResults(stats, outputFormat)

	return nil
}

func parseStartOption(pos string) (lift.SubscriptionOption, error) {
	pos = strings.ToLower(strings.TrimSpace(pos))

	switch {
	case pos == "earliest":
		return lift.StartAtEarliestReceived(), nil
	case pos == "latest":
		return lift.StartAtLatestReceived(), nil
	case strings.HasPrefix(pos, "offset:"):
		offsetStr := strings.TrimPrefix(pos, "offset:")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid offset: %s", offsetStr)
		}
		return lift.StartAtOffset(offset), nil
	default:
		return nil, fmt.Errorf("invalid start position: %s (use earliest, latest, or offset:<n>)", pos)
	}
}

func normalizeServers(servers []string) []string {
	var result []string
	for _, s := range servers {
		parts := strings.Split(s, ",")
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				result = append(result, trimmed)
			}
		}
	}
	return result
}
