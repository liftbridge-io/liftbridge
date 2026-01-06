package main

import (
	"context"
	"fmt"
	"os"
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
	app.Name = "liftbridge-bench-producer"
	app.Usage = "Benchmark tool for Liftbridge message ingestion"
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
			Usage: "Stream name to publish to",
			Value: "bench-stream",
		},
		cli.StringFlag{
			Name:  "subject",
			Usage: "NATS subject for the stream",
			Value: "bench-subject",
		},
		cli.IntFlag{
			Name:  "messages, n",
			Usage: "Total number of messages to publish",
			Value: 100000,
		},
		cli.IntFlag{
			Name:  "message-size, ms",
			Usage: "Size of each message payload in bytes",
			Value: 256,
		},
		cli.IntFlag{
			Name:  "pub-batch, pb",
			Usage: "Number of async publishes before waiting for acks (like JetStream pubbatch)",
			Value: 1,
		},
		cli.IntFlag{
			Name:  "partitions, p",
			Usage: "Number of partitions for the stream",
			Value: 1,
		},
		cli.StringFlag{
			Name:  "ack-policy",
			Usage: "Ack policy: none, leader, all",
			Value: "leader",
		},
		cli.IntFlag{
			Name:  "concurrent, c",
			Usage: "Number of concurrent publisher goroutines",
			Value: 1,
		},
		cli.BoolFlag{
			Name:  "create-stream",
			Usage: "Create the stream before benchmarking",
		},
		cli.BoolFlag{
			Name:  "delete-stream",
			Usage: "Delete the stream after benchmarking",
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
	subject := c.String("subject")
	numMessages := c.Int("messages")
	messageSize := c.Int("message-size")
	pubBatch := c.Int("pub-batch")
	partitions := c.Int("partitions")
	ackPolicy := strings.ToLower(c.String("ack-policy"))
	concurrent := c.Int("concurrent")
	createStream := c.Bool("create-stream")
	deleteStream := c.Bool("delete-stream")
	outputFormat := c.String("output")

	// Validate
	if numMessages <= 0 {
		return fmt.Errorf("messages must be > 0")
	}
	if messageSize <= 0 {
		return fmt.Errorf("message-size must be > 0")
	}
	if concurrent <= 0 {
		concurrent = 1
	}

	// Connect to Liftbridge
	fmt.Printf("Connecting to Liftbridge: %v\n", servers)
	client, err := lift.Connect(servers)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create stream if requested
	if createStream {
		fmt.Printf("Creating stream '%s' with %d partition(s)...\n", streamName, partitions)
		err = client.CreateStream(ctx, subject, streamName, lift.Partitions(int32(partitions)))
		if err != nil && err != lift.ErrStreamExists {
			return fmt.Errorf("failed to create stream: %w", err)
		}
		if err == lift.ErrStreamExists {
			fmt.Println("Stream already exists, continuing...")
		}
	}

	// Pre-generate messages (NOT timed)
	fmt.Printf("Pre-generating %d messages of %d bytes each...\n", numMessages, messageSize)
	messages := common.PreGenerateMessagesFlat(numMessages, messageSize)
	fmt.Printf("Generated %d messages (%.2f MB total)\n",
		len(messages), float64(len(messages)*messageSize)/1024/1024)

	// Setup stats
	stats := common.NewStats()

	// Warn about pub-batch with concurrency
	if pubBatch > 1 && concurrent > 1 {
		fmt.Println("WARNING: pub-batch > 1 with concurrent > 1 may hang due to go-liftbridge client limitations.")
		fmt.Println("         Consider using either pub-batch=1 with high concurrency, or pub-batch>1 with concurrent=1.")
	}

	// Run benchmark
	fmt.Printf("Starting benchmark with %d concurrent publisher(s), pub-batch=%d, ack-policy=%s...\n", concurrent, pubBatch, ackPolicy)
	fmt.Println("---")

	stats.Start()
	err = runBenchmark(ctx, client, streamName, messages, concurrent, pubBatch, ackPolicy, stats)
	stats.Stop()

	if err != nil {
		return fmt.Errorf("benchmark failed: %w", err)
	}

	// Print results
	common.PrintProducerResults(stats, outputFormat)

	// Delete stream if requested
	if deleteStream {
		fmt.Printf("Deleting stream '%s'...\n", streamName)
		if err := client.DeleteStream(ctx, streamName); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to delete stream: %v\n", err)
		}
	}

	return nil
}

func runBenchmark(
	ctx context.Context,
	client lift.Client,
	stream string,
	messages []common.PreparedMessage,
	concurrent int,
	pubBatch int,
	ackPolicy string,
	stats *common.Stats,
) error {
	var wg sync.WaitGroup

	totalMessages := len(messages)
	messagesPerWorker := totalMessages / concurrent
	remainder := totalMessages % concurrent

	// Get ack policy option
	ackOpt := getAckPolicyOption(ackPolicy)

	// Progress counter
	var published int64
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	// Progress reporter
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-progressTicker.C:
				count := atomic.LoadInt64(&published)
				pct := float64(count) / float64(totalMessages) * 100
				fmt.Printf("Progress: %d/%d (%.1f%%)\n", count, totalMessages, pct)
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < concurrent; i++ {
		start := i * messagesPerWorker
		end := start + messagesPerWorker
		if i == concurrent-1 {
			end += remainder
		}

		workerMessages := messages[start:end]
		wg.Add(1)

		go func(msgs []common.PreparedMessage) {
			defer wg.Done()

			if pubBatch <= 1 {
				// Synchronous mode: one message at a time
				for _, msg := range msgs {
					sendTime := time.Now()
					_, err := client.Publish(ctx, stream, msg.Value,
						lift.Key(msg.Key),
						ackOpt,
					)
					latency := time.Since(sendTime)

					if err != nil {
						stats.RecordError()
						continue
					}

					stats.RecordLatency(latency)
					stats.RecordSent(len(msg.Value))
					atomic.AddInt64(&published, 1)
				}
			} else {
				// Async batch mode: send pubBatch messages, then wait for all acks
				for i := 0; i < len(msgs); i += pubBatch {
					batchEnd := i + pubBatch
					if batchEnd > len(msgs) {
						batchEnd = len(msgs)
					}
					batch := msgs[i:batchEnd]

					var batchWg sync.WaitGroup
					batchWg.Add(len(batch))

					for _, msg := range batch {
						msgVal := msg.Value
						msgKey := msg.Key
						sendTime := time.Now()

						client.PublishAsync(ctx, stream, msgVal,
							func(ack *lift.Ack, err error) {
								defer batchWg.Done()
								latency := time.Since(sendTime)

								if err != nil {
									stats.RecordError()
									return
								}

								stats.RecordLatency(latency)
								stats.RecordSent(len(msgVal))
								atomic.AddInt64(&published, 1)
							},
							lift.Key(msgKey),
							ackOpt,
						)
					}

					// Wait for all acks in this batch before sending next batch
					batchWg.Wait()
				}
			}
		}(workerMessages)
	}

	wg.Wait()
	close(done)

	return nil
}

func getAckPolicyOption(policy string) lift.MessageOption {
	switch policy {
	case "none":
		return lift.AckPolicyNone()
	case "all":
		return lift.AckPolicyAll()
	default:
		return lift.AckPolicyLeader()
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
