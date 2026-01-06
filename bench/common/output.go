package common

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
)

// BenchmarkResult holds the formatted benchmark results.
type BenchmarkResult struct {
	Duration          string  `json:"duration"`
	TotalMessages     int64   `json:"total_messages"`
	TotalBytes        int64   `json:"total_bytes"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
	MBPerSecond       float64 `json:"mb_per_second"`
	LatencyMin        string  `json:"latency_min,omitempty"`
	LatencyMean       string  `json:"latency_mean,omitempty"`
	LatencyP50        string  `json:"latency_p50,omitempty"`
	LatencyP95        string  `json:"latency_p95,omitempty"`
	LatencyP99        string  `json:"latency_p99,omitempty"`
	LatencyP999       string  `json:"latency_p999,omitempty"`
	LatencyMax        string  `json:"latency_max,omitempty"`
	Errors            int64   `json:"errors"`
}

// PrintProducerResults outputs the producer benchmark results.
func PrintProducerResults(stats *Stats, format string) {
	result := BenchmarkResult{
		Duration:          stats.Duration().String(),
		TotalMessages:     stats.MessagesSent(),
		TotalBytes:        stats.BytesSent(),
		MessagesPerSecond: stats.MessagesPerSecond(),
		BytesPerSecond:    stats.BytesPerSecond(),
		MBPerSecond:       stats.MBPerSecond(),
		Errors:            stats.Errors(),
	}

	// Include latency stats if we have samples
	if stats.LatencyCount() > 0 {
		result.LatencyMin = stats.LatencyMin().String()
		result.LatencyMean = stats.LatencyMean().String()
		result.LatencyP50 = stats.LatencyPercentile(50).String()
		result.LatencyP95 = stats.LatencyPercentile(95).String()
		result.LatencyP99 = stats.LatencyPercentile(99).String()
		result.LatencyP999 = stats.LatencyPercentile(99.9).String()
		result.LatencyMax = stats.LatencyMax().String()
	}

	switch format {
	case "json":
		printJSON(result)
	default:
		printTextProducer(result)
	}
}

// PrintConsumerResults outputs the consumer benchmark results.
func PrintConsumerResults(stats *Stats, format string) {
	result := BenchmarkResult{
		Duration:          stats.Duration().String(),
		TotalMessages:     stats.MessagesReceived(),
		TotalBytes:        stats.BytesReceived(),
		MessagesPerSecond: stats.MessagesPerSecond(),
		BytesPerSecond:    stats.BytesPerSecond(),
		MBPerSecond:       stats.MBPerSecond(),
		Errors:            stats.Errors(),
	}

	switch format {
	case "json":
		printJSON(result)
	default:
		printTextConsumer(result)
	}
}

func printJSON(result BenchmarkResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(result)
}

func printTextProducer(r BenchmarkResult) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "=== Producer Benchmark Results ===")
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Duration:\t%s\n", r.Duration)
	fmt.Fprintf(w, "Messages Sent:\t%s\n", humanize.Comma(r.TotalMessages))
	fmt.Fprintf(w, "Bytes Sent:\t%s\n", humanize.Bytes(uint64(r.TotalBytes)))
	fmt.Fprintf(w, "Throughput:\t%s msgs/sec\n", humanize.CommafWithDigits(r.MessagesPerSecond, 2))
	fmt.Fprintf(w, "Bandwidth:\t%.2f MB/sec\n", r.MBPerSecond)
	fmt.Fprintln(w, "")

	if r.LatencyP50 != "" {
		fmt.Fprintln(w, "--- Ack Latency ---")
		fmt.Fprintf(w, "Min:\t%s\n", r.LatencyMin)
		fmt.Fprintf(w, "Mean:\t%s\n", r.LatencyMean)
		fmt.Fprintf(w, "P50:\t%s\n", r.LatencyP50)
		fmt.Fprintf(w, "P95:\t%s\n", r.LatencyP95)
		fmt.Fprintf(w, "P99:\t%s\n", r.LatencyP99)
		fmt.Fprintf(w, "P99.9:\t%s\n", r.LatencyP999)
		fmt.Fprintf(w, "Max:\t%s\n", r.LatencyMax)
		fmt.Fprintln(w, "")
	}

	fmt.Fprintf(w, "Errors:\t%d\n", r.Errors)
	fmt.Fprintln(w, "")
	w.Flush()
}

func printTextConsumer(r BenchmarkResult) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "=== Consumer Benchmark Results ===")
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Duration:\t%s\n", r.Duration)
	fmt.Fprintf(w, "Messages Received:\t%s\n", humanize.Comma(r.TotalMessages))
	fmt.Fprintf(w, "Bytes Received:\t%s\n", humanize.Bytes(uint64(r.TotalBytes)))
	fmt.Fprintf(w, "Throughput:\t%s msgs/sec\n", humanize.CommafWithDigits(r.MessagesPerSecond, 2))
	fmt.Fprintf(w, "Bandwidth:\t%.2f MB/sec\n", r.MBPerSecond)
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Errors:\t%d\n", r.Errors)
	fmt.Fprintln(w, "")
	w.Flush()
}
