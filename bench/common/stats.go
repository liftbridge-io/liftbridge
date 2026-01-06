package common

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Stats tracks benchmark statistics including throughput and latency.
type Stats struct {
	mu        sync.Mutex
	startTime time.Time
	endTime   time.Time

	messagesSent int64
	messagesRecv int64
	bytesSent    int64
	bytesRecv    int64
	errors       int64

	// HDR histogram for latency tracking (in microseconds)
	// Range: 1 microsecond to 60 seconds, 3 significant figures
	latencyHist *hdrhistogram.Histogram
}

// NewStats creates a new Stats instance with HDR histogram initialized.
func NewStats() *Stats {
	return &Stats{
		latencyHist: hdrhistogram.New(1, 60000000, 3),
	}
}

// Start begins the timing period.
func (s *Stats) Start() {
	s.startTime = time.Now()
}

// Stop ends the timing period.
func (s *Stats) Stop() {
	s.endTime = time.Now()
}

// RecordSent records a sent message with its byte size.
func (s *Stats) RecordSent(bytes int) {
	atomic.AddInt64(&s.messagesSent, 1)
	atomic.AddInt64(&s.bytesSent, int64(bytes))
}

// RecordReceived records a received message with its byte size.
func (s *Stats) RecordReceived(bytes int) {
	atomic.AddInt64(&s.messagesRecv, 1)
	atomic.AddInt64(&s.bytesRecv, int64(bytes))
}

// RecordLatency records a latency measurement.
func (s *Stats) RecordLatency(d time.Duration) {
	s.mu.Lock()
	s.latencyHist.RecordValue(d.Microseconds())
	s.mu.Unlock()
}

// RecordError increments the error counter.
func (s *Stats) RecordError() {
	atomic.AddInt64(&s.errors, 1)
}

// Duration returns the total benchmark duration.
func (s *Stats) Duration() time.Duration {
	return s.endTime.Sub(s.startTime)
}

// MessagesSent returns the total messages sent.
func (s *Stats) MessagesSent() int64 {
	return atomic.LoadInt64(&s.messagesSent)
}

// MessagesReceived returns the total messages received.
func (s *Stats) MessagesReceived() int64 {
	return atomic.LoadInt64(&s.messagesRecv)
}

// TotalMessages returns sent + received messages.
func (s *Stats) TotalMessages() int64 {
	return s.MessagesSent() + s.MessagesReceived()
}

// BytesSent returns the total bytes sent.
func (s *Stats) BytesSent() int64 {
	return atomic.LoadInt64(&s.bytesSent)
}

// BytesReceived returns the total bytes received.
func (s *Stats) BytesReceived() int64 {
	return atomic.LoadInt64(&s.bytesRecv)
}

// TotalBytes returns sent + received bytes.
func (s *Stats) TotalBytes() int64 {
	return s.BytesSent() + s.BytesReceived()
}

// Errors returns the total error count.
func (s *Stats) Errors() int64 {
	return atomic.LoadInt64(&s.errors)
}

// MessagesPerSecond calculates the message throughput.
func (s *Stats) MessagesPerSecond() float64 {
	duration := s.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	return float64(s.TotalMessages()) / duration
}

// BytesPerSecond calculates the byte throughput.
func (s *Stats) BytesPerSecond() float64 {
	duration := s.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	return float64(s.TotalBytes()) / duration
}

// MBPerSecond calculates the MB/s throughput.
func (s *Stats) MBPerSecond() float64 {
	return s.BytesPerSecond() / 1024 / 1024
}

// LatencyPercentile returns the latency at a given percentile.
func (s *Stats) LatencyPercentile(p float64) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(s.latencyHist.ValueAtQuantile(p)) * time.Microsecond
}

// LatencyMean returns the mean latency.
func (s *Stats) LatencyMean() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(s.latencyHist.Mean()) * time.Microsecond
}

// LatencyMin returns the minimum latency recorded.
func (s *Stats) LatencyMin() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(s.latencyHist.Min()) * time.Microsecond
}

// LatencyMax returns the maximum latency recorded.
func (s *Stats) LatencyMax() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(s.latencyHist.Max()) * time.Microsecond
}

// LatencyCount returns the number of latency samples recorded.
func (s *Stats) LatencyCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latencyHist.TotalCount()
}
