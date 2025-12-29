package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

const (
	// DefaultEndpoint is the telemetry collection endpoint (hardcoded, not configurable)
	DefaultEndpoint = "https://telemetry.basekick.net/api/v1/liftbridge/telemetry"

	// DefaultInterval is the telemetry reporting interval
	DefaultInterval = 24 * time.Hour

	// instanceIDFile stores the persistent instance ID
	instanceIDFile = ".instance_id"
)

// Config holds telemetry configuration
type Config struct {
	Enabled  bool          // Enable telemetry (default: true)
	Interval time.Duration // Reporting interval (default: 24h)
	DataDir  string        // Directory for instance ID file
}

// DefaultConfig returns the default telemetry configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:  true,
		Interval: DefaultInterval,
		DataDir:  "./data",
	}
}

// Collector collects and sends telemetry data
type Collector struct {
	config     *Config
	instanceID string
	version    string
	startTime  time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	client *http.Client
	logger logger.Logger
}

// TelemetryPayload represents the data sent to the telemetry endpoint
type TelemetryPayload struct {
	InstanceID        string  `json:"instance_id"`
	Timestamp         string  `json:"timestamp"`
	LiftbridgeVersion string  `json:"liftbridge_version"`
	OS                OSInfo  `json:"os"`
	CPU               CPUInfo `json:"cpu"`
	Memory            MemInfo `json:"memory"`
}

// OSInfo contains operating system information
type OSInfo struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	Architecture string `json:"architecture"`
	Platform     string `json:"platform"`
}

// CPUInfo contains CPU information
type CPUInfo struct {
	PhysicalCores *int `json:"physical_cores"`
	LogicalCores  *int `json:"logical_cores"`
	FrequencyMHz  *int `json:"frequency_mhz"`
}

// MemInfo contains memory information
type MemInfo struct {
	TotalGB *float64 `json:"total_gb"`
}

// New creates a new telemetry collector
func New(cfg *Config, version string, log logger.Logger) (*Collector, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// Load or generate instance ID
	instanceID, err := loadOrCreateInstanceID(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance ID: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Collector{
		config:     cfg,
		instanceID: instanceID,
		version:    version,
		startTime:  time.Now(),
		ctx:        ctx,
		cancel:     cancel,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: log,
	}

	return c, nil
}

// Start begins periodic telemetry collection
func (c *Collector) Start() {
	if !c.config.Enabled {
		c.logger.Info("Telemetry is disabled")
		return
	}

	c.logger.Infof("Telemetry collector started [instance_id=%s, interval=%s]",
		c.instanceID, c.config.Interval)

	c.wg.Add(1)
	go c.run()
}

// Stop stops the telemetry collector
func (c *Collector) Stop() {
	c.cancel()
	c.wg.Wait()
	c.logger.Info("Telemetry collector stopped")
}

// GetInstanceID returns the instance ID
func (c *Collector) GetInstanceID() string {
	return c.instanceID
}

func (c *Collector) run() {
	defer c.wg.Done()

	// Send initial telemetry beacon
	c.logger.Info("Sending initial telemetry beacon")
	c.sendTelemetry()

	// Then send periodically
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sendTelemetry()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Collector) sendTelemetry() {
	payload := c.collectPayload()

	data, err := json.Marshal(payload)
	if err != nil {
		c.logger.Errorf("Failed to marshal telemetry payload: %v", err)
		return
	}

	c.logger.Infof("Sending telemetry [instance_id=%s]", c.instanceID)

	req, err := http.NewRequestWithContext(c.ctx, "POST", DefaultEndpoint, bytes.NewReader(data))
	if err != nil {
		c.logger.Errorf("Failed to create telemetry request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("Liftbridge/%s", c.version))

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warnf("Failed to send telemetry: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		c.logger.Warnf("Telemetry endpoint returned error status: %d", resp.StatusCode)
		return
	}

	c.logger.Infof("Telemetry sent successfully [status=%d]", resp.StatusCode)
}

func (c *Collector) collectPayload() *TelemetryPayload {
	// Get CPU info
	numCPU := runtime.NumCPU()

	// Get memory info (total system memory via runtime)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	totalGB := float64(memStats.Sys) / (1024 * 1024 * 1024)

	// Build OS platform string
	platform := fmt.Sprintf("%s-%s-%s", runtime.GOOS, runtime.Version(), runtime.GOARCH)

	return &TelemetryPayload{
		InstanceID:        c.instanceID,
		Timestamp:         time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		LiftbridgeVersion: c.version,
		OS: OSInfo{
			Name:         runtime.GOOS,
			Version:      runtime.Version(),
			Architecture: runtime.GOARCH,
			Platform:     platform,
		},
		CPU: CPUInfo{
			PhysicalCores: &numCPU,
			LogicalCores:  &numCPU,
			FrequencyMHz:  nil, // Not easily available in Go without cgo
		},
		Memory: MemInfo{
			TotalGB: &totalGB,
		},
	}
}

func loadOrCreateInstanceID(dataDir string) (string, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create data directory: %w", err)
	}

	idPath := filepath.Join(dataDir, instanceIDFile)

	// Try to load existing ID
	data, err := os.ReadFile(idPath)
	if err == nil && len(data) > 0 {
		return string(bytes.TrimSpace(data)), nil
	}

	// Generate new ID as UUID format
	id, err := generateUUID()
	if err != nil {
		return "", fmt.Errorf("failed to generate instance ID: %w", err)
	}

	// Save ID
	if err := os.WriteFile(idPath, []byte(id), 0644); err != nil {
		return "", fmt.Errorf("failed to save instance ID: %w", err)
	}

	return id, nil
}

func generateUUID() (string, error) {
	// Generate UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	// Set version (4) and variant bits
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant is 10

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
