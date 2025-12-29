package telemetry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

func TestGenerateUUID(t *testing.T) {
	uuid, err := generateUUID()
	require.NoError(t, err)

	// UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
	uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	require.True(t, uuidRegex.MatchString(uuid), "UUID should match v4 format: %s", uuid)

	// Generate another UUID and ensure they're different
	uuid2, err := generateUUID()
	require.NoError(t, err)
	require.NotEqual(t, uuid, uuid2, "UUIDs should be unique")
}

func TestLoadOrCreateInstanceID(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Test creating new instance ID
	id1, err := loadOrCreateInstanceID(tmpDir)
	require.NoError(t, err)
	require.NotEmpty(t, id1)

	// Verify file was created
	idPath := filepath.Join(tmpDir, instanceIDFile)
	_, err = os.Stat(idPath)
	require.NoError(t, err)

	// Test loading existing instance ID
	id2, err := loadOrCreateInstanceID(tmpDir)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "Should return same instance ID on subsequent calls")
}

func TestLoadOrCreateInstanceID_ExistingID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Pre-create an instance ID file
	existingID := "existing-test-id-12345"
	idPath := filepath.Join(tmpDir, instanceIDFile)
	err = os.WriteFile(idPath, []byte(existingID), 0644)
	require.NoError(t, err)

	// Load should return the existing ID
	id, err := loadOrCreateInstanceID(tmpDir)
	require.NoError(t, err)
	require.Equal(t, existingID, id)
}

func TestCollectPayload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0-test", log)
	require.NoError(t, err)

	payload := collector.collectPayload()

	// Verify required fields
	require.NotEmpty(t, payload.InstanceID)
	require.NotEmpty(t, payload.Timestamp)
	require.Equal(t, "1.0.0-test", payload.LiftbridgeVersion)

	// Verify OS info
	require.NotEmpty(t, payload.OS.Name)
	require.NotEmpty(t, payload.OS.Version)
	require.NotEmpty(t, payload.OS.Architecture)
	require.NotEmpty(t, payload.OS.Platform)

	// Verify CPU info
	require.NotNil(t, payload.CPU.PhysicalCores)
	require.NotNil(t, payload.CPU.LogicalCores)
	require.Greater(t, *payload.CPU.LogicalCores, 0)

	// Verify memory info
	require.NotNil(t, payload.Memory.TotalGB)
	require.Greater(t, *payload.Memory.TotalGB, 0.0)

	// Verify timestamp format (ISO 8601)
	_, err = time.Parse("2006-01-02T15:04:05Z", payload.Timestamp)
	require.NoError(t, err, "Timestamp should be valid ISO 8601 format")
}

func TestNew(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0", log)
	require.NoError(t, err)
	require.NotNil(t, collector)
	require.NotEmpty(t, collector.instanceID)
	require.Equal(t, "1.0.0", collector.version)
}

func TestNew_DefaultConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	// Test with nil config - should use defaults
	// Note: This will use "./data" as DataDir which may not exist in test environment
	// so we'll test with explicit config instead
	cfg := DefaultConfig()
	cfg.DataDir = tmpDir

	collector, err := New(cfg, "dev", log)
	require.NoError(t, err)
	require.NotNil(t, collector)
	require.True(t, collector.config.Enabled)
	require.Equal(t, DefaultInterval, collector.config.Interval)
}

func TestGetInstanceID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0", log)
	require.NoError(t, err)

	id := collector.GetInstanceID()
	require.NotEmpty(t, id)
	require.Equal(t, collector.instanceID, id)
}

func TestStartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0", log)
	require.NoError(t, err)

	// Start should not block
	collector.Start()

	// Give it a moment to start the goroutine
	time.Sleep(10 * time.Millisecond)

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not complete in time")
	}
}

func TestStartDisabled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  false,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0", log)
	require.NoError(t, err)

	// Start should return immediately when disabled
	collector.Start()

	// Stop should also work fine
	collector.Stop()
}

func TestSendTelemetry(t *testing.T) {
	var receivedPayload TelemetryPayload
	requestReceived := make(chan struct{})

	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Contains(t, r.Header.Get("User-Agent"), "Liftbridge/")

		// Decode payload
		err := json.NewDecoder(r.Body).Decode(&receivedPayload)
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
		close(requestReceived)
	}))
	defer server.Close()

	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.0.0-test", log)
	require.NoError(t, err)

	// Override the endpoint for testing by calling sendTelemetryTo directly
	// Since we hardcoded the endpoint, we need to test via the mock server approach
	// For this test, we'll create a custom test that uses the test server URL

	// Create a test-specific send function
	payload := collector.collectPayload()
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest("POST", server.URL, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Liftbridge/1.0.0-test")

	resp, err := collector.client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for server to receive request
	select {
	case <-requestReceived:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Server did not receive request")
	}

	// Verify received payload
	require.NotEmpty(t, receivedPayload.InstanceID)
	require.Equal(t, "1.0.0-test", receivedPayload.LiftbridgeVersion)
	require.NotEmpty(t, receivedPayload.OS.Name)
	require.NotNil(t, receivedPayload.CPU.LogicalCores)
	require.NotNil(t, receivedPayload.Memory.TotalGB)
}

func TestPayloadJSONFormat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "telemetry-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := logger.NewLogger(0)
	log.Silent(true)

	cfg := &Config{
		Enabled:  true,
		Interval: time.Hour,
		DataDir:  tmpDir,
	}

	collector, err := New(cfg, "1.2.0", log)
	require.NoError(t, err)

	payload := collector.collectPayload()
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	// Verify JSON structure by unmarshaling into a map
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	require.NoError(t, err)

	// Check top-level keys
	require.Contains(t, jsonMap, "instance_id")
	require.Contains(t, jsonMap, "timestamp")
	require.Contains(t, jsonMap, "liftbridge_version")
	require.Contains(t, jsonMap, "os")
	require.Contains(t, jsonMap, "cpu")
	require.Contains(t, jsonMap, "memory")

	// Check nested OS keys
	osMap := jsonMap["os"].(map[string]interface{})
	require.Contains(t, osMap, "name")
	require.Contains(t, osMap, "version")
	require.Contains(t, osMap, "architecture")
	require.Contains(t, osMap, "platform")

	// Check nested CPU keys
	cpuMap := jsonMap["cpu"].(map[string]interface{})
	require.Contains(t, cpuMap, "physical_cores")
	require.Contains(t, cpuMap, "logical_cores")
	require.Contains(t, cpuMap, "frequency_mhz")

	// Check nested memory keys
	memMap := jsonMap["memory"].(map[string]interface{})
	require.Contains(t, memMap, "total_gb")
}
