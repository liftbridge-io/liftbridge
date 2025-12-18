package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// mockFuture implements raft.Future for testing.
type mockFuture struct {
	err      error
	errDelay time.Duration
}

func (m *mockFuture) Error() error {
	if m.errDelay > 0 {
		time.Sleep(m.errDelay)
	}
	return m.err
}

// mockApplyFuture implements raft.ApplyFuture for testing.
type mockApplyFuture struct {
	mockFuture
	response interface{}
	index    uint64
}

func (m *mockApplyFuture) Response() interface{} {
	return m.response
}

func (m *mockApplyFuture) Index() uint64 {
	return m.index
}

func TestTimeoutFuture_ErrorSuccess(t *testing.T) {
	wrapped := &mockFuture{err: nil}
	// Create future with deadline set right before calling Error() to avoid
	// deadline expiring during slow test execution (e.g., with race detector)
	future := newTimeoutFuture(time.Now().Add(5*time.Minute), wrapped)

	err := future.Error()
	require.NoError(t, err)

	// Calling Error() again should return cached result
	err = future.Error()
	require.NoError(t, err)
}

func TestTimeoutFuture_ErrorFailure(t *testing.T) {
	expectedErr := errors.New("test error")
	wrapped := &mockFuture{err: expectedErr}
	// Create future with deadline set right before calling Error() to avoid
	// deadline expiring during slow test execution (e.g., with race detector)
	future := newTimeoutFuture(time.Now().Add(5*time.Minute), wrapped)

	err := future.Error()
	require.Error(t, err)
	require.Equal(t, expectedErr, err)

	// Calling Error() again should return cached result
	err = future.Error()
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
}

func TestTimeoutFuture_Timeout(t *testing.T) {
	// Create a future that takes longer than the deadline
	wrapped := &mockFuture{err: nil, errDelay: 500 * time.Millisecond}
	future := newTimeoutFuture(time.Now().Add(50*time.Millisecond), wrapped)

	err := future.Error()
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out")
}

func TestTimeoutFuture_ResponseWithApplyFuture(t *testing.T) {
	wrapped := &mockApplyFuture{
		mockFuture: mockFuture{err: nil},
		response:   "test response",
		index:      42,
	}
	future := newTimeoutFuture(time.Now().Add(time.Second), wrapped)

	require.Equal(t, "test response", future.Response())
	require.Equal(t, uint64(42), future.Index())
}

func TestTimeoutFuture_ResponseWithNonApplyFuture(t *testing.T) {
	wrapped := &mockFuture{err: nil}
	future := newTimeoutFuture(time.Now().Add(time.Second), wrapped)

	// Should return nil for non-ApplyFuture
	require.Nil(t, future.Response())
	require.Equal(t, uint64(0), future.Index())
}

func TestRaftNode_IsLeader(t *testing.T) {
	node := &raftNode{}

	// Initially not leader
	require.False(t, node.isLeader())

	// Set as leader
	node.setLeader(true)
	require.True(t, node.isLeader())

	// Set as follower
	node.setLeader(false)
	require.False(t, node.isLeader())
}

func TestComputeDeadline_WithContext(t *testing.T) {
	expectedDeadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), expectedDeadline)
	defer cancel()

	deadline := computeDeadline(ctx)
	require.Equal(t, expectedDeadline, deadline)
}

func TestComputeDeadline_WithoutContext(t *testing.T) {
	before := time.Now()
	deadline := computeDeadline(context.Background())
	after := time.Now()

	// Should be approximately defaultRaftApplyTimeout from now
	require.True(t, deadline.After(before.Add(defaultRaftApplyTimeout-time.Millisecond)))
	require.True(t, deadline.Before(after.Add(defaultRaftApplyTimeout+time.Millisecond)))
}

func TestRaftLogger_Write(t *testing.T) {
	// Create a minimal server with logger
	config := getTestConfig("test", true, 0)
	config.LogRaft = true
	s := &Server{config: config, logger: &testLogger{t: t}}

	logger := &raftLogger{s}

	// Test different log levels
	testCases := []struct {
		input    string
		expected string
	}{
		{"[DEBUG] test debug message", "test debug message"},
		{"[INFO] test info message", "test info message"},
		{"[WARN] test warn message", "test warn message"},
		{"[ERR] test error message", "test error message"},
		{"no level prefix message", "no level prefix message"},
	}

	for _, tc := range testCases {
		n, err := logger.Write([]byte(tc.input))
		require.NoError(t, err)
		require.Equal(t, len(tc.input), n)
	}

	// Test Close is no-op
	require.NoError(t, logger.Close())
}

func TestRaftLogger_WriteDisabled(t *testing.T) {
	// Create a server with LogRaft disabled
	config := getTestConfig("test", true, 0)
	config.LogRaft = false
	s := &Server{config: config, logger: &testLogger{t: t}}

	logger := &raftLogger{s}

	n, err := logger.Write([]byte("[DEBUG] this should not log"))
	require.NoError(t, err)
	require.Equal(t, len("[DEBUG] this should not log"), n)
}

// testLogger implements the logger interface for testing
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debugf(format string, v ...interface{}) {}
func (l *testLogger) Infof(format string, v ...interface{})  {}
func (l *testLogger) Warnf(format string, v ...interface{})  {}
func (l *testLogger) Errorf(format string, v ...interface{}) {}
func (l *testLogger) Fatalf(format string, v ...interface{}) {}
func (l *testLogger) Debug(v ...interface{})                 {}
func (l *testLogger) Info(v ...interface{})                  {}
func (l *testLogger) Warn(v ...interface{})                  {}
func (l *testLogger) Error(v ...interface{})                 {}
func (l *testLogger) Fatal(v ...interface{})                 {}
func (l *testLogger) Silent(bool)                            {}
func (l *testLogger) Prefix(string)                          {}

// Ensure mockApplyFuture implements raft.ApplyFuture
var _ raft.ApplyFuture = (*mockApplyFuture)(nil)
