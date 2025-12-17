package logger

import (
	"bytes"
	"strings"
	"testing"

	gnatsd "github.com/nats-io/nats-server/v2/server"
	log "github.com/sirupsen/logrus"
)

func TestNewLogger(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel))
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLogger_LogMethods(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel))

	// These should not panic
	l.Debug("test debug")
	l.Info("test info")
	l.Warn("test warn")
	l.Debugf("test %s", "debugf")
	l.Infof("test %s", "infof")
	l.Warnf("test %s", "warnf")
	l.Errorf("test %s", "errorf")
}

func TestLogger_Prefix(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel)).(*logger)

	// Set output to capture logs
	var buf bytes.Buffer
	l.Logger.SetOutput(&buf)

	l.Prefix("[test] ")
	l.Info("message")

	output := buf.String()
	if !strings.Contains(output, "[test]") {
		t.Errorf("expected prefix in output, got: %s", output)
	}

	// Clear prefix
	l.Prefix("")
	buf.Reset()
	l.Info("no prefix")

	output = buf.String()
	if strings.Contains(output, "[test]") {
		t.Errorf("expected no prefix in output, got: %s", output)
	}
}

func TestLogger_Silent(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel)).(*logger)

	// Capture original output
	var buf bytes.Buffer
	l.Logger.SetOutput(&buf)

	// Enable silent mode
	l.Silent(true)
	l.Info("should not appear")

	if buf.Len() > 0 {
		t.Errorf("expected no output in silent mode, got: %s", buf.String())
	}

	// Disable silent mode
	l.Silent(false)
	l.Info("should appear")

	if buf.Len() == 0 {
		t.Error("expected output after disabling silent mode")
	}
}

func TestLogger_SilentPanicsIfNotEnabled(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel)).(*logger)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when disabling Silent without enabling first")
		}
	}()

	l.Silent(false) // Should panic
}

func TestNewNATSLogger_Enabled(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel))
	natsLog := NewNATSLogger(l, true)

	if natsLog == nil {
		t.Fatal("expected non-nil NATS logger")
	}

	// Should be the enabled logger type
	if _, ok := natsLog.(*natsLogger); !ok {
		t.Error("expected enabled NATS logger type")
	}

	// Test all log methods don't panic
	natsLog.Noticef("test %s", "notice")
	natsLog.Warnf("test %s", "warn")
	natsLog.Errorf("test %s", "error")
	natsLog.Debugf("test %s", "debug")
	natsLog.Tracef("test %s", "trace")
}

func TestNewNATSLogger_Disabled(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel))
	natsLog := NewNATSLogger(l, false)

	if natsLog == nil {
		t.Fatal("expected non-nil NATS logger")
	}

	// Should be the noop logger type
	if _, ok := natsLog.(*noopNATSLogger); !ok {
		t.Error("expected noop NATS logger type")
	}

	// Test all log methods don't panic (they're no-ops)
	natsLog.Noticef("test %s", "notice")
	natsLog.Warnf("test %s", "warn")
	natsLog.Errorf("test %s", "error")
	natsLog.Debugf("test %s", "debug")
	natsLog.Tracef("test %s", "trace")
}

func TestNATSLoggerOutput(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel)).(*logger)

	// Capture output
	var buf bytes.Buffer
	l.Logger.SetOutput(&buf)

	natsLog := NewNATSLogger(l, true)

	natsLog.Noticef("test message")

	output := buf.String()
	if !strings.Contains(output, "nats:") {
		t.Errorf("expected 'nats:' prefix in output, got: %s", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("expected 'test message' in output, got: %s", output)
	}
}

func TestNoopNATSLoggerDoesNotLog(t *testing.T) {
	l := NewLogger(uint32(log.DebugLevel)).(*logger)

	// Capture output
	var buf bytes.Buffer
	l.Logger.SetOutput(&buf)

	natsLog := NewNATSLogger(l, false)

	// These should not produce output
	natsLog.Noticef("test")
	natsLog.Warnf("test")
	natsLog.Errorf("test")
	natsLog.Debugf("test")
	natsLog.Tracef("test")

	if buf.Len() > 0 {
		t.Errorf("expected no output from noop logger, got: %s", buf.String())
	}
}

// Ensure interfaces are implemented
var _ Logger = (*logger)(nil)
var _ gnatsd.Logger = (*natsLogger)(nil)
var _ gnatsd.Logger = (*noopNATSLogger)(nil)
