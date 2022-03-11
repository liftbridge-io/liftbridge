package logger

import (
	"io"
	"io/ioutil"
	"sync"

	gnatsd "github.com/nats-io/nats-server/v2/server"
	log "github.com/sirupsen/logrus"
)

// Logger interface is used to allow tests to inject custom loggers.
type Logger interface {
	Fatalf(string, ...interface{})
	Debugf(string, ...interface{})
	Errorf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Debug(...interface{})
	Warn(...interface{})
	Info(...interface{})
	Fatal(...interface{})
	Silent(bool)
	Prefix(string)
}

type logger struct {
	*log.Logger
	oldOut io.Writer
	prefix string
	mu     sync.RWMutex
}

// NewLogger returns a new Logger instance backed by Logrus.
func NewLogger(level uint32) Logger {
	l := log.New()
	l.SetLevel(log.Level(level))
	logFormatter := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	}
	l.Formatter = logFormatter
	return &logger{Logger: l}
}

// Fatalf logs a fatal error.
func (l *logger) Fatalf(format string, v ...interface{}) {
	l.Logger.Fatalf(l.prefixFormat(format), v...)
}

// Debugf logs a debug statement.
func (l *logger) Debugf(format string, v ...interface{}) {
	l.Logger.Debugf(l.prefixFormat(format), v...)
}

// Errorf logs an error statement.
func (l *logger) Errorf(format string, v ...interface{}) {
	l.Logger.Errorf(l.prefixFormat(format), v...)
}

// Infof logs an info statement.
func (l *logger) Infof(format string, v ...interface{}) {
	l.Logger.Infof(l.prefixFormat(format), v...)
}

// Warnf logs an warn statement.
func (l *logger) Warnf(format string, v ...interface{}) {
	l.Logger.Warnf(l.prefixFormat(format), v...)
}

// Debug logs a debug statement.
func (l *logger) Debug(v ...interface{}) {
	l.Logger.Debug(l.prefixVars(v)...)
}

// Warn logs a warn statement.
func (l *logger) Warn(v ...interface{}) {
	l.Logger.Warn(l.prefixVars(v)...)
}

// Info logs an info statement.
func (l *logger) Info(v ...interface{}) {
	l.Logger.Info(l.prefixVars(v)...)
}

// Fatal logs a fatal error.
func (l *logger) Fatal(v ...interface{}) {
	l.Logger.Fatal(l.prefixVars(v)...)
}

// Silent is used to enable and disable log silencing. Silent must be called
// with true before it can be called with false.
func (l *logger) Silent(enable bool) {
	if enable {
		l.oldOut = l.Out
		l.Out = ioutil.Discard
	} else {
		oldOut := l.oldOut
		if oldOut == nil {
			panic("Must enable logger.Silent before disabling")
		}
		l.Out = oldOut
	}
}

// Prefix all log output with the given string. Pass an empty string to clear
// any previously set prefix.
func (l *logger) Prefix(prefix string) {
	l.mu.Lock()
	l.prefix = prefix
	l.mu.Unlock()
}

func (l *logger) prefixFormat(format string) string {
	l.mu.RLock()
	format = l.prefix + format
	l.mu.RUnlock()
	return format
}

func (l *logger) prefixVars(v []interface{}) []interface{} {
	l.mu.RLock()
	prefix := l.prefix
	l.mu.RUnlock()
	if prefix == "" {
		return v
	}
	return append([]interface{}{prefix}, v...)
}

// natsLogger implements the NATS server logger interface by writing log
// messages to a Liftbridge logger.
type natsLogger struct {
	logger Logger
}

// NewNATSLogger creates a NATS logger that writes log messages to the given
// Logger.
func NewNATSLogger(logger Logger, enabled bool) gnatsd.Logger {
	if enabled {
		return &natsLogger{logger}
	}
	return &noopNATSLogger{logger}
}

// Noticef logs a notice statement.
func (n *natsLogger) Noticef(format string, v ...interface{}) {
	n.logger.Infof("nats: "+format, v...)
}

// Warnf logs a warning statement.
func (n *natsLogger) Warnf(format string, v ...interface{}) {
	n.logger.Warnf("nats: "+format, v...)
}

// Fatalf logs a fatal error.
func (n *natsLogger) Fatalf(format string, v ...interface{}) {
	n.logger.Fatalf("nats: "+format, v...)
}

// Errorf logs an error.
func (n *natsLogger) Errorf(format string, v ...interface{}) {
	n.logger.Errorf("nats: "+format, v...)
}

// Debugf logs a debug statement.
func (n *natsLogger) Debugf(format string, v ...interface{}) {
	n.logger.Debugf("nats: "+format, v...)
}

// Tracef logs a trace statement.
func (n *natsLogger) Tracef(format string, v ...interface{}) {
	n.logger.Debugf("nats: "+format, v...)
}

// noopNATSLogger implements the NATS server logger interface by performing a
// no-op for each log statement with the exception of Fatalf.
type noopNATSLogger struct {
	logger Logger
}

// Noticef is a no-op.
func (n *noopNATSLogger) Noticef(format string, v ...interface{}) {
	// No-op
}

// Warnf is a no-op.
func (n *noopNATSLogger) Warnf(format string, v ...interface{}) {
	// No-op
}

// Fatalf logs a fatal error.
func (n *noopNATSLogger) Fatalf(format string, v ...interface{}) {
	n.logger.Fatalf("nats: "+format, v...)
}

// Errorf is a no-op.
func (n *noopNATSLogger) Errorf(format string, v ...interface{}) {
	// No-op
}

// Debugf is a no-op.
func (n *noopNATSLogger) Debugf(format string, v ...interface{}) {
	// No-op
}

// Tracef is a no-op.
func (n *noopNATSLogger) Tracef(format string, v ...interface{}) {
	// No-op
}
