package logger

import (
	"io"

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
	Writer() io.Writer
	SetWriter(io.Writer)
}

type logger struct {
	*log.Logger
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
	return &logger{l}
}

func (l *logger) Writer() io.Writer {
	return l.Out
}

func (l *logger) SetWriter(writer io.Writer) {
	l.Out = writer
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
