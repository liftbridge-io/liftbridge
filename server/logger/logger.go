package logger

import (
	"io"

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
