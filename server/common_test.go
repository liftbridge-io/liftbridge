package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

// Used by both testing.B and testing.T so need to use
// a common interface: tLogger
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

type dummyLogger struct {
	sync.Mutex
	msg string
}

func (d *dummyLogger) logf(format string, args ...interface{}) {
	d.Lock()
	d.msg = fmt.Sprintf(format, args...)
	d.Unlock()
}

func (d *dummyLogger) log(args ...interface{}) {
	d.Lock()
	d.msg = fmt.Sprint(args...)
	d.Unlock()
}

func (d *dummyLogger) Infof(format string, args ...interface{})  { d.logf(format, args...) }
func (d *dummyLogger) Debugf(format string, args ...interface{}) { d.logf(format, args...) }
func (d *dummyLogger) Errorf(format string, args ...interface{}) { d.logf(format, args...) }
func (d *dummyLogger) Warnf(format string, args ...interface{})  { d.logf(format, args...) }
func (d *dummyLogger) Fatalf(format string, args ...interface{}) { d.logf(format, args...) }
func (d *dummyLogger) Debug(args ...interface{})                 { d.log(args...) }
func (d *dummyLogger) Warn(args ...interface{})                  { d.log(args...) }
func (d *dummyLogger) Info(args ...interface{})                  { d.log(args...) }
func (d *dummyLogger) Fatal(args ...interface{})                 { d.log(args...) }

type captureFatalLogger struct {
	dummyLogger
	fatal string
}

func (c *captureFatalLogger) Fatalf(format string, args ...interface{}) {
	c.Lock()
	// Normally the server would stop after first fatal error, so capture only
	// one.
	if c.fatal == "" {
		c.fatal = fmt.Sprintf(format, args...)
	}
	c.Unlock()
}

func (c *captureFatalLogger) Silent(enable bool) {
}

func (c *captureFatalLogger) Prefix(prefix string) {
}

func noopLogger() logger.Logger {
	log := logger.NewLogger(0)
    log.Silent(true)
	return log
}
