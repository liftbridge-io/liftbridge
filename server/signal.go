// +build !windows

package server

import (
	"os"
	"os/signal"
	"syscall"
)

// handleSignals sets up a handler for SIGINT to do a graceful shutdown.
func (s *Server) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGINT:
				s.Stop()
				os.Exit(0)
			}
		}
	}()
}
