package server

import (
	"os"
	"os/signal"
)

// handleSignals sets up a handler for interrupts to do a graceful shutdown.
func (s *Server) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		s.Stop()
		os.Exit(0)
	}()
}
