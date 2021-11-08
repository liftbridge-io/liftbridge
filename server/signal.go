package server

import (
	"os"
	"os/signal"
)

// handleSignals sets up a handler for SIGINT to do a graceful shutdown.
func (s *Server) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			switch sig {
			case os.Interrupt:
				if err := s.Stop(); err != nil {
					s.logger.Errorf("Error occurred shutting down server while handling interrupt: %v", err)
					os.Exit(1)
				}
				os.Exit(0)
			}
		}
	}()
}
