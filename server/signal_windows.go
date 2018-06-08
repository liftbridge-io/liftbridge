package server

import (
	"os"
	"os/signal"
)

// Signal Handling
func (s *StanServer) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		s.Stop()
		os.Exit(0)
	}()
}
