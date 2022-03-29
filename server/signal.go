package server

import (
	"os"
	"os/signal"
	"syscall"
)

// handleSignals sets up a handler for SIGINT to do a graceful shutdown.
func (s *Server) handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGHUP)
	// Use a naked goroutine instead of startGoroutine because this stops the
	// server which would cause a deadlock.
	go func() {
		for sig := range c {
			switch sig {
			case os.Interrupt:
				if err := s.Stop(); err != nil {
					s.logger.Errorf("Error occurred shutting down server while handling interrupt: %v", err)
					os.Exit(1)
				}
				os.Exit(0)

			case syscall.SIGHUP:
				// Reload authz permissions from storage
				s.authzEnforcer.authzLock.Lock()
				// LoadPolicy, if fails, will likely throw panic
				// Casbin raise a panic if it fails to load the policy, i.e: policy file is corrupted,
				// Refer to issue: https://github.com/casbin/casbin/issues/640
				if err := s.authzEnforcer.enforcer.LoadPolicy(); err != nil {
					s.logger.Errorf("Error occurred while reloading authorization permissions from storage: %v", err)
					s.authzEnforcer.authzLock.Unlock()
					continue
				}
				s.authzEnforcer.authzLock.Unlock()
				s.logger.Info("Reloaded authorization permissions successfully")

			}
		}
	}()
}
