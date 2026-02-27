package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"mulldb/config"
	"mulldb/executor"
)

// Server accepts TCP connections and spawns a goroutine per client.
type Server struct {
	cfg      *config.Config
	exec     *executor.Executor
	mu       sync.Mutex // protects listener
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// New creates a server with the given configuration and executor.
func New(cfg *config.Config, exec *executor.Executor) *Server {
	return &Server{
		cfg:  cfg,
		exec: exec,
		quit: make(chan struct{}),
	}
}

// ListenAndServe starts accepting connections. It blocks until Shutdown
// is called or an unrecoverable error occurs.
func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	log.Printf("mulldb listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c := newConnection(conn, s.cfg, s.exec)
			c.Handle()
		}()
	}
}

// Addr returns the listener's network address, or nil if not yet listening.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		return ln.Addr()
	}
	return nil
}

// Shutdown stops accepting new connections and waits for existing ones
// to finish, respecting the context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	close(s.quit)
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		ln.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
