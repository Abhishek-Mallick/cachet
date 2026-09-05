// Package engine is Cachet's query engine: the gRPC server, the read and write paths, and the
// enforcement of consistency levels.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/config"
)

// ServerOptions configures a Server.
type ServerOptions struct {
	// Listen holds addresses in "tcp://host:port" or "unix:///path" form.
	Listen []string

	// DrainTimeout is how long in-flight requests have to finish once shutdown begins, before the
	// server stops them.
	DrainTimeout time.Duration

	Logger *slog.Logger

	// Interceptors are applied in order. Observability is wired in this way so the transport has no
	// opinion about metrics.
	UnaryInterceptors []grpc.UnaryServerInterceptor
}

// Server hosts the Cachet gRPC service on one or more listeners.
//
// One binary, several sockets: the same service is served over TCP and over a Unix domain socket,
// because topology is a deployment choice rather than a product variant (ADR 0004). The sidecar —
// the default topology — reaches the engine over the Unix socket, so making that an afterthought
// would leave the default deployment as the untested one.
type Server struct {
	grpc         *grpc.Server
	listeners    []net.Listener
	drainTimeout time.Duration
	log          *slog.Logger
}

// NewServer binds the configured listeners and prepares the gRPC server.
//
// Binding happens here rather than in Serve so that a bad address or an occupied port is a boot
// failure. Discovering it after traffic has been routed to the process turns an operator problem
// into a customer problem.
func NewServer(ctx context.Context, svc cachetv1.CacheServiceServer, opts ServerOptions) (*Server, error) {
	if svc == nil {
		return nil, errors.New("engine: nil service")
	}
	if len(opts.Listen) == 0 {
		return nil, errors.New("engine: no listen addresses")
	}
	if opts.DrainTimeout <= 0 {
		return nil, fmt.Errorf("engine: drain timeout must be positive, got %s", opts.DrainTimeout)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	listeners := make([]net.Listener, 0, len(opts.Listen))
	closeAll := func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}

	for _, raw := range opts.Listen {
		spec, err := config.ParseListen(raw)
		if err != nil {
			closeAll()
			return nil, err
		}
		if spec.Network == "unix" {
			// A socket file left behind by an unclean exit makes the next start fail with "address
			// already in use". In a sidecar that means the pod never comes back after a crash, so
			// the stale file is cleared rather than inherited.
			if err := removeStaleSocket(ctx, spec.Address); err != nil {
				closeAll()
				return nil, err
			}
		}

		var lc net.ListenConfig
		l, err := lc.Listen(ctx, spec.Network, spec.Address)
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("engine: listen on %s: %w", raw, err)
		}
		listeners = append(listeners, l)
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(opts.UnaryInterceptors...))
	cachetv1.RegisterCacheServiceServer(s, svc)

	return &Server{
		grpc:         s,
		listeners:    listeners,
		drainTimeout: opts.DrainTimeout,
		log:          log,
	}, nil
}

// Addrs returns the addresses actually bound, which is how a caller discovers the port when it
// asked for :0.
func (s *Server) Addrs() []net.Addr {
	out := make([]net.Addr, 0, len(s.listeners))
	for _, l := range s.listeners {
		out = append(out, l.Addr())
	}
	return out
}

// Serve accepts connections until ctx is cancelled, then drains.
//
// Shutdown is graceful by construction: the listeners stop accepting, in-flight requests are given
// DrainTimeout to finish, and only then are the remaining ones stopped. That ordering matters more
// here than in most services — an ack the client never receives, for a write the database has
// already committed, is the worst possible failure for a system whose headline claim is
// read-own-writes.
func (s *Server) Serve(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	for _, l := range s.listeners {
		g.Go(func() error {
			s.log.Info("listening", "network", l.Addr().Network(), "address", l.Addr().String())
			if err := s.grpc.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				return fmt.Errorf("engine: serve %s: %w", l.Addr(), err)
			}
			return nil
		})
	}

	// The shutdown watcher is itself part of the group, so it has an owner and is awaited — no
	// goroutine outlives Serve (CONTRIBUTING.md rule 2).
	g.Go(func() error {
		<-gctx.Done()
		s.shutdown()
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Server) shutdown() {
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.log.Info("drained cleanly")
	case <-time.After(s.drainTimeout):
		// Exceeding the budget is reported rather than silently absorbed: it means requests were
		// cut off, and someone needs to know which deploy started doing that.
		s.log.Warn("drain timeout exceeded; stopping in-flight requests",
			"drain_timeout", s.drainTimeout)
		s.grpc.Stop()
		<-stopped
	}
}

// removeStaleSocket clears a leftover socket file, but only if nothing is listening on it.
func removeStaleSocket(ctx context.Context, path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: stat socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		// Refusing to unlink a regular file is deliberate: a mistyped path must not delete
		// something that was never ours.
		return fmt.Errorf("engine: %s exists and is not a socket", path)
	}

	// If something is still listening, this is a genuine conflict and must not be papered over by
	// stealing the address from a running process.
	probe := net.Dialer{Timeout: 200 * time.Millisecond}
	if conn, err := probe.DialContext(ctx, "unix", path); err == nil {
		_ = conn.Close()
		return fmt.Errorf("engine: %s is already in use by a running process", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("engine: remove stale socket %s: %w", path, err)
	}
	return nil
}
