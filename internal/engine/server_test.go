package engine_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
)

// stubService answers Handshake and lets a test block inside Get, which is how the drain behaviour
// is observed rather than assumed.
type stubService struct {
	cachetv1.UnimplementedCacheServiceServer

	entered chan struct{}
	release chan struct{}
}

func newStub() *stubService {
	return &stubService{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (s *stubService) Handshake(context.Context, *cachetv1.HandshakeRequest) (*cachetv1.HandshakeResponse, error) {
	return &cachetv1.HandshakeResponse{ProtocolVersion: "cachet.v1", Compatible: true}, nil
}

func (s *stubService) Get(ctx context.Context, _ *cachetv1.GetRequest) (*cachetv1.GetResponse, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return &cachetv1.GetResponse{Found: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// socketPath returns a short-enough Unix socket path.
//
// t.TempDir() on macOS lives under /var/folders/... and, with the test name appended, routinely
// exceeds the kernel's 104-byte sun_path limit — so the helper exists to test the transport rather
// than the platform's path length.
func socketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "cs")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "c.sock")
}

func newServer(t *testing.T, svc cachetv1.CacheServiceServer, listen ...string) *engine.Server {
	t.Helper()

	srv, err := engine.NewServer(context.Background(), svc, engine.ServerOptions{
		Listen:       listen,
		DrainTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func serve(t *testing.T, srv *engine.Server) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Serve: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Error("Serve did not return after cancellation")
			}
		})
	}
}

func dial(t *testing.T, addr net.Addr) cachetv1.CacheServiceClient {
	t.Helper()

	target := "passthrough:///" + addr.String()
	if addr.Network() == "unix" {
		target = "unix://" + addr.String()
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return cachetv1.NewCacheServiceClient(conn)
}

func TestServerAcceptsOverTCP(t *testing.T) {
	t.Parallel()

	srv := newServer(t, newStub(), "tcp://127.0.0.1:0")
	defer serve(t, srv)()

	resp, err := dial(t, srv.Addrs()[0]).Handshake(context.Background(), &cachetv1.HandshakeRequest{})
	if err != nil {
		t.Fatalf("Handshake over tcp: %v", err)
	}
	if !resp.GetCompatible() {
		t.Error("handshake reported incompatible")
	}
}

func TestServerAcceptsOverUnixSocket(t *testing.T) {
	t.Parallel()

	sock := socketPath(t)
	srv := newServer(t, newStub(), "unix://"+sock)
	defer serve(t, srv)()

	// The sidecar is the default topology and reaches the engine over a Unix socket (ADR 0004).
	// If this transport is an afterthought, the default deployment is the untested one.
	resp, err := dial(t, srv.Addrs()[0]).Handshake(context.Background(), &cachetv1.HandshakeRequest{})
	if err != nil {
		t.Fatalf("Handshake over unix: %v", err)
	}
	if !resp.GetCompatible() {
		t.Error("handshake reported incompatible")
	}
}

func TestBothTransportsServeTheSameServer(t *testing.T) {
	t.Parallel()

	sock := socketPath(t)
	srv := newServer(t, newStub(), "tcp://127.0.0.1:0", "unix://"+sock)
	defer serve(t, srv)()

	if got := len(srv.Addrs()); got != 2 {
		t.Fatalf("Addrs() has %d entries, want 2", got)
	}
	// Topology must be a deployment choice, not a product variant — one binary, identical
	// behaviour, whichever socket the request arrived on.
	for _, addr := range srv.Addrs() {
		if _, err := dial(t, addr).Handshake(context.Background(), &cachetv1.HandshakeRequest{}); err != nil {
			t.Errorf("Handshake over %s: %v", addr.Network(), err)
		}
	}
}

func TestShutdownWaitsForInFlightRequests(t *testing.T) {
	t.Parallel()

	stub := newStub()
	srv := newServer(t, stub, "tcp://127.0.0.1:0")

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()

	client := dial(t, srv.Addrs()[0])
	result := make(chan error, 1)
	go func() {
		_, err := client.Get(context.Background(), &cachetv1.GetRequest{Key: "entities:1"})
		result <- err
	}()

	<-stub.entered // the request is inside the handler
	cancel()       // SIGTERM equivalent, mid-request

	select {
	case err := <-served:
		t.Fatalf("Serve returned while a request was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(stub.release)

	// An ack the client never receives, for a write the database already committed, is the worst
	// possible failure for a system whose claim is read-own-writes.
	if err := <-result; err != nil {
		t.Errorf("in-flight request failed during shutdown: %v", err)
	}
	if err := <-served; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Serve: %v", err)
	}
}

func TestServerStopsListeningAfterShutdown(t *testing.T) {
	t.Parallel()

	stub := newStub()
	close(stub.release)
	srv := newServer(t, stub, "tcp://127.0.0.1:0")

	addr := srv.Addrs()[0]
	stop := serve(t, srv)
	stop()

	probe := net.Dialer{Timeout: time.Second}
	conn, err := probe.DialContext(context.Background(), "tcp", addr.String())
	if err == nil {
		_ = conn.Close()
		t.Error("the listener still accepts connections after shutdown")
	}
}

func TestUnixSocketIsRemovedOnShutdown(t *testing.T) {
	t.Parallel()

	sock := socketPath(t)
	stub := newStub()
	close(stub.release)

	srv := newServer(t, stub, "unix://"+sock)
	stop := serve(t, srv)
	stop()

	// A stale socket file left behind makes the next start fail with "address already in use",
	// which in a sidecar means the pod never comes back after a restart.
	second, err := engine.NewServer(context.Background(), stub, engine.ServerOptions{
		Listen:       []string{"unix://" + sock},
		DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("restarting on the same socket path failed: %v", err)
	}
	stopSecond := serve(t, second)
	stopSecond()
}

func TestNewServerRejectsABadListenAddress(t *testing.T) {
	t.Parallel()

	_, err := engine.NewServer(context.Background(), newStub(), engine.ServerOptions{
		Listen:       []string{"http://:8080"},
		DrainTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("NewServer accepted an unsupported listen scheme")
	}
	if !strings.Contains(err.Error(), "http") {
		t.Errorf("error %q does not name the offending address", err)
	}
}

func TestNewServerRequiresAtLeastOneListener(t *testing.T) {
	t.Parallel()

	// Binding happens at construction, not on Serve, so a port conflict is a boot failure rather
	// than a surprise once traffic is already being routed to the process.
	if _, err := engine.NewServer(context.Background(), newStub(), engine.ServerOptions{DrainTimeout: time.Second}); err == nil {
		t.Error("NewServer accepted a config with nowhere to listen")
	}
}

func TestNewServerFailsFastOnAPortConflict(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	held, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = held.Close() }()

	_, err = engine.NewServer(context.Background(), newStub(), engine.ServerOptions{
		Listen:       []string{"tcp://" + held.Addr().String()},
		DrainTimeout: time.Second,
	})
	if err == nil {
		t.Error("NewServer succeeded on an address already in use")
	}
}
