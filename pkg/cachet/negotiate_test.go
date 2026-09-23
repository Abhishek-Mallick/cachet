package cachet_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
)

// Protocol negotiation, against a server that serves only the original protocol.
//
// This is the case the dual-serve window exists for and the one no live test can produce, because
// every engine this build starts serves both. A stub is the only way to hold the old server still.

// v1OnlyServer answers the v1 handshake and nothing else.
type v1OnlyServer struct {
	cachetv1.UnimplementedCacheServiceServer
	handshakes int
}

func (s *v1OnlyServer) Handshake(context.Context, *cachetv1.HandshakeRequest) (*cachetv1.HandshakeResponse, error) {
	s.handshakes++
	return &cachetv1.HandshakeResponse{ServerVersion: "test", Compatible: true}, nil
}

func startV1Only(t *testing.T) (string, *v1OnlyServer) {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	svc := &v1OnlyServer{}
	s := grpc.NewServer()
	cachetv1.RegisterCacheServiceServer(s, svc)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Serve(l)
	}()
	t.Cleanup(func() {
		s.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the stub server did not stop")
		}
	})
	return l.Addr().String(), svc
}

// TestDialFallsBackToV1AgainstAnOlderServer keeps the SDK upgradeable on its own schedule. An SDK
// that required a server upgrade first would make the two impossible to roll independently, which
// is the whole reason both protocols are served for a release.
func TestDialFallsBackToV1AgainstAnOlderServer(t *testing.T) {
	addr, stub := startV1Only(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cachet.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial against a v1-only server: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if stub.handshakes != 1 {
		t.Errorf("the client performed %d v1 handshakes, want 1", stub.handshakes)
	}

	// The typed API is available; the generic one says why it is not, rather than reading a nil
	// descriptor map and producing an unexplained miss.
	if got := c.Tables(); len(got) != 0 {
		t.Errorf("the client learned %d tables from a v1-only server, want 0", len(got))
	}
	if _, err := c.Table("entities"); !errors.Is(err, cachet.ErrNoDescriptors) {
		t.Errorf("Table: err = %v, want ErrNoDescriptors", err)
	}
	if _, err := c.GetRow(ctx, "entities:1"); !errors.Is(err, cachet.ErrNoDescriptors) {
		t.Errorf("GetRow: err = %v, want ErrNoDescriptors", err)
	}
	if _, _, err := c.BatchGetRows(ctx, []string{"entities:1"}); !errors.Is(err, cachet.ErrNoDescriptors) {
		t.Errorf("BatchGetRows: err = %v, want ErrNoDescriptors", err)
	}
	if _, err := c.PutRow(ctx, "entities:1", cachet.Row{}); !errors.Is(err, cachet.ErrNoDescriptors) {
		t.Errorf("PutRow: err = %v, want ErrNoDescriptors", err)
	}
}
