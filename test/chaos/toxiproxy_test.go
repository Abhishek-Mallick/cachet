//go:build chaos

package chaos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// A Toxiproxy control client, written here rather than pulled in as a dependency.
//
// The API is four verbs and the suite uses all of them; a module would be more code to audit than
// the thing it wraps, and this is test-only code injecting faults into a test-only container.

const toxiproxyAPI = "http://127.0.0.1:8474"

// proxy is one dependency the engine reaches through Toxiproxy: the engine dials Listen, and
// Toxiproxy forwards to Upstream. A toxic between them is the fault.
type proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

// The proxies the chaos suite drives. Ports match test/env/compose.chaos.yml; upstreams are
// container names because Toxiproxy resolves them on the compose network, not on the host.
var proxies = []proxy{
	{Name: "shard0", Listen: "0.0.0.0:23306", Upstream: "shard0:3306", Enabled: true},
	{Name: "shard1", Listen: "0.0.0.0:23307", Upstream: "shard1:3306", Enabled: true},
	{Name: "shard2", Listen: "0.0.0.0:23308", Upstream: "shard2:3306", Enabled: true},
	{Name: "cache", Listen: "0.0.0.0:26379", Upstream: "cache:6379", Enabled: true},
}

func toxiproxyDo(ctx context.Context, t *testing.T, method, path string, body, out any) error {
	t.Helper()

	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(b)
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, toxiproxyAPI+path, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, raw)
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// resetToxiproxy removes every toxic and re-enables every proxy.
//
// Called before each fault rather than only after, because a test that crashed mid-fault would
// otherwise poison every test that follows it — and the resulting failure would point at the
// innocent test rather than the guilty one.
func resetToxiproxy(ctx context.Context, t *testing.T) {
	t.Helper()

	for _, p := range proxies {
		if err := toxiproxyDo(ctx, t, "POST", "/proxies/"+p.Name, map[string]any{"enabled": true}, nil); err != nil {
			t.Fatalf("re-enable proxy %s: %v", p.Name, err)
		}
		var toxics []struct {
			Name string `json:"name"`
		}
		if err := toxiproxyDo(ctx, t, "GET", "/proxies/"+p.Name+"/toxics", nil, &toxics); err != nil {
			t.Fatalf("list toxics on %s: %v", p.Name, err)
		}
		for _, tox := range toxics {
			if err := toxiproxyDo(ctx, t, "DELETE", "/proxies/"+p.Name+"/toxics/"+tox.Name, nil, nil); err != nil {
				t.Fatalf("remove toxic %s from %s: %v", tox.Name, p.Name, err)
			}
		}
	}
}

// addToxic installs a toxic and returns a function that removes it.
func addToxic(ctx context.Context, t *testing.T, proxyName, toxicName, typ string, attrs map[string]any) func() {
	t.Helper()

	body := map[string]any{
		"name":       toxicName,
		"type":       typ,
		"stream":     "downstream",
		"toxicity":   1.0,
		"attributes": attrs,
	}
	if err := toxiproxyDo(ctx, t, "POST", "/proxies/"+proxyName+"/toxics", body, nil); err != nil {
		t.Fatalf("add toxic %s to %s: %v", typ, proxyName, err)
	}
	// A restore runs during cleanup, when the test's context may already be cancelled. It gets its
	// own context so the fault is actually lifted rather than left in place for the next test.
	//nolint:contextcheck // deliberate: inheriting the cancelled context would skip the cleanup.
	return func() {
		_ = toxiproxyDo(context.Background(), t, "DELETE", "/proxies/"+proxyName+"/toxics/"+toxicName, nil, nil)
	}
}

// disableProxy cuts the connection entirely, and returns a function that restores it.
func disableProxy(ctx context.Context, t *testing.T, proxyName string) func() {
	t.Helper()

	if err := toxiproxyDo(ctx, t, "POST", "/proxies/"+proxyName, map[string]any{"enabled": false}, nil); err != nil {
		t.Fatalf("disable proxy %s: %v", proxyName, err)
	}
	//nolint:contextcheck // deliberate: a proxy left disabled would fail every later test.
	return func() {
		_ = toxiproxyDo(context.Background(), t, "POST", "/proxies/"+proxyName, map[string]any{"enabled": true}, nil)
	}
}
