package cachet

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/baggage"
)

// BaggageKey is the OpenTelemetry baggage key the session watermark travels under.
//
// Baggage rather than a bespoke header, because baggage is already propagated across service
// boundaries by every OpenTelemetry-instrumented transport. Inventing a header would mean every hop
// in a system needed to be taught about Cachet before the guarantee could survive it, and the hops
// nobody remembered to teach would silently downgrade a caller to EVENTUAL (CONSISTENCY.md §4).
const BaggageKey = "cachet-session"

// sessionKey is the private context key for the in-process session.
type sessionKey struct{}

// ContextWithSession attaches a set of per-shard watermarks to a context.
//
// The watermark is a sparse MAP, not a scalar, because each shard runs its own clock and versions
// from different shards are incomparable (ADR 0003). A scalar would have to be either the maximum —
// which invents a guarantee about shards the session never touched, costing hit rate everywhere —
// or the minimum, which discards the guarantee it was supposed to carry.
func ContextWithSession(ctx context.Context, watermarks map[string]uint64) context.Context {
	return context.WithValue(ctx, sessionKey{}, copySession(watermarks))
}

// SessionFromContext returns the watermarks carried on a context.
//
// The boolean distinguishes "this request is not part of a session" from "this session has not
// touched any shard yet". They are different states and conflating them would make a fresh client
// indistinguishable from an unpropagated hop.
func SessionFromContext(ctx context.Context) (map[string]uint64, bool) {
	w, ok := ctx.Value(sessionKey{}).(map[string]uint64)
	if !ok {
		return nil, false
	}
	// A copy, so a caller mutating what it was handed cannot reach back into the context and change
	// a guarantee held by code it has never seen.
	return copySession(w), true
}

// InjectSession encodes the context's session into OpenTelemetry baggage, ready to cross a service
// boundary.
//
// A context with no session is injected as nothing rather than as an error: every process starts
// without a session, and failing there would make a caller special-case its own first request.
func InjectSession(ctx context.Context) (context.Context, error) {
	w, ok := SessionFromContext(ctx)
	if !ok || len(w) == 0 {
		return ctx, nil
	}

	member, err := baggage.NewMember(BaggageKey, url.QueryEscape(encodeSession(w)))
	if err != nil {
		return ctx, fmt.Errorf("cachet: encode session baggage: %w", err)
	}
	bag, err := baggage.FromContext(ctx).SetMember(member)
	if err != nil {
		return ctx, fmt.Errorf("cachet: set session baggage: %w", err)
	}
	return baggage.ContextWithBaggage(ctx, bag), nil
}

// ExtractSession reads a session out of OpenTelemetry baggage.
//
// Malformed baggage is ignored rather than fatal. Baggage is shared with every other instrumentation
// in a process, so something else writing this key must cost the request its session guarantee, not
// its success: a failed read is worse than a weaker one, and the weaker one is already visible to
// the caller through the degraded flag.
func ExtractSession(ctx context.Context) (map[string]uint64, bool) {
	escaped := baggage.FromContext(ctx).Member(BaggageKey).Value()
	if escaped == "" {
		return nil, false
	}
	raw, err := url.QueryUnescape(escaped)
	if err != nil {
		return nil, false
	}
	w, err := decodeSession(raw)
	if err != nil || len(w) == 0 {
		return nil, false
	}
	return w, true
}

// MergeSessions combines two sets of watermarks, taking the highest version per shard.
//
// Two watermarks for one shard are never in conflict: the higher has observed everything the lower
// has. Taking the maximum is what lets an upstream's guarantee and a local one combine without
// either being silently dropped.
func MergeSessions(a, b map[string]uint64) map[string]uint64 {
	out := copySession(a)
	for shard, v := range b {
		if v > out[shard] {
			out[shard] = v
		}
	}
	return out
}

// encodeSession renders watermarks as "shard:version,shard:version", percent-encoded by the caller.
//
// The encoding is percent-escaped before it enters baggage because W3C baggage reserves the comma
// as its own member separator — an unescaped value is rejected outright, which is how this was
// found. Escaping also means a shard id containing anything unusual travels intact rather than
// corrupting the member list.
//
// Versions are written as decimal STRINGS and never pass through a float. An HLC version is a full
// uint64 and lives far above 2^53, where a float64 silently rounds — two adjacent versions would
// compare equal, which is the same class of bug the Lua scripts encode versions as strings to avoid,
// and it would be just as invisible here.
//
// Shards are sorted so the encoding is deterministic: an identical session must produce an identical
// baggage value, or it becomes impossible to tell a changed session from a re-serialised one in a
// trace.
func encodeSession(w map[string]uint64) string {
	shards := make([]string, 0, len(w))
	for shard := range w {
		shards = append(shards, shard)
	}
	sort.Strings(shards)

	var b strings.Builder
	for i, shard := range shards {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(shard)
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(w[shard], 10))
	}
	return b.String()
}

// decodeSession parses what encodeSession wrote.
func decodeSession(s string) (map[string]uint64, error) {
	out := make(map[string]uint64)
	for _, pair := range strings.Split(s, ",") {
		shard, raw, ok := strings.Cut(pair, ":")
		if !ok || shard == "" {
			return nil, fmt.Errorf("cachet: malformed session baggage %q", s)
		}
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cachet: malformed watermark in session baggage %q: %w", s, err)
		}
		out[shard] = v
	}
	return out, nil
}

func copySession(w map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(w))
	for shard, v := range w {
		out[shard] = v
	}
	return out
}
