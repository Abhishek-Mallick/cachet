package cachet_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/baggage"

	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
)

// A session guarantee is a property of the TOKEN, not of the client, the connection, or the engine
// (CONSISTENCY.md §4). These tests cover the two ways a token travels: explicitly on a context, and
// implicitly through OpenTelemetry baggage so it survives a service hop the application did not
// write any code for.
//
// The SDK carries the token from its first release rather than shipping stateless and adding it
// later. An application that forgets to propagate does not get an error — it silently gets a weaker
// guarantee than the one it asked for, which is the failure mode this whole project exists to
// remove (O-303).

func TestSessionRoundTripsThroughAContext(t *testing.T) {
	t.Parallel()

	want := map[string]uint64{"shard0": 42, "shard1": 99}
	ctx := cachet.ContextWithSession(context.Background(), want)

	got, ok := cachet.SessionFromContext(ctx)
	if !ok {
		t.Fatal("SessionFromContext found nothing on a context that carries a session")
	}
	if len(got) != len(want) {
		t.Fatalf("watermarks = %v, want %v", got, want)
	}
	for shard, v := range want {
		if got[shard] != v {
			t.Errorf("shard %s watermark = %d, want %d", shard, got[shard], v)
		}
	}
}

func TestAContextWithNoSessionReportsSo(t *testing.T) {
	t.Parallel()

	// Distinguishable from an empty session, because they mean different things: "this request is
	// not part of a session" versus "this session has not touched any shard yet".
	if _, ok := cachet.SessionFromContext(context.Background()); ok {
		t.Error("SessionFromContext reported a session on a bare context")
	}
}

func TestSessionFromContextReturnsACopy(t *testing.T) {
	t.Parallel()

	original := map[string]uint64{"shard0": 42}
	ctx := cachet.ContextWithSession(context.Background(), original)

	got, _ := cachet.SessionFromContext(ctx)
	got["shard0"] = 1
	got["shard9"] = 7

	again, _ := cachet.SessionFromContext(ctx)
	if again["shard0"] != 42 {
		t.Errorf("mutating the returned map changed the context's session: shard0 = %d", again["shard0"])
	}
	if _, leaked := again["shard9"]; leaked {
		t.Error("a key added to the returned map appeared in the context's session")
	}
}

func TestSessionSurvivesBaggageRoundTrip(t *testing.T) {
	t.Parallel()

	// The service-hop path. An upstream injects into baggage, the transport carries it, and a
	// downstream extracts it — with no application code in between aware a watermark exists.
	want := map[string]uint64{"shard0": 1234567890123456789, "shard2": 7}

	upstream := cachet.ContextWithSession(context.Background(), want)
	carried, err := cachet.InjectSession(upstream)
	if err != nil {
		t.Fatalf("InjectSession: %v", err)
	}

	// Simulate the wire: only the baggage crosses.
	downstream := baggage.ContextWithBaggage(context.Background(), baggage.FromContext(carried))

	got, ok := cachet.ExtractSession(downstream)
	if !ok {
		t.Fatal("ExtractSession found no session in the baggage")
	}
	for shard, v := range want {
		if got[shard] != v {
			t.Errorf("shard %s watermark = %d, want %d", shard, got[shard], v)
		}
	}
}

func TestALargeWatermarkSurvivesBaggageExactly(t *testing.T) {
	t.Parallel()

	// HLC versions live far above 2^53, where anything that round-trips through a float loses
	// precision. Two adjacent versions comparing equal is the same class of bug the Lua scripts
	// encode versions as strings to avoid, and it would be just as invisible here.
	want := map[string]uint64{"shard0": 18446744073709551615, "shard1": 9007199254740993}

	ctx, err := cachet.InjectSession(cachet.ContextWithSession(context.Background(), want))
	if err != nil {
		t.Fatalf("InjectSession: %v", err)
	}
	got, ok := cachet.ExtractSession(ctx)
	if !ok {
		t.Fatal("ExtractSession found nothing")
	}
	for shard, v := range want {
		if got[shard] != v {
			t.Errorf("shard %s watermark = %d, want %d — precision was lost in transit", shard, got[shard], v)
		}
	}
}

func TestExtractingFromBaggageWithoutASessionReportsSo(t *testing.T) {
	t.Parallel()

	if _, ok := cachet.ExtractSession(context.Background()); ok {
		t.Error("ExtractSession reported a session on a context with no baggage")
	}
}

func TestInjectingAnEmptySessionIsNotAnError(t *testing.T) {
	t.Parallel()

	// A session that has touched no shard is a normal state — every process starts there. Injecting
	// it must not fail, or a caller would have to special-case its own first request.
	ctx, err := cachet.InjectSession(context.Background())
	if err != nil {
		t.Fatalf("InjectSession on a bare context: %v", err)
	}
	if _, ok := cachet.ExtractSession(ctx); ok {
		t.Error("an empty session was injected as though it carried watermarks")
	}
}

func TestMalformedBaggageIsIgnoredRatherThanFatal(t *testing.T) {
	t.Parallel()

	// Baggage is shared with every other instrumentation in the process. Something else writing the
	// key must cost this request its session guarantee, not its success — a failed read is worse
	// than a weaker one, and the weaker one is already reported through degraded.
	member, err := baggage.NewMember(cachet.BaggageKey, "not-a-watermark-map")
	if err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), bag)

	if _, ok := cachet.ExtractSession(ctx); ok {
		t.Error("ExtractSession accepted malformed baggage as a session")
	}
}

func TestMergeSessionsTakesTheHighestPerShard(t *testing.T) {
	t.Parallel()

	// Two watermarks for the same shard are not in conflict: the higher one has seen everything the
	// lower one has. Taking the max is what lets an upstream's guarantee and a local one combine
	// without either being dropped.
	a := map[string]uint64{"shard0": 10, "shard1": 50}
	b := map[string]uint64{"shard0": 30, "shard2": 5}

	got := cachet.MergeSessions(a, b)
	for shard, want := range map[string]uint64{"shard0": 30, "shard1": 50, "shard2": 5} {
		if got[shard] != want {
			t.Errorf("shard %s = %d, want %d", shard, got[shard], want)
		}
	}
	if a["shard0"] != 10 {
		t.Error("MergeSessions mutated its first argument")
	}
}
