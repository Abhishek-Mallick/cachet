package consistency_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

func TestLevelRoundTripsThroughProto(t *testing.T) {
	t.Parallel()

	for _, lv := range []consistency.Level{
		consistency.Strong, consistency.Session, consistency.Bounded, consistency.Eventual,
	} {
		got, err := consistency.LevelFromProto(lv.Proto())
		if err != nil {
			t.Errorf("LevelFromProto(%v): %v", lv, err)
			continue
		}
		if got != lv {
			t.Errorf("round trip of %v produced %v", lv, got)
		}
	}
}

func TestUnspecifiedProtoLevelMeansSession(t *testing.T) {
	t.Parallel()

	// A client that forgets to set the field must get the documented default — read-own-writes —
	// not the weakest available guarantee. Defaulting downward would make the safe path the one you
	// have to remember.
	got, err := consistency.LevelFromProto(cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED)
	if err != nil {
		t.Fatalf("LevelFromProto(UNSPECIFIED): %v", err)
	}
	if got != consistency.Session {
		t.Errorf("UNSPECIFIED mapped to %v, want Session", got)
	}
}

func TestUnknownProtoLevelIsRejected(t *testing.T) {
	t.Parallel()

	// A newer client asking for a level this server does not implement must be told so, not quietly
	// served something else.
	if _, err := consistency.LevelFromProto(cachetv1.ConsistencyLevel(99)); err == nil {
		t.Error("LevelFromProto accepted an unknown enum value")
	}
}

func TestRequirementFromProtoCarriesTheBound(t *testing.T) {
	t.Parallel()

	req, err := consistency.RequirementFromProto(
		cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED,
		durationpb.New(5*time.Second),
	)
	if err != nil {
		t.Fatalf("RequirementFromProto: %v", err)
	}
	if req.Level != consistency.Bounded {
		t.Errorf("Level = %v, want Bounded", req.Level)
	}
	if req.StalenessBound != 5*time.Second {
		t.Errorf("StalenessBound = %s, want 5s", req.StalenessBound)
	}
}

func TestRequirementFromProtoValidatesOnTheWay(t *testing.T) {
	t.Parallel()

	// Validation belongs at the wire boundary. A malformed request must be rejected before it can
	// reach the read path and be answered by guessing which half the caller meant.
	if _, err := consistency.RequirementFromProto(
		cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED, nil,
	); err == nil {
		t.Error("RequirementFromProto accepted BOUNDED with no staleness bound")
	}
	if _, err := consistency.RequirementFromProto(
		cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION, durationpb.New(time.Second),
	); err == nil {
		t.Error("RequirementFromProto accepted SESSION with a staleness bound")
	}
}

func TestTokenRoundTripsThroughProto(t *testing.T) {
	t.Parallel()

	tok := consistency.NewToken(8)
	tok.Advance("shard0", 100)
	tok.Advance("shard1", 200)

	restored := consistency.TokenFromProto(tok.Proto(), 8)

	for _, tc := range []struct {
		shard string
		want  uint64
	}{{"shard0", 100}, {"shard1", 200}} {
		got, known := restored.Watermark(tc.shard)
		if !known {
			t.Errorf("%s missing after round trip", tc.shard)
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %d after round trip, want %d", tc.shard, got, tc.want)
		}
	}
}

func TestTokenFromNilProtoIsAnEmptySession(t *testing.T) {
	t.Parallel()

	// A first request legitimately carries no token. That is a new session, not an error.
	tok := consistency.TokenFromProto(nil, 8)
	if tok == nil {
		t.Fatal("TokenFromProto(nil) returned nil; callers must not have to nil-check")
	}
	if len(tok.Watermarks()) != 0 {
		t.Errorf("TokenFromProto(nil) produced %d watermarks, want 0", len(tok.Watermarks()))
	}
}

func TestNilTokenProtoIsUsable(t *testing.T) {
	t.Parallel()

	var tok *consistency.Token
	if got := tok.Proto(); got == nil || len(got.GetWatermarks()) != 0 {
		t.Errorf("(*Token)(nil).Proto() = %v, want an empty token", got)
	}
}
