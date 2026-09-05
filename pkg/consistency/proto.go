package consistency

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
)

// The conversions between the wire enum and the domain type live here rather than in the engine and
// the SDK separately. Two copies of this mapping would be two chances to disagree about what
// UNSPECIFIED means, and the level the caller gets would then depend on which side did the decoding.

var levelToProto = map[Level]cachetv1.ConsistencyLevel{
	Strong:   cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
	Session:  cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
	Bounded:  cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED,
	Eventual: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
}

// Proto returns the wire representation of a level.
func (l Level) Proto() cachetv1.ConsistencyLevel {
	if p, ok := levelToProto[l]; ok {
		return p
	}
	return cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED
}

// LevelFromProto decodes a wire level.
//
// UNSPECIFIED maps to Session, the documented default: a client that forgets to set the field gets
// read-own-writes rather than the weakest available guarantee. Defaulting downward would make the
// safe path the one you have to remember, which is how caches end up serving stale data nobody
// asked for.
//
// An unrecognised value is an error rather than a fallback, because it means a newer client is
// asking for something this server does not implement, and answering it with a different guarantee
// silently is worse than refusing.
func LevelFromProto(p cachetv1.ConsistencyLevel) (Level, error) {
	if p == cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED {
		return Session, nil
	}
	for lv, want := range levelToProto {
		if want == p {
			return lv, nil
		}
	}
	return 0, fmt.Errorf("consistency: unsupported wire level %d", int32(p))
}

// RequirementFromProto decodes and validates a request's consistency requirement.
//
// Validation happens here, at the wire boundary, so a malformed request is rejected before it can
// reach the read path — where the only remaining option would be to guess which half the caller
// meant.
func RequirementFromProto(p cachetv1.ConsistencyLevel, bound *durationpb.Duration) (Requirement, error) {
	lv, err := LevelFromProto(p)
	if err != nil {
		return Requirement{}, err
	}

	req := Requirement{Level: lv}
	if bound != nil {
		if err := bound.CheckValid(); err != nil {
			return Requirement{}, fmt.Errorf("consistency: invalid staleness bound: %w", err)
		}
		req.StalenessBound = bound.AsDuration()
	}
	if err := req.Validate(); err != nil {
		return Requirement{}, err
	}
	return req, nil
}

// Proto returns the wire representation of a session token.
//
// It is nil-safe: a caller holding no token yet should not have to branch before putting one on the
// wire.
func (s *Token) Proto() *cachetv1.SessionToken {
	if s == nil {
		return &cachetv1.SessionToken{Watermarks: map[string]uint64{}}
	}
	return &cachetv1.SessionToken{Watermarks: s.Watermarks()}
}

// TokenFromProto rebuilds a session token from the wire.
//
// A nil token is a new session, not an error: a first request legitimately carries none. It always
// returns a usable Token so that callers never have to nil-check what they were handed.
func TokenFromProto(p *cachetv1.SessionToken, maxShards int) *Token {
	t := NewToken(maxShards)
	t.Merge(p.GetWatermarks())
	return t
}
