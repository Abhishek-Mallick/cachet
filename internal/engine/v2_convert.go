package engine

import (
	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
)

// The session token and consistency level are the two things both protocols describe identically,
// and they are carried through one representation so the two cannot drift into meaning different
// things. Everything else about v2 is served straight from the engine's own row operations.

func levelToV1(l cachetv2.ConsistencyLevel) cachetv1.ConsistencyLevel {
	return cachetv1.ConsistencyLevel(l)
}

func sessionToV1(s *cachetv2.SessionToken) *cachetv1.SessionToken {
	if s == nil {
		return nil
	}
	return &cachetv1.SessionToken{Watermarks: s.GetWatermarks()}
}

func sessionToV2(s *cachetv1.SessionToken) *cachetv2.SessionToken {
	if s == nil {
		return nil
	}
	return &cachetv2.SessionToken{Watermarks: s.GetWatermarks()}
}
