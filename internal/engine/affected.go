package engine

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// degradedReason is the exact wording CONSISTENCY.md §5 promises a caller will see.
const degradedReason = "predicate exceeded max_affected_keys"

// UpdateWhere applies a conditional write across every shard and invalidates exactly the rows it
// touched.
//
// This is the Tier 1 capability the whole architecture is for. A proxy sitting outside the database
// sees `UPDATE entities SET status=? WHERE tenant_id=?` and can only infer which rows that touched;
// an engine that owns the read path resolves them exactly, inside the transaction, and invalidates
// them before the write is acknowledged (product spec §4).
//
// The predicate is not key-scoped, so it runs on every shard. Each shard resolves and commits
// independently at its own HLC version — versions from different shards are incomparable (ADR 0003),
// which is why the session token is a per-shard map rather than a scalar.
func (e *Engine) UpdateWhere(ctx context.Context, req *cachetv1.UpdateWhereRequest) (*cachetv1.UpdateWhereResponse, error) {
	// The same guard Put carries, for the same reason: converting silently would wrap, a client
	// sending 300 would store 44, and the row would differ from what the caller believes it wrote.
	match, err := statusToUint8(req.GetMatchStatus(), "match_status")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	set, err := statusToUint8(req.GetSetStatus(), "set_status")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	pred := storage.Predicate{TenantID: req.GetTenantId(), MatchStatus: match, SetStatus: set}
	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)

	var (
		matched  uint64
		resolved []string
		degraded bool
		// newest is the highest version any shard stamped. It is reported for logs and debugging
		// only: versions from different shards are incomparable (ADR 0003), so a single scalar
		// cannot describe a write that touched several. The SESSION TOKEN is authoritative — it
		// carries one watermark per shard, which is the only shape that means anything here.
		newest storage.Version
	)

	for _, shardID := range e.router.Shards() {
		shard, ok := e.shards[shardID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "engine: no open shard %s", shardID)
		}

		res, err := shard.UpdateWhere(ctx, pred, e.maxAffectedKeys)
		if err != nil {
			return nil, e.rpcError(ctx, "update where", err)
		}

		if res.Matched > 0 {
			matched += uint64(res.Matched)
		}

		// The watermark advances on every shard the write touched, degraded or not. This is what
		// keeps the WRITER's own read-own-writes guarantee intact when exact invalidation was given
		// up: its later reads reject any entry filled from a database state older than this write,
		// with no invalidation involved at all (CONSISTENCY.md §5).
		if res.Version > 0 {
			token.Advance(string(shardID), uint64(res.Version))
			if res.Version > newest {
				newest = res.Version
			}
		}

		if res.Degraded {
			degraded = true
			continue
		}
		for _, id := range res.AffectedIDs {
			key := Key{Table: entitiesTable, ID: id}.String()
			// After the commit, before the ack — the same ordering Put relies on, and what lets a
			// different session see the change immediately.
			e.invalidate(ctx, key, res.Version)
			resolved = append(resolved, key)
		}
	}

	meta := &cachetv1.WriteMeta{Version: uint64(newest)}
	if degraded {
		meta.Degraded = true
		meta.DegradedReason = degradedReason
		meta.EffectiveStalenessBound = durationpb.New(e.cdcLagBound)

		// Reported empty, never partial — even though the shards that stayed under budget were
		// invalidated exactly and that work is kept. The list is a CONTRACT: complete when degraded
		// is false, absent when it is true. Handing back a partial list would leave the caller
		// unable to distinguish the rows it must treat as BOUNDED(cdc_lag_bound) from the rows
		// already invalidated, which is the one thing it needs this field for.
		resolved = nil
	}

	return &cachetv1.UpdateWhereResponse{
		Matched:      matched,
		AffectedKeys: resolved,
		Meta:         meta,
		Session:      token.Proto(),
	}, nil
}

// statusToUint8 narrows a proto status field, rejecting anything that would wrap.
func statusToUint8(v uint32, field string) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("engine: %s %d does not fit in a uint8", field, v)
	}
	return uint8(v), nil
}
