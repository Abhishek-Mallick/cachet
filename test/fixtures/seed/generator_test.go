package main

import (
	"math"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func TestUnknownProfileIsRejected(t *testing.T) {
	t.Parallel()

	if _, err := ProfileByName("enormous"); err == nil {
		t.Error("ProfileByName(\"enormous\") succeeded; an unknown profile must be a startup error")
	}
}

func TestGenerateProducesTheRequestedRowCount(t *testing.T) {
	t.Parallel()

	p := Profile{Name: "tiny", Rows: 500, Seed: 42, Tenants: 8, PayloadBytes: 64}

	var n uint64
	if err := Generate(p, func(storage.Record) error { n++; return nil }); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if n != p.Rows {
		t.Errorf("Generate yielded %d rows, want %d", n, p.Rows)
	}
}

func TestGenerateIsByteIdenticalAcrossRuns(t *testing.T) {
	t.Parallel()

	p := Profile{Name: "tiny", Rows: 2000, Seed: 42, Tenants: 8, PayloadBytes: 64}

	// A consistency violation must be reproducible from the test name alone (build plan §9.2).
	// That is only true if the data underneath it is identical on every run, so determinism is a
	// hard requirement of the fixture, not a nicety.
	first, second := collect(t, p), collect(t, p)

	if len(first) != len(second) {
		t.Fatalf("run lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.ID != b.ID || a.TenantID != b.TenantID || a.Status != b.Status || string(a.Payload) != string(b.Payload) {
			t.Fatalf("row %d differs between runs:\n  %+v\n  %+v", i, a, b)
		}
	}
	if Checksum(first) != Checksum(second) {
		t.Errorf("checksums differ: %s vs %s", Checksum(first), Checksum(second))
	}
}

func TestDifferentSeedsProduceDifferentData(t *testing.T) {
	t.Parallel()

	a := collect(t, Profile{Name: "a", Rows: 500, Seed: 1, Tenants: 8, PayloadBytes: 32})
	b := collect(t, Profile{Name: "b", Rows: 500, Seed: 2, Tenants: 8, PayloadBytes: 32})

	if Checksum(a) == Checksum(b) {
		t.Error("two different seeds produced identical data; the seed is not reaching the generator")
	}
}

func TestPayloadMatchesTheProfileSize(t *testing.T) {
	t.Parallel()

	p := Profile{Name: "tiny", Rows: 100, Seed: 7, Tenants: 4, PayloadBytes: 128}

	for _, rec := range collect(t, p) {
		if len(rec.Payload) != p.PayloadBytes {
			t.Fatalf("payload is %d bytes, want %d — row size drives the benchmark's memory numbers",
				len(rec.Payload), p.PayloadBytes)
		}
	}
}

func TestTenantIDsStayWithinTheProfileRange(t *testing.T) {
	t.Parallel()

	p := Profile{Name: "tiny", Rows: 1000, Seed: 7, Tenants: 4, PayloadBytes: 16}

	for _, rec := range collect(t, p) {
		if rec.TenantID >= p.Tenants {
			t.Fatalf("tenant_id %d is outside the profile's range of %d", rec.TenantID, p.Tenants)
		}
	}
}

func TestGenerateStopsOnYieldError(t *testing.T) {
	t.Parallel()

	p := Profile{Name: "tiny", Rows: 1000, Seed: 7, Tenants: 4, PayloadBytes: 16}

	// The loader batches inserts; a failing batch must abort the seed rather than leave a shard
	// half-populated and a checksum that claims otherwise.
	sentinel := errStop
	n := 0
	err := Generate(p, func(storage.Record) error {
		n++
		if n == 10 {
			return sentinel
		}
		return nil
	})
	if err == nil {
		t.Fatal("Generate ignored the yield error")
	}
	if n != 10 {
		t.Errorf("Generate produced %d rows after the yield failed at 10", n)
	}
}

func TestRowsSpreadEvenlyAcrossShards(t *testing.T) {
	t.Parallel()

	router, err := storage.NewRouter([]storage.ShardID{"shard0", "shard1", "shard2"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	p := Profile{Name: "tiny", Rows: 30_000, Seed: 42, Tenants: 8, PayloadBytes: 16}
	counts := map[storage.ShardID]int{}
	for _, rec := range collect(t, p) {
		id, err := router.ShardFor(CacheKey(rec.ID))
		if err != nil {
			t.Fatalf("ShardFor: %v", err)
		}
		counts[id]++
	}

	// An uneven seed makes one shard the bottleneck in every benchmark, which would misattribute a
	// routing problem to the storage engine.
	expected := float64(p.Rows) / 3
	for _, id := range router.Shards() {
		dev := math.Abs(float64(counts[id])-expected) / expected
		if dev > 0.05 {
			t.Errorf("shard %s holds %d rows, expected ~%.0f (deviation %.1f%%, budget 5%%)",
				id, counts[id], expected, dev*100)
		}
	}
}

func collect(t *testing.T, p Profile) []storage.Record {
	t.Helper()

	var out []storage.Record
	if err := Generate(p, func(r storage.Record) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return out
}
