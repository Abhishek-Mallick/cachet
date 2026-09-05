package storage

import "context"

// ForceRowVersionForTest writes a row with an arbitrary version, bypassing the HLC.
//
// It lives in an export_test.go file so it is compiled only for tests and never reaches the public
// API — a production type must not carry a method that exists solely to let a test cheat.
//
// Its legitimate uses are simulating a second engine instance whose clock runs ahead, and
// simulating an out-of-band write made directly to MySQL. It must never be used to set up ordinary
// state; use Put for that.
func ForceRowVersionForTest(ctx context.Context, s *Shard, rec Record, v Version) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE tenant_id = VALUES(tenant_id), status = VALUES(status),
		                         payload = VALUES(payload), version = VALUES(version)`,
		rec.ID, rec.TenantID, rec.Status, rec.Payload, uint64(v))
	return err
}
