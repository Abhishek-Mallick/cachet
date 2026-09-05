-- Cachet fixture schema.
--
-- The canonical copy lives in test/fixtures/schema/ and is applied here at bootstrap. Every cached
-- table carries `version BIGINT UNSIGNED NOT NULL`, indexed. That column is the integration cost
-- Cachet asks of its users, and it is stated plainly rather than hidden (ADR 0003).

CREATE DATABASE IF NOT EXISTS cachet CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE cachet;

CREATE TABLE IF NOT EXISTS entities (
  -- Globally unique across shards. Shard assignment is by consistent hash of the key, not by id
  -- range, so ids are not contiguous within a shard. Tests rely on that.
  id          BIGINT UNSIGNED NOT NULL,

  -- Gives conditional-write tests a realistic predicate: `UPDATE ... WHERE tenant_id = ?` is the
  -- shape that forces exact affected-key resolution (CONSISTENCY.md §5).
  tenant_id   INT UNSIGNED    NOT NULL,
  status      TINYINT UNSIGNED NOT NULL,

  payload     VARBINARY(1024) NOT NULL,

  -- The HLC version. Maintained by Cachet on every write; never by the application.
  version     BIGINT UNSIGNED NOT NULL,

  updated_at  TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (id),
  -- Flux and Sextant both scan by version; Sextant's detection loop is weighted toward
  -- recently-invalidated keys, which is a version-ordered scan.
  KEY idx_version (version),
  -- Supports the conditional-write predicate and its `SELECT pk ... FOR UPDATE` resolution.
  KEY idx_tenant_status (tenant_id, status)
) ENGINE=ROCKSDB;

-- Seed bookkeeping: lets `make env-status` and the harness answer "which profile is loaded, and was
-- it loaded completely?" without counting ten million rows.
CREATE TABLE IF NOT EXISTS seed_meta (
  shard_id    INT UNSIGNED    NOT NULL,
  profile     VARCHAR(32)     NOT NULL,
  row_count   BIGINT UNSIGNED NOT NULL,
  seed        BIGINT UNSIGNED NOT NULL,
  checksum    CHAR(64)        NOT NULL,
  seeded_at   TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shard_id, profile)
) ENGINE=ROCKSDB;
