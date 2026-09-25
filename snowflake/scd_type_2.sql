-- SCD Type 2 merge: RAW.CUSTOMER_CDC_EVENTS -> CORE.CUSTOMER_HISTORY
--
-- This file is intentionally written in the SQL subset shared by Snowflake and
-- DuckDB, so the exact same statements run in Snowflake and in the local
-- DuckDB development warehouse (and therefore in the automated tests).
--
-- Rules
--   * Versions are ordered by the Postgres LSN, not by arrival time, so Kafka
--     redelivery or cross-partition interleaving cannot reorder history.
--   * VALID_FROM / VALID_TO use the source commit timestamp (source.ts_ms),
--     which makes re-runs deterministic ("as of when it happened in the
--     source", not "as of when we loaded it").
--   * An event whose tracked attributes (RECORD_HASH) and deleted-flag equal
--     the previous version is a no-op and does not create a version
--     (e.g. an UPDATE that only touched updated_at).
--   * Events with an LSN <= the LSN of the current version are ignored, so
--     replaying already-applied events is harmless (idempotent).
--   * DELETE closes the current version and inserts a final "tombstone"
--     version (IS_DELETED = TRUE, IS_CURRENT = TRUE) carrying the last known
--     attributes. History is never physically deleted. If the same
--     customer_id is inserted again later, the tombstone is closed normally.
--
-- Snowflake commits implicitly on DDL, so all temporary tables are built first
-- and only the DML is wrapped in an explicit transaction.

-- 1. Freeze the set of staged events this run is responsible for.
CREATE OR REPLACE TEMPORARY TABLE scd_pending AS
SELECT change_key
FROM raw.customer_cdc_events
WHERE processed_at IS NULL;

-- 2. Ordered, de-duplicated change set with no-op events removed.
CREATE OR REPLACE TEMPORARY TABLE scd_changes AS
WITH current_version AS (
    SELECT customer_id, record_hash, is_deleted, source_lsn
    FROM core.customer_history
    WHERE is_current
),
pending AS (
    SELECT e.*
    FROM raw.customer_cdc_events e
    JOIN scd_pending p
      ON p.change_key = e.change_key
    LEFT JOIN current_version c
      ON c.customer_id = e.customer_id
    WHERE c.source_lsn IS NULL
       OR e.source_lsn > c.source_lsn
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.change_key ORDER BY e.loaded_at) = 1
),
sequenced AS (
    SELECT
        p.*,
        LAG(p.record_hash) OVER (PARTITION BY p.customer_id ORDER BY p.source_lsn) AS prev_batch_hash,
        LAG(p.is_delete)   OVER (PARTITION BY p.customer_id ORDER BY p.source_lsn) AS prev_batch_is_delete
    FROM pending p
)
SELECT s.*
FROM sequenced s
LEFT JOIN current_version c
  ON c.customer_id = s.customer_id
WHERE NOT (
        COALESCE(s.prev_batch_hash, c.record_hash) IS NOT NULL
    AND COALESCE(s.prev_batch_hash, c.record_hash) = s.record_hash
    AND COALESCE(s.prev_batch_is_delete, c.is_deleted) = s.is_delete
);

-- 3. One new version per surviving change. Consecutive changes for the same
--    customer inside this batch are chained with LEAD().
CREATE OR REPLACE TEMPORARY TABLE scd_new_versions AS
SELECT
    CAST(customer_id AS VARCHAR) || '-' || CAST(source_lsn AS VARCHAR) AS customer_version_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    customer_status,
    source_updated_at,
    record_hash,
    is_delete AS is_deleted,
    event_ts  AS valid_from,
    LEAD(event_ts) OVER (PARTITION BY customer_id ORDER BY source_lsn) AS valid_to,
    ROW_NUMBER()   OVER (PARTITION BY customer_id ORDER BY source_lsn DESC) = 1 AS is_current,
    source_lsn,
    op AS source_op
FROM scd_changes;

BEGIN;

-- 4. Close the currently-open version of every customer that changed.
UPDATE core.customer_history
SET valid_to   = n.first_valid_from,
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT customer_id, MIN(valid_from) AS first_valid_from
    FROM scd_new_versions
    GROUP BY customer_id
) n
WHERE core.customer_history.customer_id = n.customer_id
  AND core.customer_history.is_current;

-- 5. Insert the new versions.
INSERT INTO core.customer_history (
    customer_version_key, customer_id, first_name, last_name, email, phone,
    address, city, state, customer_status, source_updated_at, record_hash,
    is_deleted, valid_from, valid_to, is_current, source_lsn, source_op,
    created_at
)
SELECT
    customer_version_key, customer_id, first_name, last_name, email, phone,
    address, city, state, customer_status, source_updated_at, record_hash,
    is_deleted, valid_from, valid_to, is_current, source_lsn, source_op,
    CURRENT_TIMESTAMP
FROM scd_new_versions;

-- 6. Mark exactly the frozen set of staged events as processed (including
--    no-ops and already-applied replays, which were intentionally skipped).
UPDATE raw.customer_cdc_events
SET processed_at = CURRENT_TIMESTAMP
WHERE processed_at IS NULL
  AND change_key IN (SELECT change_key FROM scd_pending);

COMMIT;
