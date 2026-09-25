-- ANALYTICS layer: stable, consumer-facing views over the SCD2 history.
-- Portable between Snowflake and the local DuckDB warehouse.

CREATE OR REPLACE VIEW analytics.customer_current AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    customer_status,
    valid_from AS current_since
FROM core.customer_history
WHERE is_current
  AND NOT is_deleted;

CREATE OR REPLACE VIEW analytics.customer_version_summary AS
SELECT
    customer_id,
    COUNT(*)                                            AS version_count,
    MIN(valid_from)                                     AS first_seen_at,
    MAX(valid_from)                                     AS last_changed_at,
    MAX(CASE WHEN is_current AND is_deleted THEN 1 ELSE 0 END) = 1 AS is_deleted
FROM core.customer_history
GROUP BY customer_id;
