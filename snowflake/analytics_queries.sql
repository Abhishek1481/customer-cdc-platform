-- Example analytical queries over the SCD Type 2 model.
-- Portable between Snowflake and DuckDB. Each query is preceded by a
-- "-- name:" marker so `cdc-warehouse query <name>` can run it.

-- name: current_customers
-- Current state of every non-deleted customer.
SELECT customer_id, first_name, last_name, email, city, state, customer_status, current_since
FROM analytics.customer_current
ORDER BY customer_id;

-- name: customer_history
-- Full version history for one customer (default: the most-changed customer).
SELECT
    h.customer_id,
    h.customer_status,
    h.email,
    h.city,
    h.state,
    h.is_deleted,
    h.valid_from,
    h.valid_to,
    h.is_current,
    h.source_op
FROM core.customer_history h
WHERE h.customer_id = (
    SELECT customer_id
    FROM analytics.customer_version_summary
    ORDER BY version_count DESC, customer_id
    LIMIT 1
)
ORDER BY h.valid_from, h.source_lsn;

-- name: changed_customers
-- Customers whose tracked attributes changed at least once after creation,
-- with the attributes that differ between their first and current version.
WITH ordered AS (
    SELECT
        customer_id,
        email,
        phone,
        address,
        city,
        state,
        customer_status,
        is_deleted,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY source_lsn)      AS version_asc,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY source_lsn DESC) AS version_desc
    FROM core.customer_history
),
first_last AS (
    SELECT
        f.customer_id,
        f.email AS first_email,  l.email AS latest_email,
        f.city  AS first_city,   l.city  AS latest_city,
        f.customer_status AS first_status, l.customer_status AS latest_status,
        l.is_deleted
    FROM ordered f
    JOIN ordered l
      ON l.customer_id = f.customer_id
     AND l.version_desc = 1
    WHERE f.version_asc = 1
      AND f.version_desc > 1
)
SELECT
    customer_id,
    first_email, latest_email,
    first_city, latest_city,
    first_status, latest_status,
    is_deleted
FROM first_last
ORDER BY customer_id;

-- name: versions_per_customer
-- Distribution of version counts (how "busy" customer records are).
SELECT
    version_count,
    COUNT(*) AS customers
FROM analytics.customer_version_summary
GROUP BY version_count
ORDER BY version_count;

-- name: customer_as_of
-- Point-in-time reconstruction: state of all customers as of a timestamp.
-- Uses the latest valid_from in the table as the default "as of" point.
SELECT h.customer_id, h.email, h.city, h.customer_status, h.is_deleted
FROM core.customer_history h
CROSS JOIN (SELECT MAX(valid_from) AS as_of FROM core.customer_history) p
WHERE h.valid_from <= p.as_of
  AND (h.valid_to IS NULL OR h.valid_to > p.as_of)
ORDER BY h.customer_id;

-- name: pipeline_lag
-- Staging backlog and end-to-end freshness of the history table.
SELECT
    COUNT(*)                                             AS staged_events,
    SUM(CASE WHEN processed_at IS NULL THEN 1 ELSE 0 END) AS unprocessed_events,
    MAX(event_ts)                                        AS latest_source_change,
    MAX(loaded_at)                                       AS latest_load
FROM raw.customer_cdc_events;
