# Interview notes: Customer Data Change Tracking Platform

## 1. 30-second explanation

"I built a change data capture pipeline for a customer table. Instead of nightly batch queries, Debezium reads PostgreSQL's write-ahead log and publishes every insert, update and delete to Kafka, keyed by customer ID so each customer's changes stay in order. A Python consumer validates the events, sends bad ones to a dead-letter topic, and stages them idempotently in the warehouse. Then a SQL job builds an SCD Type 2 history table. You can see any customer's state at any point in time, deletes included, and replaying events never creates duplicates. It runs locally on DuckDB with the same SQL I wrote for Snowflake."

## 2. Two-minute explanation

- **Source.** PostgreSQL runs with `wal_level=logical`. The `customers` table has `REPLICA IDENTITY FULL`, so deletes carry the full old row, and a publication scopes CDC to that table. Debezium connects with a least-privilege replication user through the built-in `pgoutput` plugin. I verified the full-before-image behavior in an integration test that decodes the `pgoutput` protocol directly.
- **Transport.** Debezium emits an envelope with `before`, `after`, `op` and `source` (which includes the LSN and commit timestamp). The topic has three partitions keyed by `customer_id`. That gives per-customer ordering and lets the consumer group scale to three instances. There's a separate DLQ topic.
- **Consumer.** It polls micro-batches with auto-commit off.
  - Parsing is strict: malformed messages go to the DLQ with error headers, and tombstones are skipped.
  - The batch is staged into `RAW.CUSTOMER_CDC_EVENTS`, skipping any `change_key` that already exists. Transient warehouse errors are retried with exponential backoff and jitter.
  - Only then are offsets committed. If anything fails, it exits without committing and Kafka redelivers. That's at-least-once delivery with idempotent writes, which gives effectively-once results.
- **Transformation.** One SQL script runs identically on Snowflake and DuckDB. It freezes the pending events, orders them by LSN, and drops no-op changes using an MD5 hash of the tracked columns. It chains multiple changes per customer with `LEAD()`, closes the current version, inserts the new versions, and marks the events processed, all in one transaction. A delete becomes a tombstone version, so history is never lost.
- **Serving.** Analytics views give current state and change summaries. The example queries cover current state, history, changed customers, versions per customer and point-in-time reconstruction.

## 3. Architecture questions

**Why Kafka and not direct PostgreSQL → Snowflake?**
Decoupling and durability. If Snowflake is down or slow, events wait in Kafka (7-day retention) instead of backing up WAL on the production database. Kafka also gives replay (reset offsets to rebuild), fan-out (other consumers such as search indexing or cache invalidation can read the same topic), and per-key ordering.

**Why Debezium instead of writing my own?**
Snapshotting, LSN offset tracking, type mapping, schema changes, heartbeats and restart semantics are all solved, battle-tested problems. My own code focuses on the business-specific part: validation, idempotent staging and SCD2.

**Why not batch ETL?**
Batch captures state, not changes. Intermediate updates between runs and hard deletes are invisible. Incremental batch on `updated_at` also depends on the application always setting that column, and it can't see deletes at all.

**How does CDC work at the database level?**
PostgreSQL writes every change to the WAL before applying it. Logical decoding turns WAL records into row changes. A replication slot tracks how far the consumer has read, and PostgreSQL keeps WAL until the slot's confirmed position passes it. `pgoutput` streams changes for tables in a publication.

**How do you handle duplicate events?**
At three levels:

1. Staging inserts only unseen `change_key`s (`customer_id|lsn|op`).
2. The merge ignores events whose LSN is at or below the current version's.
3. A no-op change (same attribute hash) creates no version.

There are tests for redelivery, replay at a different offset, and "touch" updates.

**How do you guarantee ordering?**
Kafka guarantees order within a partition, and the key is the primary key, so one customer's events are ordered. I don't rely on that alone. The SCD merge orders by source LSN, so even events that arrive out of order in a batch produce the right history. There's a test that shuffles arrival order.

**What happens if Kafka goes down?**
Debezium can't produce, so it pauses. PostgreSQL retains WAL for the slot, so nothing is lost, but disk usage on the primary grows. You alert on slot lag and set `max_slot_wal_keep_size` as a safety valve. The consumer's librdkafka client reconnects automatically. Fatal errors stop the process and it restarts.

**What happens if Snowflake is unavailable?**
The consumer retries with backoff. When retries run out, it exits without committing offsets. The container restarts and resumes from the last committed offset. Data waits in Kafka within retention, and staging dedupe makes the redelivered batches safe.

**How are deletes handled in history?**
The current version is closed and a tombstone version is inserted, with `is_deleted = TRUE` and `is_current = TRUE` and the last known attributes. "Current customers" filters out deleted rows. "As of" queries still see the customer before the delete. If the same ID is inserted again, the tombstone is closed like any other version.

**Why use source commit time for `valid_from`?**
It's deterministic and reflects business reality. Load time would make history depend on when the pipeline ran, and a replay would produce different timestamps.

**How would you handle schema changes (a new column)?**
Debezium picks up the new column automatically, and the consumer ignores unknown fields. To track it, add the column to `TRACKED_COLUMNS`, the staging DDL and the history DDL. The hash then changes for new events only. For breaking changes, use a schema registry with Avro or Protobuf and compatibility rules.

## 4. SQL questions

1. **Write a query for each customer's current state from an SCD2 table.** `WHERE is_current AND NOT is_deleted`. Or, without the flag: `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY valid_from DESC) = 1`.
2. **Show the state of all customers on a given date.** `WHERE valid_from <= :d AND (valid_to IS NULL OR valid_to > :d)`. Note the half-open interval: `valid_to` of one version equals `valid_from` of the next.
3. **How do you close old versions and insert new ones in one pass?** Use `UPDATE … FROM (new versions)` followed by `INSERT`, inside a transaction. A single `MERGE` can't both update the old row and insert the new row for the same key unless you use the "union the key twice" trick.
4. **Multiple changes for a customer in one batch?** Use `LEAD(event_ts) OVER (PARTITION BY customer_id ORDER BY lsn)` for `valid_to`, and `ROW_NUMBER() … DESC = 1` for `is_current`.
5. **Find customers whose email changed.** `LAG(email) OVER (PARTITION BY customer_id ORDER BY valid_from)` and compare. See the `changed_customers` query for a first-vs-latest version.
6. **Validate SCD2 integrity.** Exactly one current row per customer (`GROUP BY … HAVING SUM(CASE WHEN is_current …) <> 1`). No overlapping intervals (self-join, or `LEAD(valid_from) <> valid_to`).
7. **Deduplicate events.** `QUALIFY ROW_NUMBER() OVER (PARTITION BY change_key ORDER BY loaded_at) = 1`, or `INSERT … WHERE NOT EXISTS`.

## 5. Python questions

- **JSON parsing robustness:** `decode_envelope` handles bytes vs str, invalid UTF-8, non-object JSON and schema-wrapped payloads. Type checks reject `True` where an int is expected: in Python, `bool` is a subclass of `int`, so `isinstance(True, int)` is true.
- **Retries:** tenacity with `wait_exponential_jitter`, retrying only on the `RetryableError` hierarchy. Non-retryable errors, such as SQL syntax errors, fail fast. Jitter avoids a thundering herd when many consumers restart together.
- **Exceptions:** The error taxonomy drives behavior. `MalformedEventError` means DLQ. `RetryableWarehouseError` means retry. Anything else means crash without committing.
- **Generators and memory:** Batches are bounded by `CONSUMER_BATCH_SIZE`, so memory is O(batch). A replay of a very large file should stream lines with a generator instead of `read_text().splitlines()`. That's a known simplification in replay mode.
- **Concurrency:** One consumer process per partition group, scaled horizontally through the consumer group rather than threads. librdkafka does network I/O on its own threads. Threads inside the consumer would complicate offset ordering.
- **Graceful shutdown:** SIGTERM/SIGINT set a flag. The loop finishes the batch, runs a final SCD merge, commits and closes. `consumer.close()` leaves the group cleanly, which triggers a fast rebalance.
- **Testability:** The Kafka client, DLQ sink and warehouse are injected. Tests use fakes for Kafka and a real DuckDB for SQL.

## 6. Scenario questions

**The source generates 10x more CDC events than normal. What would you change?**

1. Check where the lag is: slot lag (Debezium side) or consumer lag (Kafka side).
2. On the consumer side:
   - Add consumers up to the partition count. If that isn't enough, increase partitions (existing keys may re-map; the LSN-based merge tolerates that).
   - Raise `CONSUMER_BATCH_SIZE`.
   - Switch RAW loading to bulk `COPY`/Snowpipe Streaming.
   - Run the SCD merge less often but on bigger sets.
3. On the Debezium side: raise `max.batch.size` and `max.queue.size`.
4. Investigate the cause. A bulk backfill job may not need row-level history, and could be run with the table excluded from the publication plus a re-snapshot.

**A bad deploy produced 5,000 malformed events in the DLQ. How do you recover?**
Fix the parser, then replay the DLQ topic (the original bytes are preserved) through the same processor. Dedupe makes it safe even if some events were processed before.

**Analysts report a customer shows two current rows.**
Run the integrity query. The likely cause is two SCD jobs running concurrently. Fix it by serializing the job (a single Snowflake TASK, or a lock table), then repair with a `QUALIFY` that keeps the latest version per customer.

**You need to rebuild history from scratch.**
Truncate CORE, set `processed_at = NULL` on RAW, and re-run the merge. The result is deterministic because it's ordered by LSN and uses commit timestamps. If RAW was also lost, reset the consumer group offsets to earliest (within Kafka retention), or trigger a Debezium incremental snapshot.

**The Debezium connector was down for three days.**
The slot kept the WAL, so Debezium resumes where it stopped. Check disk usage on the primary first. If WAL was dropped because `max_slot_wal_keep_size` was exceeded, the slot is invalidated. Recreate it and re-snapshot, and the snapshot rows (`op=r`) flow through the same idempotent path.
