# CDC concepts used in this project

## Why the WAL?

Every PostgreSQL change is first written to the **write-ahead log** (WAL) for crash recovery. With `wal_level=logical`, the WAL holds enough information to decode *row-level* changes. Reading it has three advantages:

- **Completeness.** Every committed change is in the log, in commit order, including deletes and intermediate states that a polling query would miss.
- **Low overhead.** There are no `SELECT … WHERE updated_at > ?` scans against production tables, and no dependency on the application maintaining `updated_at` correctly.
- **Transactional truth.** Only committed changes are decoded. Rolled-back work never appears.

How this is configured here (`postgres/init.sql`, `docker-compose.yml`):

| Setting | Why |
|---|---|
| `wal_level=logical` | Enables logical decoding |
| `max_replication_slots`, `max_wal_senders` | Capacity for Debezium's slot and connection |
| `CREATE PUBLICATION customers_publication FOR TABLE public.customers` | Limits `pgoutput` to the table we care about. Created by the table owner so the Debezium user doesn't need ownership. |
| `ALTER TABLE customers REPLICA IDENTITY FULL` | Makes UPDATE/DELETE WAL records carry the complete old row (the "before" image) |
| Role `debezium` with `REPLICATION` + `SELECT` | Least privilege: stream the WAL and snapshot the table, nothing else |

A **replication slot** (`customers_cdc_slot`) makes PostgreSQL keep WAL segments until the consumer (Debezium) confirms it has processed them. That is what makes the pipeline lossless across restarts. It is also the main operational risk: if the connector is down for days, WAL piles up on the primary. Monitor `pg_replication_slots` lag.

`tests/integration/test_postgres_source.py::test_wal_contains_full_before_image_for_delete` creates a `pgoutput` slot on the same publication, runs insert → update → update → delete, and decodes the binary replication protocol. It asserts the DELETE message carries a full old tuple (`'O'`), not just the key (`'K'`).

## Why Debezium?

Debezium is a Kafka Connect source connector that implements the hard parts of log-based CDC:

- An **initial snapshot** (`snapshot.mode=initial`), so existing rows are emitted as `op=r` before streaming starts
- **Streaming** from the replication slot, with type mapping from PostgreSQL types to JSON
- **Offset handling.** The last processed LSN is stored in the Connect offsets topic (`connect_offsets`), and Debezium confirms flushed LSNs back to the slot. After a restart it resumes from the stored LSN. Events may be re-sent around a crash, so the downstream consumer must be idempotent (at-least-once).
- **Heartbeats** (`heartbeat.interval.ms`), so the slot keeps advancing even when the captured table is idle

## Anatomy of a change event

With `JsonConverter` and `schemas.enable=false`:

```json
{
  "before": {"customer_id": 1, "first_name": "Maria", "city": "Austin", "...": "..."},
  "after":  {"customer_id": 1, "first_name": "Maria", "city": "Dallas", "...": "..."},
  "source": {
    "connector": "postgresql", "name": "cdc", "db": "customers_db",
    "schema": "public", "table": "customers",
    "lsn": 24000640, "txId": 2400064, "ts_ms": 1717236360000, "snapshot": "false"
  },
  "op": "u",
  "ts_ms": 1717236360150
}
```

| Field | Meaning | Use in this pipeline |
|---|---|---|
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read (also `t` truncate, `m` message) | Picks the row image. `t`/`m` are ignored. |
| `before` | Row before the change (complete because of `REPLICA IDENTITY FULL`). `null` for inserts. | Row image for deletes |
| `after` | Row after the change. `null` for deletes. | Row image for c/u/r |
| `source.lsn` | Log sequence number: position in the WAL | Ordering of versions. Part of the idempotency key. |
| `source.ts_ms` | Commit time in the source | `valid_from` / `valid_to` |
| `source.txId` | Source transaction id | Stored for lineage |
| `ts_ms` | When Debezium processed the event | Not used for history (processing time ≠ event time) |

After a `d` event, Debezium sends a **tombstone**: same key, `null` value. It lets Kafka log compaction drop the key entirely. The consumer counts and skips tombstones.

Temporal types: `timestamptz` arrives as an ISO-8601 string (`ZonedTimestamp`). A plain `timestamp` arrives as epoch microseconds (`MicroTimestamp`). `parse_source_timestamp` handles both.

## Kafka terms as used here

| Term | In this project |
|---|---|
| **Producer** | Debezium (via Kafka Connect) writes change events. The consumer's DLQ sink is also a producer, with idempotence enabled and `acks=all`. |
| **Consumer** | `cdc-consumer` (confluent-kafka / librdkafka) |
| **Topic** | `cdc.public.customers` (`<topic.prefix>.<schema>.<table>`) and `cdc.public.customers.dlq` |
| **Partition** | 3 partitions. The message key is the primary key, so **all changes for one customer are in one partition, in order**. There is no ordering across customers, and none is needed because SCD2 is per customer. |
| **Offset** | Position of a message within a partition. The consumer commits offsets **manually and synchronously after** the batch is durably staged. |
| **Consumer group** | `customer-cdc-warehouse-loader`. Partitions are shared across instances in the group, so the maximum useful parallelism is the partition count. Uses the `cooperative-sticky` assignor to avoid stop-the-world rebalances. |
| **Retention** | 7 days on the CDC topic (`retention.ms=604800000`) and 14 days on the DLQ. This is the window within which a warehouse outage is recoverable by replay. |
| **Replication factor** | 1 locally (single broker). Production would use 3 with `min.insync.replicas=2`. |

## Delivery semantics

End to end, the pipeline is **at-least-once**:

- Debezium can re-send events around a restart.
- The consumer commits after staging, so a crash before the commit means a redelivery.

The results are **effectively-once**, because every layer is idempotent:

1. Staging inserts only `change_key`s that are not already present (`customer_id|lsn|op`).
2. The SCD merge ignores events with LSN ≤ the current version's LSN.
3. The SCD merge ignores events whose attribute hash equals the previous version's.

Kafka transactions (exactly-once) wouldn't help here: the sink is an external warehouse, not Kafka. Idempotent writes are the standard answer.
