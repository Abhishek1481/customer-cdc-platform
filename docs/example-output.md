# Example output (captured, not hand-written)

Captured on 2026-09-25 (UTC) by replaying the synthetic fixture `sample_events/customer_changes.jsonl` into the local DuckDB warehouse (`WAREHOUSE_TARGET=duckdb`). It exercises the same parse -> stage -> SCD2 code the Kafka consumer uses. It was **not** produced by a live Debezium connector or by Snowflake. Timestamps in `valid_from`/`valid_to` come from the fixture's `source.ts_ms` values.

```text
$ uv run cdc-replay sample_events/customer_changes.jsonl
INFO    customer_cdc.warehouse.base: warehouse initialized warehouse=duckdb
WARNING customer_cdc.consumer.processor: malformed event sent to dead-letter topic topic=customer_changes.jsonl partition=0 offset=12 reason=message is not valid UTF-8 JSON: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
WARNING customer_cdc.consumer.dead_letter: event rejected reason=message is not valid UTF-8 JSON: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
INFO    customer_cdc.warehouse.base: events staged warehouse=duckdb received=12 staged=11 duplicates_skipped=1
INFO    customer_cdc.consumer.processor: batch processed received=14 parsed=12 tombstones=1 ignored=0 dead_lettered=1 staged=11 inserts=1 updates=6 deletes=1 snapshot_reads=4
INFO    customer_cdc.warehouse.base: scd type 2 applied warehouse=duckdb events_processed=11 versions_inserted=10
{"file": "sample_events\\customer_changes.jsonl", "batch": {"received": 14, "parsed": 12, "tombstones": 1, "ignored": 0, "dead_lettered": 1, "staged": 11, "inserts": 1, "updates": 6, "deletes": 1, "snapshot_reads": 4}, "scd": {"events_processed": 11, "versions_inserted": 10}, "rejected": 1}
```

## `cdc-warehouse query current_customers`

```text
customer_id | first_name | last_name | email                    | city    | state | customer_status | current_since
------------+------------+-----------+--------------------------+---------+-------+-----------------+--------------------
1           | Maria      | Garcia    | maria.g@example.com      | Dallas  | TX    | ACTIVE          | 2024-06-01 10:08:00
2           | James      | Chen      | james.chen@example.com   | Seattle | WA    | ACTIVE          | 2024-06-01 10:11:00
3           | Aisha      | Okafor    | aisha.okafor@example.com | Chicago | IL    | ACTIVE          | 2024-06-01 10:03:00
5           | Sofia      | Rossi     | sofia.rossi@example.com  | Denver  | CO    | ACTIVE          | 2024-06-01 10:05:00
(4 rows)
```

## `cdc-warehouse query customer_history`

```text
customer_id | customer_status | email                    | city   | state | is_deleted | valid_from          | valid_to            | is_current | source_op
------------+-----------------+--------------------------+--------+-------+------------+---------------------+---------------------+------------+----------
1           | ACTIVE          | maria.garcia@example.com | Austin | TX    | False      | 2024-06-01 10:01:00 | 2024-06-01 10:06:00 | False      | r
1           | ACTIVE          | maria.garcia@example.com | Dallas | TX    | False      | 2024-06-01 10:06:00 | 2024-06-01 10:08:00 | False      | u
1           | ACTIVE          | maria.g@example.com      | Dallas | TX    | False      | 2024-06-01 10:08:00 |                     | True       | u
(3 rows)
```

## `cdc-warehouse query changed_customers`

```text
customer_id | first_email              | latest_email            | first_city | latest_city | first_status | latest_status | is_deleted
------------+--------------------------+-------------------------+------------+-------------+--------------+---------------+-----------
1           | maria.garcia@example.com | maria.g@example.com     | Austin     | Dallas      | ACTIVE       | ACTIVE        | False
2           | james.chen@example.com   | james.chen@example.com  | Seattle    | Seattle     | ACTIVE       | ACTIVE        | False
4           | liam.murphy@example.com  | liam.murphy@example.com | Boston     | Boston      | ACTIVE       | ACTIVE        | True
(3 rows)
```

## `cdc-warehouse query versions_per_customer`

```text
version_count | customers
--------------+----------
1             | 2
2             | 1
3             | 2
(3 rows)
```

## `cdc-warehouse query customer_as_of`

```text
customer_id | email                    | city    | customer_status | is_deleted
------------+--------------------------+---------+-----------------+-----------
1           | maria.g@example.com      | Dallas  | ACTIVE          | False
2           | james.chen@example.com   | Seattle | ACTIVE          | False
3           | aisha.okafor@example.com | Chicago | ACTIVE          | False
4           | liam.murphy@example.com  | Boston  | ACTIVE          | True
5           | sofia.rossi@example.com  | Denver  | ACTIVE          | False
(5 rows)
```

## `cdc-warehouse query pipeline_lag`

```text
staged_events | unprocessed_events | latest_source_change | latest_load
--------------+--------------------+----------------------+---------------------------
11            | 0                  | 2024-06-01 10:11:00  | 2026-09-25 00:39:23.438437
(1 rows)
```
