"""``cdc-workload``: simulate the operational application writing to PostgreSQL.

In a CDC architecture the application never talks to Kafka; it just writes to
its database and Debezium turns WAL entries into events. This tool plays the
role of that application.

    cdc-workload run --inserts 10 --updates 25 --deletes 3
    cdc-workload lifecycle        one customer: insert -> 2 updates -> delete
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from faker import Faker

from customer_cdc.utils.config import SourceDbSettings
from customer_cdc.utils.logging_setup import configure_logging

logger = logging.getLogger("customer_cdc.workload")

STATUSES = ("ACTIVE", "INACTIVE", "SUSPENDED", "CLOSED")


@dataclass
class WorkloadResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0


class CustomerWorkload:
    """Generates realistic INSERT/UPDATE/DELETE traffic, one transaction per change."""

    def __init__(self, conn: psycopg.Connection[Any], seed: int | None = None) -> None:
        self.conn = conn
        self.rng = random.Random(seed)  # noqa: S311 - synthetic data, not crypto
        self.fake = Faker("en_US")
        if seed is not None:
            self.fake.seed_instance(seed)

    def _new_customer(self) -> dict[str, str]:
        first, last = self.fake.first_name(), self.fake.last_name()
        suffix = self.fake.unique.random_int(1000, 999999)
        return {
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{suffix}@example.com".lower(),
            "phone": f"+1-{self.rng.randint(200, 989)}-555-{self.rng.randint(0, 9999):04d}",
            "address": self.fake.street_address(),
            "city": self.fake.city(),
            "state": self.fake.state_abbr(include_territories=False),
            "customer_status": "ACTIVE",
        }

    def insert(self) -> int:
        row = self._new_customer()
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.customers (first_name, last_name, email, phone, address, "
                "city, state, customer_status) VALUES (%(first_name)s, %(last_name)s, %(email)s, "
                "%(phone)s, %(address)s, %(city)s, %(state)s, %(customer_status)s) "
                "RETURNING customer_id",
                row,
            )
            result = cur.fetchone()
        assert result is not None
        return int(result[0])

    def _random_customer_id(self) -> int | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM public.customers ORDER BY random() LIMIT 1")
            row = cur.fetchone()
        return int(row[0]) if row else None

    def _update_change(self) -> tuple[str, Any]:
        kind = self.rng.choice(("address", "email", "phone", "status"))
        if kind == "address":
            return (
                "address = %s, city = %s, state = %s",
                (
                    self.fake.street_address(),
                    self.fake.city(),
                    self.fake.state_abbr(include_territories=False),
                ),
            )
        if kind == "email":
            return (
                "email = %s",
                (
                    f"{self.fake.user_name()}.{self.fake.unique.random_int(1000, 999999)}"
                    "@example.com",
                ),
            )
        if kind == "phone":
            return (
                "phone = %s",
                (f"+1-{self.rng.randint(200, 989)}-555-{self.rng.randint(0, 9999):04d}",),
            )
        return ("customer_status = %s", (self.rng.choice(STATUSES),))

    def update(self, customer_id: int | None = None) -> int | None:
        customer_id = customer_id or self._random_customer_id()
        if customer_id is None:
            return None
        assignments, params = self._update_change()
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                f"UPDATE public.customers SET {assignments} WHERE customer_id = %s",
                (*params, customer_id),
            )
        return customer_id

    def delete(self, customer_id: int | None = None) -> int | None:
        customer_id = customer_id or self._random_customer_id()
        if customer_id is None:
            return None
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute("DELETE FROM public.customers WHERE customer_id = %s", (customer_id,))
        return customer_id

    def run(
        self,
        inserts: int,
        updates: int,
        deletes: int,
        sleep_seconds: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> WorkloadResult:
        ops = ["insert"] * inserts + ["update"] * updates + ["delete"] * deletes
        self.rng.shuffle(ops)
        result = WorkloadResult()
        for op in ops:
            if op == "insert":
                self.insert()
                result.inserted += 1
            elif op == "update" and self.update() is not None:
                result.updated += 1
            elif op == "delete" and self.delete() is not None:
                result.deleted += 1
            if sleep_seconds:
                sleep(sleep_seconds)
        logger.info("workload complete", extra=vars(result))
        return result

    def lifecycle(self) -> int:
        """Deterministic story for demos: create, move house, suspend, delete."""
        customer_id = self.insert()
        logger.info("inserted customer", extra={"customer_id": customer_id})
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                "UPDATE public.customers SET address = %s, city = %s, state = %s "
                "WHERE customer_id = %s",
                ("500 Market Street", "San Francisco", "CA", customer_id),
            )
        logger.info("updated address", extra={"customer_id": customer_id})
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                "UPDATE public.customers SET customer_status = 'SUSPENDED' WHERE customer_id = %s",
                (customer_id,),
            )
        logger.info("suspended customer", extra={"customer_id": customer_id})
        self.delete(customer_id)
        logger.info("deleted customer", extra={"customer_id": customer_id})
        return customer_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--inserts", type=int, default=10)
    run.add_argument("--updates", type=int, default=25)
    run.add_argument("--deletes", type=int, default=3)
    run.add_argument("--sleep", type=float, default=0.0, help="seconds between changes")
    run.add_argument("--seed", type=int, default=None)
    sub.add_parser("lifecycle")
    args = parser.parse_args(argv)

    configure_logging()
    settings = SourceDbSettings.from_env()
    with psycopg.connect(**settings.conninfo(), autocommit=True) as conn:  # type: ignore[arg-type]
        workload = CustomerWorkload(conn, seed=getattr(args, "seed", None))
        if args.command == "run":
            workload.run(args.inserts, args.updates, args.deletes, args.sleep)
        else:
            print(f"lifecycle customer_id={workload.lifecycle()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
