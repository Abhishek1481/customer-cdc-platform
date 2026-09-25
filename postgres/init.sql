-- Operational (OLTP) schema for the customer service.
-- The server is started with wal_level=logical (see docker-compose.yml) so that
-- Debezium can read row-level changes from the write-ahead log via pgoutput.

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id      SERIAL PRIMARY KEY,
    first_name       VARCHAR(100) NOT NULL,
    last_name        VARCHAR(100) NOT NULL,
    email            VARCHAR(255) NOT NULL UNIQUE,
    phone            VARCHAR(40),
    address          VARCHAR(255),
    city             VARCHAR(100),
    state            CHAR(2),
    customer_status  VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE'
        CHECK (customer_status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON public.customers;
CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- REPLICA IDENTITY FULL makes the WAL carry the complete "before" image for
-- UPDATE and DELETE. Without it Debezium only receives the primary key for a
-- delete, and the warehouse could not record the last known attribute values.
-- Trade-off: more WAL volume per update/delete.
ALTER TABLE public.customers REPLICA IDENTITY FULL;

-- Publication consumed by Debezium's pgoutput plugin. Created here (by the
-- table owner) so the connector's replication user does not need ownership.
DROP PUBLICATION IF EXISTS customers_publication;
CREATE PUBLICATION customers_publication FOR TABLE public.customers;
