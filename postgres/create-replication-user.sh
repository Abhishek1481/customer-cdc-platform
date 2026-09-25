#!/usr/bin/env bash
# Creates a least-privilege login for Debezium. Runs once, on first container
# start, from /docker-entrypoint-initdb.d. The password comes from the
# environment (.env), never from a committed file.
set -euo pipefail

: "${DEBEZIUM_DB_USER:?DEBEZIUM_DB_USER must be set}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v repl_user="$DEBEZIUM_DB_USER" \
     -v repl_password="$DEBEZIUM_DB_PASSWORD" <<'EOSQL'
CREATE ROLE :"repl_user" WITH LOGIN REPLICATION PASSWORD :'repl_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO :"repl_user";
GRANT USAGE ON SCHEMA public TO :"repl_user";
GRANT SELECT ON public.customers TO :"repl_user";
EOSQL
