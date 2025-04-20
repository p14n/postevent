#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER SYSTEM SET wal_level to logical;
    ALTER SYSTEM SET max_wal_senders to 3;
EOSQL
