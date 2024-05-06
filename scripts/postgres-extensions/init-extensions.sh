#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER docker;
  CREATE DATABASE docker;
  GRANT ALL PRIVILEGES ON DATABASE docker TO docker;
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  SELECT * FROM pg_extension;
EOSQL
