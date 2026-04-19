#!/bin/bash
# 01_create_multiple_databases.sh
#
# Creates multiple PostgreSQL databases from the POSTGRES_MULTIPLE_DATABASES
# environment variable.  Format:  db1:user1:pass1,db2:user2:pass2,...
#
# Sourced from:
#   https://github.com/mrts/docker-postgresql-multiple-databases

set -e
set -u

function create_user_and_database() {
    local entry=$1
    local db=$(echo "$entry" | cut -d: -f1)
    local user=$(echo "$entry" | cut -d: -f2)
    local pass=$(echo "$entry" | cut -d: -f3)

    echo "  Creating database '$db' with owner '$user' ..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $user WITH PASSWORD '$pass';
        CREATE DATABASE $db;
        GRANT ALL PRIVILEGES ON DATABASE $db TO $user;
        \c $db;
        GRANT ALL ON SCHEMA public TO $user;
EOSQL
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for entry in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
        create_user_and_database "$entry"
    done
    echo "Multiple databases created."
fi
