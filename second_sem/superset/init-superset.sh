#!/bin/sh
set -eu

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

superset init

superset set_database_uri \
  --database_name "${SUPERSET_CLICKHOUSE_DATABASE_NAME}" \
  --uri "${SUPERSET_CLICKHOUSE_URI}"

exec superset run -h 0.0.0.0 -p 8088
