from __future__ import annotations

from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task
from sqlalchemy import text

from date_state import load_date_state, save_next_date
from etl_config import (
    CLICKHOUSE_USER_PAYMENTS_RAW_TABLE,
    CLICKHOUSE_USER_PAYMENTS_TMP_TABLE,
    get_clickhouse_client,
    get_postgres_engine,
)

DATE_STATE_PATH = Path(__file__).resolve().with_name(".env.user_payments")


def resolve_run_date() -> str:
    return load_date_state(DATE_STATE_PATH).current_date.isoformat()


@task
def download_object_from_postgres(run_date: str) -> str:
    logger = get_run_logger()
    output_path = Path("/tmp") / f"{run_date}-user_payments.csv"
    engine = get_postgres_engine()
    query = text(
        """
        SELECT
            date,
            "timestamp",
            user_client_id,
            item,
            price,
            quantity,
            amount,
            discount,
            order_id,
            status
        FROM public.user_payments
        WHERE date = :run_date
        ORDER BY "timestamp", order_id
        """
    )

    with engine.connect() as connection:
        data = pd.read_sql(query, connection, params={"run_date": run_date})

    data.to_csv(output_path, index=False)
    logger.info("Downloaded %s rows from Postgres for %s", len(data), run_date)
    return str(output_path)


@task
def load_object_from_postgres_to_clickhouse(file_path: str) -> int:
    logger = get_run_logger()
    df = pd.read_csv(file_path)
    client = get_clickhouse_client()
    client.insert_df(CLICKHOUSE_USER_PAYMENTS_TMP_TABLE, df)
    logger.info("Loaded %s rows into %s", len(df), CLICKHOUSE_USER_PAYMENTS_TMP_TABLE)
    return len(df)


@task
def etl_inside_clickhouse(run_date: str) -> None:
    logger = get_run_logger()
    client = get_clickhouse_client()
    client.command(
        f"""
        INSERT INTO {CLICKHOUSE_USER_PAYMENTS_RAW_TABLE}
        SELECT
            toDate(parseDateTimeBestEffort(date)) AS date,
            parseDateTimeBestEffort(timestamp) AS timestamp,
            user_client_id,
            item,
            price,
            quantity,
            amount,
            discount,
            order_id,
            status,
            now() AS insert_time,
            cityHash64(
                date,
                timestamp,
                user_client_id,
                item,
                price,
                quantity,
                amount,
                discount,
                order_id,
                status
            ) AS hash
        FROM {CLICKHOUSE_USER_PAYMENTS_TMP_TABLE}
        WHERE date = '{run_date}'
        """
    )
    client.command(f"TRUNCATE TABLE {CLICKHOUSE_USER_PAYMENTS_TMP_TABLE}")
    logger.info("Loaded data for %s into %s", run_date, CLICKHOUSE_USER_PAYMENTS_RAW_TABLE)


@task
def advance_run_date() -> None:
    logger = get_run_logger()
    next_date = save_next_date(DATE_STATE_PATH)
    logger.info("Next user_payments date set to %s", next_date.isoformat())


@task
def remove_tmp_file(file_path: str) -> None:
    path = Path(file_path)
    if path.exists():
        path.unlink()


@flow(name="postgres-etl")
def etl_flow() -> None:
    selected_date = resolve_run_date()
    file_path = download_object_from_postgres(selected_date)
    load_object_from_postgres_to_clickhouse(file_path)
    etl_inside_clickhouse(selected_date)
    remove_tmp_file(file_path)
    advance_run_date()


if __name__ == "__main__":
    etl_flow()
