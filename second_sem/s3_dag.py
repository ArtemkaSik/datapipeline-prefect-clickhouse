from __future__ import annotations

from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

from date_state import load_date_state, save_next_date
from etl_config import (
    CLICKHOUSE_SITE_VISITS_RAW_TABLE,
    CLICKHOUSE_SITE_VISITS_TMP_TABLE,
    MINIO_BUCKET,
    get_clickhouse_client,
    get_s3_client,
)

DATE_STATE_PATH = Path(__file__).resolve().with_name(".env.site_visits")


def resolve_run_date() -> str:
    return load_date_state(DATE_STATE_PATH).current_date.isoformat()


@task
def download_object_from_s3(run_date: str) -> str:
    logger = get_run_logger()
    key = f"{run_date}-site-visits.csv"
    output_path = Path("/tmp") / key
    s3_client = get_s3_client()

    s3_client.download_file(MINIO_BUCKET, key, str(output_path))
    logger.info("Downloaded %s from bucket %s", key, MINIO_BUCKET)
    return str(output_path)


@task
def load_object_from_s3_to_clickhouse(file_path: str) -> int:
    logger = get_run_logger()
    df = pd.read_csv(file_path)
    client = get_clickhouse_client()
    client.insert_df(CLICKHOUSE_SITE_VISITS_TMP_TABLE, df)
    logger.info("Loaded %s rows into %s", len(df), CLICKHOUSE_SITE_VISITS_TMP_TABLE)
    return len(df)


@task
def etl_inside_clickhouse(run_date: str) -> None:
    logger = get_run_logger()
    client = get_clickhouse_client()
    client.command(
        f"""
        INSERT INTO {CLICKHOUSE_SITE_VISITS_RAW_TABLE}
        SELECT
            toDate(parseDateTimeBestEffort(date)) AS date,
            parseDateTimeBestEffort(timestamp) AS timestamp,
            user_client_id,
            action_type,
            placement_type,
            placement_id,
            user_visit_url,
            toDate(parseDateTimeBestEffort(load_date)) AS load_date,
            now() AS insert_time,
            cityHash64(
                date,
                timestamp,
                user_client_id,
                action_type,
                placement_type,
                placement_id,
                user_visit_url,
                load_date
            ) AS hash
        FROM {CLICKHOUSE_SITE_VISITS_TMP_TABLE}
        WHERE date = '{run_date}'
        """
    )
    client.command(f"TRUNCATE TABLE {CLICKHOUSE_SITE_VISITS_TMP_TABLE}")
    logger.info("Loaded data for %s into %s", run_date, CLICKHOUSE_SITE_VISITS_RAW_TABLE)


@task
def advance_run_date() -> None:
    logger = get_run_logger()
    next_date = save_next_date(DATE_STATE_PATH)
    logger.info("Next site_visits date set to %s", next_date.isoformat())


@task
def remove_tmp_file(file_path: str) -> None:
    path = Path(file_path)
    if path.exists():
        path.unlink()


@flow(name="s3-etl")
def etl_flow() -> None:
    selected_date = resolve_run_date()
    file_path = download_object_from_s3(selected_date)
    load_object_from_s3_to_clickhouse(file_path)
    etl_inside_clickhouse(selected_date)
    remove_tmp_file(file_path)
    advance_run_date()


if __name__ == "__main__":
    etl_flow()
