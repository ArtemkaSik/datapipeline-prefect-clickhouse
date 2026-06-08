from __future__ import annotations

import csv
import sys
import tempfile
import time
from pathlib import Path

from botocore.exceptions import ClientError
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl_config import MINIO_BUCKET, get_postgres_engine, get_s3_client

SITE_VISITS_COPY_PREFIX = "COPY public.site_visits"
USER_PAYMENTS_COPY_PREFIX = "COPY public.user_payments"

SITE_VISITS_COLUMNS = [
    "date",
    "timestamp",
    "user_client_id",
    "action_type",
    "placement_type",
    "placement_id",
    "user_visit_url",
    "load_date",
]

USER_PAYMENTS_COLUMNS = [
    "date",
    "timestamp",
    "user_client_id",
    "item",
    "price",
    "quantity",
    "amount",
    "discount",
    "order_id",
    "status",
]


def wait_for_postgres(retries: int = 30, delay: int = 2) -> None:
    engine = get_postgres_engine()
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception:
            if attempt == retries:
                raise
            time.sleep(delay)


def wait_for_minio(retries: int = 30, delay: int = 2) -> None:
    s3_client = get_s3_client()
    for attempt in range(1, retries + 1):
        try:
            s3_client.list_buckets()
            return
        except Exception:
            if attempt == retries:
                raise
            time.sleep(delay)


def ensure_bucket() -> None:
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3_client.create_bucket(Bucket=MINIO_BUCKET)


def split_dump(dump_path: Path, workspace: Path) -> tuple[Path, Path]:
    site_visits_dir = workspace / "site_visits"
    site_visits_dir.mkdir(parents=True, exist_ok=True)
    user_payments_path = workspace / "user_payments.csv"

    site_visit_files: dict[str, tuple[object, csv.writer]] = {}
    user_rows = 0
    site_rows = 0
    current_section: str | None = None

    with dump_path.open("r", encoding="utf-8") as dump_file, user_payments_path.open(
        "w", newline="", encoding="utf-8"
    ) as user_payments_file:
        user_writer = csv.writer(user_payments_file)
        user_writer.writerow(USER_PAYMENTS_COLUMNS)

        for line in dump_file:
            if line.startswith(SITE_VISITS_COPY_PREFIX):
                current_section = "site_visits"
                continue

            if line.startswith(USER_PAYMENTS_COPY_PREFIX):
                current_section = "user_payments"
                continue

            if current_section is None:
                continue

            if line.rstrip("\n") == "\\.":
                current_section = None
                continue

            row = line.rstrip("\n").split("\t")

            if current_section == "site_visits":
                date_value = row[0]
                if date_value not in site_visit_files:
                    file_path = site_visits_dir / f"{date_value}-site-visits.csv"
                    file_handle = file_path.open("w", newline="", encoding="utf-8")
                    writer = csv.writer(file_handle)
                    writer.writerow(SITE_VISITS_COLUMNS)
                    site_visit_files[date_value] = (file_handle, writer)
                _, writer = site_visit_files[date_value]
                writer.writerow(row)
                site_rows += 1
                continue

            user_writer.writerow(row)
            user_rows += 1

    for file_handle, _ in site_visit_files.values():
        file_handle.close()

    print(f"site_visits rows prepared: {site_rows}")
    print(f"user_payments rows prepared: {user_rows}")

    return site_visits_dir, user_payments_path


def load_user_payments(user_payments_path: Path, force: bool) -> None:
    engine = get_postgres_engine()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.user_payments (
        date text NOT NULL,
        "timestamp" timestamp without time zone NOT NULL,
        user_client_id integer NOT NULL,
        item text NOT NULL,
        price integer NOT NULL,
        quantity integer NOT NULL,
        amount double precision NOT NULL,
        discount double precision NOT NULL,
        order_id integer NOT NULL,
        status text NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_user_payments_date
        ON public.user_payments (date);
    """

    with engine.begin() as connection:
        connection.exec_driver_sql(create_table_sql)
        if force:
            connection.exec_driver_sql("TRUNCATE TABLE public.user_payments")

    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            if not force:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM public.user_payments LIMIT 1)")
                if cursor.fetchone()[0]:
                    print("user_payments already loaded, skipping")
                    raw_connection.rollback()
                    return

            with user_payments_path.open("r", encoding="utf-8") as source_file:
                cursor.copy_expert(
                    """
                    COPY public.user_payments (
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
                    )
                    FROM STDIN WITH CSV HEADER
                    """,
                    source_file,
                )
        raw_connection.commit()
    finally:
        raw_connection.close()

    print("user_payments loaded into Postgres")


def upload_site_visits(site_visits_dir: Path, force: bool) -> None:
    ensure_bucket()
    s3_client = get_s3_client()

    for csv_file in sorted(site_visits_dir.glob("*.csv")):
        if not force:
            try:
                s3_client.head_object(Bucket=MINIO_BUCKET, Key=csv_file.name)
                continue
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code", "")
                if error_code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
        s3_client.upload_file(str(csv_file), MINIO_BUCKET, csv_file.name)

    print("site_visits uploaded to MinIO")


def main() -> None:
    dump_path = PROJECT_ROOT / "dump.sql"
    if not dump_path.exists():
        raise FileNotFoundError(f"Dump file not found: {dump_path}")

    wait_for_postgres()
    wait_for_minio()

    with tempfile.TemporaryDirectory() as tmp_dir:
        workspace = Path(tmp_dir)
        site_visits_dir, user_payments_path = split_dump(dump_path, workspace)
        load_user_payments(user_payments_path, force=False)
        upload_site_visits(site_visits_dir, force=False)


if __name__ == "__main__":
    main()
