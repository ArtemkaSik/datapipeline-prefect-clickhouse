# ETL project for semester 2

Локальный стек:

- `postgres` на `5434` хранит `user_payments` и метаданные Prefect.
- `minio` на `9002/9003` хранит ежедневные файлы `site_visits`.
- `clickhouse` на `8124` принимает данные из ETL.
- `prefect-server` на `4200` показывает историю запусков.
- `prefect-cli` содержит код и зависимости пайплайна.
- `prefect-worker` забирает запуски из `Prefect` по расписанию.
- `superset` на `8088` показывает BI-интерфейс и подключен к `ClickHouse`.

## 1. Поднять инфраструктуру

```bash
docker compose up --build -d
```

## 2. Разложить `dump.sql` по источникам

Скрипт:

- вытащит `user_payments` из `dump.sql` и загрузит таблицу в `Postgres`;
- разобьет `site_visits` по дням;
- загрузит ежедневные CSV-файлы в бакет `clicks-data` в `MinIO`.

```bash
docker compose exec prefect-cli python scripts/bootstrap_sources.py
```

## 3. Запустить ETL

`site_visits` из `MinIO` в `ClickHouse`:

```bash
docker compose exec prefect-cli python s3_dag.py
```

`user_payments` из `Postgres` в `ClickHouse`:

```bash
docker compose exec prefect-cli python mysql_dag.py
```

Оба flow берут дату из `.env`-файлов в корне проекта:

- `s3_dag.py` читает `.env.site_visits`
- `mysql_dag.py` читает `.env.user_payments`

В каждом файле есть:

- `CURRENT_DATE` — дата текущей выгрузки
- `MIN_DATE` — начало диапазона
- `MAX_DATE` — конец диапазона

После успешного запуска `CURRENT_DATE` автоматически переключается на следующий день.
Если следующая дата выходит за `MAX_DATE`, скрипт возвращается к `MIN_DATE`.

## 4. Автоматический запуск через Prefect

В проекте настроены два deployment'а с расписанием:

- каждую среду в `18:00`
- timezone: `Europe/Moscow`

Чтобы опубликовать deployments в `Prefect`, выполните:

```bash
docker compose exec prefect-cli prefect deploy --all --no-prompt
```

После этого `prefect-worker` будет автоматически забирать запуски по расписанию.

## 5. Что лежит в целевых системах

- `Postgres`: база `etl_source`, таблица `public.user_payments`
- `MinIO`: бакет `clicks-data`, объекты вида `2022-06-30-site-visits.csv`
- `ClickHouse`: таблицы `tmp.site_visits`, `raw.site_visits`, `tmp.user_payments`, `raw.user_payments`
- `Prefect UI`: [http://localhost:4200](http://localhost:4200)
- `MinIO Console`: [http://localhost:9003](http://localhost:9003)
- `Superset`: [http://localhost:8088](http://localhost:8088)

## 6. Superset

Чтобы поднять `Superset`, достаточно перезапустить compose-стек:

```bash
docker compose up --build -d
```

После первого старта `Superset` сам:

- инициализирует свой metastore;
- создаст локального администратора;
- зарегистрирует подключение к `ClickHouse`.

Параметры входа:

- login: `admin`
- password: `admin`

Подключение к `ClickHouse` создается автоматически с именем `ClickHouse Local`.

## 7. Управление датами ETL

Примеры файлов:

```dotenv
CURRENT_DATE=2022-06-30
MIN_DATE=2022-06-30
MAX_DATE=2022-07-26
```

Если нужно начать цикл заново, просто вручную поменяйте `CURRENT_DATE` в нужном `.env`-файле.

## 8. Проверка руками

Примеры:

```bash
docker compose exec postgres psql -U postgres -d etl_source -c "select count(*) from public.user_payments;"
docker compose exec prefect-cli python -c "from etl_config import get_s3_client, MINIO_BUCKET; print(get_s3_client().list_objects_v2(Bucket=MINIO_BUCKET).get('KeyCount', 0))"
docker compose exec clickhouse clickhouse-client --query "select count() from raw.user_payments"
```
