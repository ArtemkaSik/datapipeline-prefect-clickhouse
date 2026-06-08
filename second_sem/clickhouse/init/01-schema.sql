CREATE DATABASE IF NOT EXISTS tmp;
CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS tmp.site_visits (
    date String,
    timestamp String,
    user_client_id Int32,
    action_type String,
    placement_type String,
    placement_id Int32,
    user_visit_url String,
    load_date String
) ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.site_visits (
    date Date,
    timestamp DateTime,
    user_client_id Int32,
    action_type String,
    placement_type String,
    placement_id Int32,
    user_visit_url String,
    load_date Date,
    insert_time DateTime,
    hash UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, user_client_id, timestamp, placement_id, hash);

CREATE TABLE IF NOT EXISTS tmp.user_payments (
    date String,
    timestamp String,
    user_client_id Int32,
    item String,
    price Int32,
    quantity Int32,
    amount Float64,
    discount Float64,
    order_id Int32,
    status String
) ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS raw.user_payments (
    date Date,
    timestamp DateTime,
    user_client_id Int32,
    item String,
    price Int32,
    quantity Int32,
    amount Float64,
    discount Float64,
    order_id Int32,
    status String,
    insert_time DateTime,
    hash UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, user_client_id, timestamp, order_id, hash);
