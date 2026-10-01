-- ClickHouse serving layer for real-time trend scores.
--
-- Three objects, following ClickHouse's standard Kafka-ingestion pattern:
--   1. trend_scores          -- the actual queryable table (MergeTree)
--   2. trend_scores_kafka    -- a "virtual" table representing the Kafka topic
--   3. trend_scores_mv       -- a materialized view that continuously moves
--                               rows from (2) into (1) as they arrive
--
-- Run with: docker exec -it clickhouse clickhouse-client --multiquery < this file
-- (or paste each statement one at a time into `docker exec -it clickhouse clickhouse-client`)

-- 1. Target table: this is what your dashboard and Trino queries will read from.
CREATE TABLE IF NOT EXISTS trend_scores
(
    video_id      String,
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    event_count   UInt64
)
ENGINE = MergeTree()
ORDER BY (video_id, window_start);

-- 2. Kafka engine table: NOT a real table -- it's a live view over the topic.
-- Every SELECT against it consumes messages, so it's only ever queried by the
-- materialized view below, never directly.
-- window_start/window_end come in as ISO-8601 strings from Flink's JSON output,
-- so they're read as String here and parsed properly in the materialized view.
CREATE TABLE IF NOT EXISTS trend_scores_kafka
(
    video_id      String,
    window_start  String,
    window_end    String,
    event_count   UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'trend-scores',
    kafka_group_name = 'clickhouse-trend-scores',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 3. Materialized view: the actual ETL step. Fires automatically whenever
-- new messages land in the Kafka topic, parsing the timestamp strings into
-- real DateTime64 values and inserting into the target table.
CREATE MATERIALIZED VIEW IF NOT EXISTS trend_scores_mv
TO trend_scores
AS
SELECT
    video_id,
    parseDateTime64BestEffort(window_start) AS window_start,
    parseDateTime64BestEffort(window_end)   AS window_end,
    event_count
FROM trend_scores_kafka;
