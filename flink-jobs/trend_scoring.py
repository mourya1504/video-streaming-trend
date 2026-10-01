"""
Flink speed layer: real-time trend scoring.

Reads the video-events Kafka topic, applies a sliding window over event
timestamps, and counts events per video_id within each window. This is the
"approximate but fast" half of the Lambda architecture -- Spark will later
recompute exact aggregates over the same time ranges for reconciliation.

Run from inside the jobmanager container:
    docker exec -it jobmanager flink run -py /opt/flink-jobs/trend_scoring.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

# --- Config (kept inline here; move to a shared config module if this grows) ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"   # internal Docker-network listener
SOURCE_TOPIC = "video-events"
SINK_TOPIC = "trend-scores"
CONSUMER_GROUP = "flink-trend-scoring"

WINDOW_SIZE = "1 MINUTES"     # how wide each window is
WINDOW_SLIDE = "10 SECONDS"   # how often a new window starts
WATERMARK_DELAY = "5 SECONDS" # how much out-of-order lateness we tolerate


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Keep parallelism low for a local single-TaskManager setup.
    t_env.get_config().set("parallelism.default", "1")

    # --- Source: video_events table backed by the Kafka topic ---
    # event_time is parsed from the ISO-8601 "timestamp" field the generator sends.
    # The watermark tells Flink how long to wait for late-arriving events before
    # closing a window -- this is what makes windowed aggregation possible on an
    # unbounded stream.
    t_env.execute_sql(f"""
        CREATE TABLE video_events (
            event_id STRING,
            event_type STRING,
            video_id STRING,
            user_id STRING,
            `timestamp` STRING,
            watch_duration_ms BIGINT,
            event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
            WATERMARK FOR event_time AS event_time - INTERVAL '{WATERMARK_DELAY.split()[0]}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{CONSUMER_GROUP}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # --- Sink: write results to the trend-scores Kafka topic as JSON ---
    # This decouples Flink from whatever consumes the results (ClickHouse,
    # a dashboard, etc.) -- Flink doesn't need to know who reads this topic.
    t_env.execute_sql(f"""
        CREATE TABLE trend_scores_sink (
            video_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # --- Windowed aggregation: sliding window event counts per video ---
    # HOP = sliding window: a new 1-minute window starts every 10 seconds,
    # so windows overlap -- this gives smoother, more frequent trend updates
    # than a plain tumbling window would.
    t_env.execute_sql(f"""
        INSERT INTO trend_scores_sink
        SELECT
            video_id,
            window_start,
            window_end,
            COUNT(*) AS event_count
        FROM TABLE(
            HOP(TABLE video_events, DESCRIPTOR(event_time), INTERVAL '{WINDOW_SLIDE.split()[0]}' SECOND, INTERVAL '{WINDOW_SIZE.split()[0]}' MINUTE)
        )
        GROUP BY video_id, window_start, window_end
    """).wait()


if __name__ == "__main__":
    main()