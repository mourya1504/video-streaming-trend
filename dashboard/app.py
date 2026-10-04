"""
Live trending-video dashboard.

Queries ClickHouse's trend_scores table (populated by Flink via Kafka) and
auto-refreshes every few seconds. This is the demo centerpiece of the
project: generator -> Kafka -> Flink -> Kafka -> ClickHouse -> this dashboard.

Run with:
    streamlit run dashboard/app.py
"""

import time

import clickhouse_connect
import pandas as pd
import streamlit as st

# --- Config ---
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
REFRESH_SECONDS = 5
LOOKBACK_MINUTES = 10   # how far back to consider "recent" for the leaderboard

st.set_page_config(page_title="Video Trend Dashboard", layout="wide")


@st.cache_resource
def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


def run_query_df(client, query):
    """Run a query and return a DataFrame, handling the 'no data yet' case cleanly."""
    try:
        return client.query_df(query)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


client = get_client()

st.title("Real-Time Video Trend Dashboard")
st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s from ClickHouse `trend_scores` "
           f"(last {LOOKBACK_MINUTES} minutes)")

leaderboard_query = f"""
    SELECT
        video_id,
        max(event_count) AS peak_count,
        max(window_end) AS last_seen
    FROM trend_scores
    WHERE window_start >= now() - INTERVAL {LOOKBACK_MINUTES} MINUTE
    GROUP BY video_id
    ORDER BY peak_count DESC
    LIMIT 10
"""

timeseries_query = f"""
    SELECT video_id, window_start, event_count
    FROM trend_scores
    WHERE video_id IN (
        SELECT video_id FROM trend_scores
        WHERE window_start >= now() - INTERVAL {LOOKBACK_MINUTES} MINUTE
        GROUP BY video_id
        ORDER BY max(event_count) DESC
        LIMIT 5
    )
    AND window_start >= now() - INTERVAL {LOOKBACK_MINUTES} MINUTE
    ORDER BY window_start
"""

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Trending now")
    leaderboard_df = run_query_df(client, leaderboard_query)
    if leaderboard_df.empty:
        st.info("No data yet. Start the generator and Flink job, then wait ~30s.")
    else:
        leaderboard_df.index = range(1, len(leaderboard_df) + 1)
        st.dataframe(leaderboard_df, use_container_width=True)

with col2:
    st.subheader("Event count over time (top 5 videos)")
    ts_df = run_query_df(client, timeseries_query)
    if ts_df.empty:
        st.info("No data yet.")
    else:
        # pivot_table (not pivot) tolerates any accidental duplicate rows
        # for the same (window_start, video_id) pair rather than erroring.
        pivot_df = ts_df.pivot_table(
            index="window_start", columns="video_id", values="event_count", aggfunc="max"
        )
        st.line_chart(pivot_df)

st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")

time.sleep(REFRESH_SECONDS)
st.rerun()