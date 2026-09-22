"""
Config for the synthetic event generator.
Tune these values to change how "realistic" or dramatic your test run looks.
"""

# --- Kafka connection ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "video-events"

# --- Video / user pool ---
NUM_VIDEOS = 50          # total distinct videos in the simulation
NUM_USERS = 500          # total distinct users who can generate events

# --- Popularity distribution ---
# Zipf's law: a small number of videos get most of the traffic, mimicking
# real-world content popularity (a few viral videos, a long tail of unpopular ones).
# Higher ZIPF_PARAM = more skewed toward a few dominant videos. 1.5-2.5 is realistic.
ZIPF_PARAM = 2.0

# --- Event type mix ---
# Every event starts as a view; only a fraction of viewers also like/share/comment.
# Weights are relative probabilities, not percentages -- they get normalized.
EVENT_TYPE_WEIGHTS = {
    "view_start": 45,
    "view_complete": 30,
    "like": 15,
    "comment": 6,
    "share": 4,
}

# --- Watch duration simulation (milliseconds) ---
# Used only for view_complete events, to simulate how long someone actually watched.
MIN_WATCH_DURATION_MS = 1_000
MAX_WATCH_DURATION_MS = 180_000

# --- Spike simulation ---
# These video indices (0-based, into the video pool) get a temporary popularity
# boost during a specific window of the run -- this is your engineered "going viral"
# moment that Flink should detect quickly and Spark should later confirm.
SPIKE_VIDEO_INDICES = [3, 17]        # which videos spike
SPIKE_START_SECOND = 120              # spike begins 2 minutes into the run
SPIKE_END_SECOND = 300                # spike ends at 5 minutes
SPIKE_WEIGHT_MULTIPLIER = 25          # how much more likely a spiking video is picked

# --- Event rate ---
# Average events per second, using Poisson-distributed inter-arrival times
# to mimic realistic (bursty but not perfectly uniform) traffic.
BASE_EVENTS_PER_SECOND = 8

# --- Run duration ---
DEFAULT_RUN_DURATION_SECONDS = 600    # 10 minutes by default