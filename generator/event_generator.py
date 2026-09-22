"""
Synthetic event generator for the streaming trend-detection pipeline.

Publishes fake video engagement events (view_start, view_complete, like,
comment, share) to a Kafka topic, with:
  - Zipf-distributed video popularity (a few videos dominate traffic)
  - Two engineered "spike" videos that temporarily go viral partway through
    the run, so downstream Flink/Spark logic has something real to detect

Usage:
    python event_generator.py
    python event_generator.py --duration 300 --rate 15
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from faker import Faker
from kafka import KafkaProducer

import config

fake = Faker()


def build_video_pool():
    """Generate a fixed pool of video IDs with Zipf-weighted popularity."""
    video_ids = [f"video_{i:04d}" for i in range(config.NUM_VIDEOS)]
    # Zipf weights: rank 1 video is most popular, weights decay by rank
    ranks = np.arange(1, config.NUM_VIDEOS + 1)
    weights = 1.0 / np.power(ranks, config.ZIPF_PARAM)
    weights = weights / weights.sum()
    return video_ids, weights


def build_user_pool():
    """Generate a fixed pool of fake user IDs, reused across events."""
    return [str(uuid.uuid4()) for _ in range(config.NUM_USERS)]


def get_current_weights(base_weights, elapsed_seconds):
    """
    Return popularity weights adjusted for any active spike.
    Spike videos get a temporary multiplier during their configured window.
    """
    weights = base_weights.copy()
    spike_active = config.SPIKE_START_SECOND <= elapsed_seconds <= config.SPIKE_END_SECOND
    if spike_active:
        for idx in config.SPIKE_VIDEO_INDICES:
            weights[idx] *= config.SPIKE_WEIGHT_MULTIPLIER
        weights = weights / weights.sum()
    return weights, spike_active


def pick_event_type():
    types = list(config.EVENT_TYPE_WEIGHTS.keys())
    weights = np.array(list(config.EVENT_TYPE_WEIGHTS.values()), dtype=float)
    weights = weights / weights.sum()
    return np.random.choice(types, p=weights)


def build_event(video_id, user_id, event_type):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "video_id": video_id,
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if event_type == "view_complete":
        event["watch_duration_ms"] = int(
            np.random.uniform(config.MIN_WATCH_DURATION_MS, config.MAX_WATCH_DURATION_MS)
        )
    return event


def run(duration_seconds, events_per_second):
    video_ids, base_weights = build_video_pool()
    user_ids = build_user_pool()

    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Starting event generator: {duration_seconds}s run at ~{events_per_second} events/sec")
    print(f"Spike videos: {[video_ids[i] for i in config.SPIKE_VIDEO_INDICES]} "
          f"active from {config.SPIKE_START_SECOND}s to {config.SPIKE_END_SECOND}s")

    start_time = time.time()
    event_count = 0
    spike_was_active = False

    while True:
        elapsed = time.time() - start_time
        if elapsed >= duration_seconds:
            break

        weights, spike_active = get_current_weights(base_weights, elapsed)

        # Log spike transitions so you can cross-reference the console output
        # against Kafka UI / your dashboard later.
        if spike_active and not spike_was_active:
            print(f"[{elapsed:6.1f}s] SPIKE STARTED")
        if spike_was_active and not spike_active:
            print(f"[{elapsed:6.1f}s] spike ended")
        spike_was_active = spike_active

        video_id = np.random.choice(video_ids, p=weights)
        user_id = np.random.choice(user_ids)
        event_type = pick_event_type()
        event = build_event(video_id, user_id, event_type)

        # Key by video_id so all events for the same video land on the same
        # Kafka partition and stay in order for downstream windowed aggregation.
        producer.send(config.TOPIC_NAME, key=video_id, value=event)
        event_count += 1

        if event_count % 100 == 0:
            print(f"[{elapsed:6.1f}s] {event_count} events sent")

        # Poisson-distributed inter-arrival time for bursty-but-realistic timing
        sleep_time = np.random.exponential(1.0 / events_per_second)
        time.sleep(sleep_time)

    producer.flush()
    producer.close()
    print(f"Done. {event_count} events sent over {duration_seconds}s.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic video-engagement events into Kafka.")
    parser.add_argument("--duration", type=int, default=config.DEFAULT_RUN_DURATION_SECONDS,
                         help="Run duration in seconds")
    parser.add_argument("--rate", type=float, default=config.BASE_EVENTS_PER_SECOND,
                         help="Average events per second")
    args = parser.parse_args()

    run(duration_seconds=args.duration, events_per_second=args.rate)