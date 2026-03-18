#!/usr/bin/env python3
"""
kafka_producer.py
Simulates a real-time Reddit comment stream by reading the archived
JSON.gz files and publishing records to Kafka topics (one per subreddit).

Usage:
    python src/streaming/kafka_producer.py \
        --bootstrap-servers localhost:9092 \
        --subreddits worldnews technology science \
        --delay 0.01
"""

import os
import gzip
import json
import time
import logging
import argparse
from kafka import KafkaProducer

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/kafka_producer.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=16384,
    )


def stream_subreddit(
    producer: KafkaProducer,
    subreddit: str,
    data_dir: str,
    delay: float,
):
    """Stream comments from a subreddit JSON.gz file to its Kafka topic."""
    file_path = os.path.join(data_dir, f"{subreddit}.json.gz")
    topic = f"reddit-{subreddit}"

    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}. Skipping r/{subreddit}.")
        return

    logger.info(f"Streaming r/{subreddit} → Kafka topic '{topic}'")
    sent = 0
    errors = 0

    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                # Use author as message key for ordering guarantees per author
                key = record.get("author", "unknown")
                producer.send(topic, key=key, value=record)
                sent += 1

                if sent % 10_000 == 0:
                    producer.flush()
                    logger.info(f"  r/{subreddit}: {sent:,} messages sent")

                if delay > 0:
                    time.sleep(delay)

            except (json.JSONDecodeError, Exception) as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"  Error on record: {e}")

    producer.flush()
    logger.info(
        f"r/{subreddit} complete — {sent:,} sent, {errors} errors"
    )


def main():
    parser = argparse.ArgumentParser(description="Reddit Kafka producer")
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--subreddits", nargs="+",
        default=["worldnews", "technology", "science"]
    )
    parser.add_argument(
        "--data-dir", default="data/raw",
        help="Directory containing subreddit JSON.gz files"
    )
    parser.add_argument(
        "--delay", type=float, default=0.0,
        help="Delay between messages in seconds (0 = as fast as possible)"
    )
    args = parser.parse_args()

    logger.info(f"Connecting to Kafka at {args.bootstrap_servers}")
    producer = create_producer(args.bootstrap_servers)

    try:
        for sub in args.subreddits:
            stream_subreddit(producer, sub, args.data_dir, args.delay)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
