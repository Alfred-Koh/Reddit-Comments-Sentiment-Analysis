#!/usr/bin/env python3
"""
kafka_consumer.py
Spark Structured Streaming consumer: reads Reddit comments from Kafka,
applies VADER sentiment scoring in real-time, and writes results to HDFS.

Usage:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        src/streaming/kafka_consumer.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/kafka_consumer.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
SUBREDDITS = ["worldnews", "technology", "science"]
TOPICS = ",".join(f"reddit-{s}" for s in SUBREDDITS)
HDFS_STREAM_OUTPUT = "/user/reddit_sentiment/streaming_output"
CHECKPOINT_DIR = "/user/reddit_sentiment/checkpoints/streaming"

# Schema matching Pushshift comment fields we care about
COMMENT_SCHEMA = StructType([
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("controversiality", IntegerType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Reddit Kafka Structured Streaming")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def get_sentiment_udf():
    """Return a Spark UDF that runs VADER on comment body text."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    def score(text: str) -> str:
        if not text or text in ("[deleted]", "[removed]"):
            return "neutral"
        compound = analyzer.polarity_scores(text)["compound"]
        if compound >= 0.05:
            return "positive"
        elif compound <= -0.05:
            return "negative"
        return "neutral"

    return F.udf(score, StringType())


def run_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ──────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ───────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.col("topic"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"), COMMENT_SCHEMA
            ).alias("data"),
        )
        .select("topic", "kafka_timestamp", "data.*")
    )

    # ── Filter junk ──────────────────────────────────────────────────────────
    cleaned = (
        parsed
        .filter(F.col("body").isNotNull())
        .filter(~F.col("body").isin("[deleted]", "[removed]", ""))
        .filter(F.col("author") != "AutoModerator")
    )

    # ── Apply VADER sentiment ────────────────────────────────────────────────
    sentiment_udf = get_sentiment_udf()
    enriched = cleaned.withColumn("sentiment", sentiment_udf(F.col("body")))

    # ── Write to HDFS (Parquet, partitioned by subreddit + date) ─────────────
    query = (
        enriched.writeStream
        .format("parquet")
        .option("path", HDFS_STREAM_OUTPUT)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .partitionBy("subreddit")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    logger.info(f"Streaming query started. Reading topics: {TOPICS}")
    logger.info(f"Writing to: {HDFS_STREAM_OUTPUT}")
    query.awaitTermination()


if __name__ == "__main__":
    run_stream()
