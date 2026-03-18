#!/usr/bin/env python3
"""
data_validation.py
Validate cleaned Parquet datasets in HDFS using PySpark.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/data_validation.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

HDFS_PROCESSED_BASE = "/user/reddit_sentiment/processed"
CATEGORIES = ["worldnews", "technology", "science", "all_subreddits"]


def create_spark_session():
    return (
        SparkSession.builder.appName("Reddit Data Validation")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def load_processed(spark, category):
    path = f"{HDFS_PROCESSED_BASE}/{category}_processed"
    logger.info(f"Loading {path}")
    try:
        df = spark.read.parquet(path)
        logger.info(f"Loaded {df.count():,} records for {category}")
        return df
    except Exception as e:
        logger.error(f"Error loading {category}: {e}")
        return None


def validate(df, category):
    if df is None:
        return {"status": "FAILED", "reason": "No data"}

    total = df.count()
    logger.info(f"\n{'='*50}\nValidating: {category} ({total:,} records)\n{'='*50}")

    # Missing values
    logger.info("Missing values per column:")
    for col in df.columns:
        n_null = df.filter(F.col(col).isNull()).count()
        pct = n_null / total * 100
        logger.info(f"  {col}: {n_null:,} ({pct:.2f}%)")

    # Sentiment distribution
    logger.info("Sentiment distribution:")
    df.groupBy("sentiment_label").count().orderBy("count", ascending=False).show()

    # Score stats
    score_stats = df.select(
        F.min("score").alias("min_score"),
        F.max("score").alias("max_score"),
        F.avg("score").alias("avg_score"),
    ).collect()[0]
    logger.info(
        f"Score — min: {score_stats['min_score']}, "
        f"max: {score_stats['max_score']}, "
        f"avg: {score_stats['avg_score']:.2f}"
    )

    # Token length
    length_stats = df.select(
        F.min("token_count").alias("min_len"),
        F.max("token_count").alias("max_len"),
        F.avg("token_count").alias("avg_len"),
    ).collect()[0]
    logger.info(
        f"Token count — min: {length_stats['min_len']}, "
        f"max: {length_stats['max_len']}, "
        f"avg: {length_stats['avg_len']:.1f}"
    )

    # Year distribution
    logger.info("Year distribution:")
    df.groupBy("year").count().orderBy("year").show()

    status = "PASSED" if total > 0 else "FAILED"
    logger.info(f"Validation status for {category}: {status}")
    return {"status": status, "total_records": total}


def validate_all():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    results = {}
    for cat in CATEGORIES:
        df = load_processed(spark, cat)
        results[cat] = validate(df, cat)

    passed = sum(1 for r in results.values() if r["status"] == "PASSED")
    logger.info(f"\nSummary: {passed}/{len(results)} datasets passed validation.")
    spark.stop()
    return results


if __name__ == "__main__":
    validate_all()
