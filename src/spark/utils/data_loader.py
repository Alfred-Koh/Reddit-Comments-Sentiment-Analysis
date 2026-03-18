#!/usr/bin/env python3
"""data_loader.py — Load processed Reddit data from HDFS into Spark DataFrames."""

import logging
import os
import subprocess
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

logger = logging.getLogger(__name__)

HDFS_PROCESSED_BASE = "/user/reddit_sentiment/processed"
SUBREDDITS = ["worldnews", "technology", "science"]


def load_subreddit(spark: SparkSession, subreddit: str) -> DataFrame | None:
    """Load a processed subreddit Parquet dataset from HDFS."""
    path = f"{HDFS_PROCESSED_BASE}/{subreddit}_processed"
    logger.info(f"Loading r/{subreddit} from {path}")
    try:
        df = spark.read.parquet(path)
        count = df.count()
        logger.info(f"  r/{subreddit}: {count:,} records")
        return df
    except Exception as e:
        logger.error(f"  Failed to load r/{subreddit}: {e}")
        return None


def load_all(
    spark: SparkSession,
    subreddits: list[str] | None = None,
) -> dict[str, DataFrame]:
    """
    Load all (or specified) subreddit datasets from HDFS.

    Returns:
        dict mapping subreddit name → DataFrame
    """
    if subreddits is None:
        subreddits = SUBREDDITS

    dfs = {}
    for sub in subreddits:
        df = load_subreddit(spark, sub)
        if df is not None:
            dfs[sub] = df

    # Also try the combined dataset
    combined_path = f"{HDFS_PROCESSED_BASE}/all_subreddits_processed"
    try:
        combined = spark.read.parquet(combined_path)
        dfs["all_subreddits"] = combined
        logger.info(f"Loaded combined dataset: {combined.count():,} records")
    except Exception as e:
        logger.warning(f"Combined dataset not found: {e}")
        if dfs:
            logger.info("Building combined from individual DataFrames...")
            combined = list(dfs.values())[0]
            for d in list(dfs.values())[1:]:
                combined = combined.union(d)
            dfs["all_subreddits"] = combined

    return dfs


def enrich_for_analysis(df: DataFrame) -> DataFrame:
    """
    Add derived columns useful for EDA and ML.
      - comment_length: character count of raw body
      - token_count_approx: approximate word count of cleaned body
      - year / month (if not already present)
    """
    if "comment_length" not in df.columns:
        df = df.withColumn("comment_length", F.length(F.col("body")))

    if "year" not in df.columns:
        df = df.withColumn(
            "year", F.year(F.to_date(F.from_unixtime(F.col("created_utc"))))
        )
    if "month" not in df.columns:
        df = df.withColumn(
            "month", F.month(F.to_date(F.from_unixtime(F.col("created_utc"))))
        )

    logger.info("Enrichment columns added.")
    return df
