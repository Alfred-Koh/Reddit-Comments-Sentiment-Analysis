#!/usr/bin/env python3
"""
run_analysis.py
Main Spark EDA job:
  - Loads processed Reddit comment data from HDFS
  - Computes sentiment distribution, volume trends, controversiality, bias
  - Saves statistical summaries as text and PNGs
"""

import os
import sys
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session
from utils.data_loader import load_all, enrich_for_analysis
from utils.visualization_helper import (
    save_sentiment_distribution,
    save_comment_volume_by_subreddit,
    save_yearly_trend,
    save_controversiality_by_subreddit,
    save_score_distribution,
    save_sentiment_over_time,
    save_length_vs_score_correlation,
)

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/spark_analysis.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

OUTPUT_BASE = "data/spark_results"


def save_basic_stats(df, category: str, output_dir: str):
    """Compute and save basic statistics to a text file."""
    from pyspark.sql import functions as F
    os.makedirs(output_dir, exist_ok=True)

    total = df.count()
    sentiment_dist = {
        row["sentiment_label"]: row["count"]
        for row in df.groupBy("sentiment_label").count().collect()
    }
    score_stats = df.select(
        F.min("score").alias("min"),
        F.max("score").alias("max"),
        F.avg("score").alias("mean"),
        F.expr("percentile_approx(score, 0.5)").alias("median"),
        F.stddev("score").alias("stddev"),
    ).collect()[0]

    token_stats = df.select(
        F.min("token_count").alias("min"),
        F.max("token_count").alias("max"),
        F.avg("token_count").alias("mean"),
    ).collect()[0]

    lines = [
        f"=== Basic Statistics: {category} ===",
        f"Total Comments    : {total:,}",
        f"Sentiment Distribution:",
        *[f"  {k.capitalize():10}: {v:,} ({v/total*100:.1f}%)"
          for k, v in sentiment_dist.items()],
        f"Score  — min:{score_stats['min']:>8}  max:{score_stats['max']:>8}"
        f"  mean:{score_stats['mean']:>8.2f}  median:{score_stats['median']:>6}"
        f"  stddev:{score_stats['stddev']:>8.2f}",
        f"Tokens — min:{token_stats['min']:>5}  max:{token_stats['max']:>5}"
        f"  mean:{token_stats['mean']:>7.1f}",
    ]

    summary_path = os.path.join(output_dir, "basic_stats_summary.txt")
    with open(summary_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"Saved stats → {summary_path}")


def run_analysis():
    start = time.time()
    logger.info("=== Starting Spark EDA Analysis ===")

    spark = create_spark_session(app_name="Reddit EDA Analysis")

    data = load_all(spark)
    if not data:
        logger.error("No data loaded. Exiting.")
        return 1

    for category, df in data.items():
        logger.info(f"\n{'='*50}\nProcessing: {category}\n{'='*50}")
        df = enrich_for_analysis(df)
        df.cache()

        out_dir = os.path.join(OUTPUT_BASE, category)
        stats_dir = os.path.join(out_dir, "stats")
        viz_dir = os.path.join(out_dir, "visualizations")

        os.makedirs(stats_dir, exist_ok=True)
        os.makedirs(viz_dir, exist_ok=True)

        # Stats
        save_basic_stats(df, category, stats_dir)

        # Visualizations
        save_sentiment_distribution(df, viz_dir)
        save_comment_volume_by_subreddit(df, viz_dir)
        save_yearly_trend(df, viz_dir)
        save_controversiality_by_subreddit(df, viz_dir)
        save_score_distribution(df, viz_dir)
        save_sentiment_over_time(df, viz_dir)
        save_length_vs_score_correlation(df, os.path.join(out_dir, "bias"))

        df.unpersist()
        logger.info(f"Completed: {category}")

    elapsed = time.time() - start
    logger.info(f"=== EDA complete in {elapsed:.1f}s ===")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(run_analysis())
