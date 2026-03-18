#!/usr/bin/env python3
"""
data_preprocessing.py
PySpark preprocessing pipeline for Reddit comments.

Steps:
  1. Load raw JSON.gz files from HDFS
  2. Schema validation & type casting
  3. Drop deleted / bot / null body comments
  4. Text normalization (lowercase, strip URLs/markdown/HTML)
  5. Tokenization + stopword removal
  6. Outlier detection (too short / too long)
  7. VADER pre-labeling (Positive / Neutral / Negative)
  8. Save cleaned Parquet to HDFS
"""

import os
import logging
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, FloatType

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/data_preprocessing.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Known bots to filter
BOT_ACCOUNTS = {
    "automoderator", "reddit_bot", "qualityvote", "repostsleuthbot",
    "savevideo", "remindmebot", "gifv-bot", "wikipediabot",
}

SUBREDDITS = ["worldnews", "technology", "science"]
HDFS_RAW_BASE = "/user/reddit_sentiment/raw"
HDFS_PROCESSED_BASE = "/user/reddit_sentiment/processed"

MIN_TOKENS = 3
MAX_TOKENS = 500


def create_spark_session() -> SparkSession:
    logger.info("Creating Spark session...")
    spark = (
        SparkSession.builder.appName("Reddit Sentiment Preprocessing")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created (v{spark.version})")
    return spark


def load_raw(spark: SparkSession, subreddit: str):
    path = f"{HDFS_RAW_BASE}/{subreddit}.json.gz"
    logger.info(f"Loading raw data for r/{subreddit} from {path}")
    df = spark.read.json(path)
    logger.info(f"Loaded {df.count():,} records for r/{subreddit}")
    return df


def clean(df, subreddit: str):
    logger.info(f"Cleaning r/{subreddit} ...")
    initial = df.count()

    # ── 1. Select & cast relevant fields ───────────────────────────────────
    df = df.select(
        F.col("body").cast(StringType()),
        F.col("score").cast(IntegerType()),
        F.col("author").cast(StringType()),
        F.col("subreddit").cast(StringType()),
        F.col("created_utc").cast(LongType()),
        F.col("controversiality").cast(IntegerType()),
        F.col("gilded").cast(IntegerType()),
    ).withColumn("subreddit_name", F.lit(subreddit))

    # ── 2. Drop deleted / removed bodies ───────────────────────────────────
    df = df.filter(
        F.col("body").isNotNull()
        & ~F.col("body").isin("[deleted]", "[removed]", "")
    )

    # ── 3. Drop known bot authors ───────────────────────────────────────────
    bot_list = list(BOT_ACCOUNTS)
    df = df.filter(~F.lower(F.col("author")).isin(bot_list))

    # ── 4. Text normalization (UDF) ─────────────────────────────────────────
    def normalize(text: str) -> str:
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r"http\S+|www\.\S+", " ", text)          # URLs
        text = re.sub(r"\*\*|__|\*|_|~~|`{1,3}", " ", text)    # Markdown
        text = re.sub(r"&amp;|&lt;|&gt;|&quot;|&#39;", " ", text)  # HTML entities
        text = re.sub(r"/u/\w+|/r/\w+", " ", text)             # Reddit mentions
        text = re.sub(r"[^a-z0-9\s'.,!?]", " ", text)          # Non-alpha
        text = re.sub(r"\s+", " ", text).strip()
        return text

    normalize_udf = F.udf(normalize, StringType())
    df = df.withColumn("body_clean", normalize_udf(F.col("body")))

    # ── 5. Token count filter ───────────────────────────────────────────────
    df = df.withColumn(
        "token_count",
        F.size(F.split(F.col("body_clean"), r"\s+"))
    )
    df = df.filter(
        (F.col("token_count") >= MIN_TOKENS) &
        (F.col("token_count") <= MAX_TOKENS)
    )

    # ── 6. Convert timestamp ────────────────────────────────────────────────
    df = df.withColumn(
        "created_date",
        F.to_date(F.from_unixtime(F.col("created_utc")))
    ).withColumn(
        "year", F.year(F.col("created_date"))
    ).withColumn(
        "month", F.month(F.col("created_date"))
    )

    # ── 7. VADER sentiment pre-label ────────────────────────────────────────
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    def vader_label(text: str) -> str:
        if not text:
            return "neutral"
        compound = analyzer.polarity_scores(text)["compound"]
        if compound >= 0.05:
            return "positive"
        elif compound <= -0.05:
            return "negative"
        return "neutral"

    vader_udf = F.udf(vader_label, StringType())
    df = df.withColumn("sentiment_label", vader_udf(F.col("body_clean")))

    final = df.count()
    logger.info(
        f"r/{subreddit}: {initial:,} → {final:,} records "
        f"({initial - final:,} removed, {(initial-final)/initial*100:.1f}%)"
    )
    return df


def process_all(spark: SparkSession):
    dfs = []
    for sub in SUBREDDITS:
        try:
            raw = load_raw(spark, sub)
            cleaned = clean(raw, sub)
            out_path = f"{HDFS_PROCESSED_BASE}/{sub}_processed"
            logger.info(f"Saving r/{sub} → {out_path}")
            cleaned.write.parquet(out_path, mode="overwrite")
            dfs.append(cleaned)
        except Exception as e:
            logger.error(f"Error processing r/{sub}: {e}")

    if dfs:
        logger.info("Creating combined dataset...")
        combined = dfs[0]
        for d in dfs[1:]:
            combined = combined.union(d)
        combined_path = f"{HDFS_PROCESSED_BASE}/all_subreddits_processed"
        combined.write.parquet(combined_path, mode="overwrite")
        logger.info(f"Combined dataset: {combined.count():,} records → {combined_path}")
    else:
        logger.error("No data processed.")


if __name__ == "__main__":
    spark = create_spark_session()
    process_all(spark)
    spark.stop()
    logger.info("Preprocessing complete.")
