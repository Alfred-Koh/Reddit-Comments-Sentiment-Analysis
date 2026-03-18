#!/usr/bin/env python3
"""
verify_pipeline.py
Quick sanity check: verifies local raw files, HDFS uploads, and processed Parquet outputs.
"""

import os
import subprocess
from pyspark.sql import SparkSession

SUBREDDITS = ["worldnews", "technology", "science"]
HDFS_RAW_DIR = "/user/reddit_sentiment/raw"
HDFS_PROCESSED_DIR = "/user/reddit_sentiment/processed"
PROCESSED_DATASETS = [f"{s}_processed" for s in SUBREDDITS] + ["all_subreddits_processed"]


def check_local_files():
    print("\n🔍 Checking local raw files...")
    missing = []
    for sub in SUBREDDITS:
        path = f"data/raw/{sub}.json.gz"
        if not os.path.isfile(path):
            missing.append(path)
    if missing:
        print(f"❌ Missing: {missing}")
    else:
        print("✅ All local raw files found.")


def check_hdfs_raw():
    print("\n🔍 Checking HDFS raw uploads...")
    try:
        output = subprocess.check_output(
            ["hdfs", "dfs", "-ls", HDFS_RAW_DIR], stderr=subprocess.DEVNULL
        ).decode()
        for sub in SUBREDDITS:
            if f"{sub}.json.gz" in output:
                print(f"  ✅ {sub}.json.gz found in HDFS")
            else:
                print(f"  ❌ {sub}.json.gz MISSING from HDFS")
    except subprocess.CalledProcessError as e:
        print(f"❌ Could not access HDFS: {e}")


def check_processed(spark):
    print("\n🔍 Checking processed Parquet outputs in HDFS...")
    for dataset in PROCESSED_DATASETS:
        path = f"hdfs://localhost:9000{HDFS_PROCESSED_DIR}/{dataset}"
        try:
            df = spark.read.parquet(path)
            count = df.count()
            print(f"  ✅ {dataset}: {count:,} records")
        except Exception as e:
            print(f"  ❌ {dataset}: {e}")


def main():
    check_local_files()
    check_hdfs_raw()

    spark = (
        SparkSession.builder.appName("Verify Pipeline")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    check_processed(spark)
    spark.stop()
    print("\n✅ Pipeline verification complete.")


if __name__ == "__main__":
    main()
