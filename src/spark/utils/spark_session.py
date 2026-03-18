#!/usr/bin/env python3
"""spark_session.py — Optimized SparkSession factory."""

import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str = "Reddit Sentiment Analysis",
    master: str = "local[*]",
    optimize: bool = True,
) -> SparkSession:
    """
    Create and return an optimized Spark session.

    Args:
        app_name:  Application name shown in Spark UI.
        master:    Spark master URL (use 'yarn' on cluster).
        optimize:  Apply performance tuning configs.

    Returns:
        SparkSession
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    if optimize:
        builder = (
            builder
            # Adaptive query execution
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            .config("spark.sql.adaptive.skewedJoin.enabled", "true")
            # Memory
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            # Shuffle
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.default.parallelism", "20")
            # I/O
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
            # Dynamic allocation
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "1")
            .config("spark.dynamicAllocation.maxExecutors", "4")
            # Stability
            .config("spark.network.timeout", "800s")
            .config("spark.executor.heartbeatInterval", "60s")
            # Columnar storage
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
            .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: v{spark.version}, app={app_name}")
    return spark
