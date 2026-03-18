#!/usr/bin/env python3
"""spark_optimizer.py — Spark performance tuning utilities."""

import time
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger(__name__)


def optimize_session(spark: SparkSession) -> SparkSession:
    """Apply runtime optimisation configs to an existing session."""
    configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewedJoin.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.files.maxPartitionBytes": "134217728",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
    }
    for k, v in configs.items():
        spark.conf.set(k, v)
    logger.info("Applied Spark optimisation configs.")
    return spark


def cache_df(df: DataFrame, storage_level=StorageLevel.MEMORY_AND_DISK) -> DataFrame:
    """Persist a DataFrame at the specified storage level."""
    df = df.persist(storage_level)
    logger.info(f"DataFrame cached at {storage_level}")
    return df


def measure(func, *args, **kwargs):
    """
    Measure wall-clock time of a function call.

    Returns:
        (result, elapsed_seconds)
    """
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    logger.info(f"{func.__name__} completed in {elapsed:.2f}s")
    return result, elapsed


def repartition_for_write(df: DataFrame, target_partitions: int = 20) -> DataFrame:
    """
    Coalesce or repartition a DataFrame before writing to avoid
    creating too many small Parquet files.
    """
    current = df.rdd.getNumPartitions()
    if current > target_partitions * 2:
        logger.info(f"Coalescing {current} → {target_partitions} partitions")
        return df.coalesce(target_partitions)
    elif current < target_partitions // 2:
        logger.info(f"Repartitioning {current} → {target_partitions} partitions")
        return df.repartition(target_partitions)
    return df
