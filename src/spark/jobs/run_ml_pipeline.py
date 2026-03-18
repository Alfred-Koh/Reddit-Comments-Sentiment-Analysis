#!/usr/bin/env python3
"""
run_ml_pipeline.py
Spark MLlib sentiment classification pipeline.

Phase 1: TF-IDF + Logistic Regression (baseline)
Phase 2: Word2Vec + Gradient Boosted Trees

Both phases use subreddit-stratified train/test splits to account for the
class imbalance and subreddit-specific vocabulary identified in EDA.
"""

import os
import sys
import json
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import create_spark_session
from utils.data_loader import load_all

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    Word2Vec, StringIndexer, IndexToString
)
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/ml_pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

RESULTS_DIR = "data/spark_results/ml"
HDFS_MODEL_BASE = "/user/reddit_sentiment/models"


def stratified_split(df, label_col: str, train_ratio: float = 0.8):
    """Subreddit-stratified train / test split."""
    train_parts, test_parts = [], []
    for sub in df.select("subreddit_name").distinct().collect():
        sub_df = df.filter(F.col("subreddit_name") == sub["subreddit_name"])
        tr, te = sub_df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
        train_parts.append(tr)
        test_parts.append(te)

    train = train_parts[0]
    test = test_parts[0]
    for tr, te in zip(train_parts[1:], test_parts[1:]):
        train = train.union(tr)
        test = test.union(te)
    return train, test


def compute_class_weights(df, label_col: str = "sentiment_label") -> dict:
    """Compute inverse-frequency class weights to handle imbalance."""
    total = df.count()
    dist = {
        row[label_col]: row["count"]
        for row in df.groupBy(label_col).count().collect()
    }
    weights = {k: total / (len(dist) * v) for k, v in dist.items()}
    logger.info(f"Class weights: {weights}")
    return weights


def evaluate(predictions, label: str = "label") -> dict:
    evaluator = MulticlassClassificationEvaluator(
        labelCol=label, predictionCol="prediction"
    )
    metrics = {}
    for metric in ("f1", "accuracy", "weightedPrecision", "weightedRecall"):
        evaluator.setMetricName(metric)
        metrics[metric] = evaluator.evaluate(predictions)
    return metrics


def save_metrics(metrics: dict, model_name: str):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{model_name}_metrics.json")
    with open(path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Metrics saved → {path}")
    for k, v in metrics.items():
        logger.info(f"  {k:25}: {v:.4f}")


# ── Phase 1: TF-IDF + Logistic Regression ───────────────────────────────────

def run_phase1(df):
    logger.info("\n=== PHASE 1: TF-IDF + Logistic Regression ===")

    # Label encoding
    indexer = StringIndexer(inputCol="sentiment_label", outputCol="label", handleInvalid="skip")
    tokenizer = Tokenizer(inputCol="body_clean", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=50_000)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=5)

    # Class weights from EDA: positive ~29%, neutral ~33%, negative ~37%
    lr = LogisticRegression(
        labelCol="label",
        featuresCol="features",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.0,
        weightCol=None,   # Add class weights if needed via a weight column
    )

    pipeline = Pipeline(stages=[indexer, tokenizer, remover, hashing_tf, idf, lr])

    train, test = stratified_split(df, "sentiment_label")
    logger.info(f"Train: {train.count():,}  Test: {test.count():,}")

    logger.info("Fitting TF-IDF + LogReg pipeline...")
    start = time.time()
    model = pipeline.fit(train)
    logger.info(f"Training complete in {time.time()-start:.1f}s")

    predictions = model.transform(test)
    metrics = evaluate(predictions)
    save_metrics(metrics, "phase1_tfidf_logreg")

    # Save model to HDFS
    model_path = f"{HDFS_MODEL_BASE}/phase1_tfidf_logreg"
    model.write().overwrite().save(model_path)
    logger.info(f"Model saved → {model_path}")

    return model, metrics


# ── Phase 2: Word2Vec + Gradient Boosted Trees ───────────────────────────────

def run_phase2(df):
    logger.info("\n=== PHASE 2: Word2Vec + Gradient Boosted Trees ===")

    indexer = StringIndexer(inputCol="sentiment_label", outputCol="label", handleInvalid="skip")
    tokenizer = Tokenizer(inputCol="body_clean", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    w2v = Word2Vec(
        inputCol="filtered_words",
        outputCol="features",
        vectorSize=100,
        minCount=5,
        numPartitions=10,
        seed=42,
    )
    gbt = GBTClassifier(
        labelCol="label",
        featuresCol="features",
        maxIter=50,
        maxDepth=5,
        seed=42,
    )

    pipeline = Pipeline(stages=[indexer, tokenizer, remover, w2v, gbt])

    train, test = stratified_split(df, "sentiment_label")
    logger.info(f"Train: {train.count():,}  Test: {test.count():,}")

    logger.info("Fitting Word2Vec + GBT pipeline...")
    start = time.time()
    model = pipeline.fit(train)
    logger.info(f"Training complete in {time.time()-start:.1f}s")

    predictions = model.transform(test)
    metrics = evaluate(predictions)
    save_metrics(metrics, "phase2_word2vec_gbt")

    # Save model to HDFS
    model_path = f"{HDFS_MODEL_BASE}/phase2_word2vec_gbt"
    model.write().overwrite().save(model_path)
    logger.info(f"Model saved → {model_path}")

    return model, metrics


def run_ml_pipeline():
    start_total = time.time()
    logger.info("=== Starting ML Pipeline ===")

    spark = create_spark_session(app_name="Reddit Sentiment ML Pipeline")

    data = load_all(spark)
    if "all_subreddits" not in data:
        logger.error("Combined dataset not found.")
        return 1

    df = data["all_subreddits"].cache()
    logger.info(f"Dataset loaded: {df.count():,} records")

    # Phase 1
    _, metrics1 = run_phase1(df)

    # Phase 2
    _, metrics2 = run_phase2(df)

    # Comparison
    logger.info("\n=== Model Comparison ===")
    logger.info(f"{'Metric':<25} {'TF-IDF+LR':>12} {'W2V+GBT':>12}")
    for k in metrics1:
        logger.info(f"  {k:<23} {metrics1[k]:>12.4f} {metrics2[k]:>12.4f}")

    elapsed = time.time() - start_total
    logger.info(f"\n=== ML Pipeline complete in {elapsed:.1f}s ===")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(run_ml_pipeline())
