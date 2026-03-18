#!/usr/bin/env python3
"""
visualization_helper.py
Matplotlib / Seaborn helpers to produce charts from Spark DataFrames.
All functions save PNG files to a specified output directory.
"""

import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import numpy as np

logger = logging.getLogger(__name__)
sns.set_theme(style="whitegrid", palette="muted")

# Reddit orange palette
PALETTE = {
    "positive": "#46D160",
    "neutral":  "#F59E0B",
    "negative": "#FF4500",
    "primary":  "#FF4500",
    "secondary":"#0DD3BB",
}


def _to_pandas(spark_df) -> pd.DataFrame:
    return spark_df.toPandas()


def save_sentiment_distribution(df, output_dir: str):
    """Pie chart of Positive / Neutral / Negative distribution."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = _to_pandas(
            df.groupBy("sentiment_label").count().orderBy("count", ascending=False)
        )
        colors = [PALETTE.get(l, "#999") for l in pdf["sentiment_label"]]
        fig, ax = plt.subplots(figsize=(7, 7))
        wedges, texts, autotexts = ax.pie(
            pdf["count"],
            labels=pdf["sentiment_label"].str.capitalize(),
            autopct="%1.1f%%",
            colors=colors,
            startangle=140,
            textprops={"fontsize": 13},
        )
        ax.set_title("Sentiment Distribution (VADER pre-label)", fontsize=15, pad=20)
        plt.tight_layout()
        path = os.path.join(output_dir, "sentiment_distribution.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_sentiment_distribution: {e}")


def save_comment_volume_by_subreddit(df, output_dir: str):
    """Horizontal bar chart of comment counts per subreddit."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = _to_pandas(
            df.groupBy("subreddit_name")
            .count()
            .orderBy("count", ascending=False)
        )
        fig, ax = plt.subplots(figsize=(9, 5))
        bars = ax.barh(pdf["subreddit_name"], pdf["count"] / 1e6,
                       color=PALETTE["primary"], edgecolor="white")
        ax.set_xlabel("Comments (Millions)", fontsize=12)
        ax.set_title("Comment Volume by Subreddit", fontsize=14)
        for bar, val in zip(bars, pdf["count"]):
            ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                    f"{val/1e6:.1f}M", va="center", fontsize=10)
        plt.tight_layout()
        path = os.path.join(output_dir, "volume_by_subreddit.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_comment_volume_by_subreddit: {e}")


def save_yearly_trend(df, output_dir: str):
    """Line chart of comment volume per year."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = _to_pandas(
            df.groupBy("year").count().orderBy("year").filter("year >= 2015")
        )
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(pdf["year"], pdf["count"] / 1e6, marker="o",
                color=PALETTE["primary"], linewidth=2.5, markersize=7)
        ax.fill_between(pdf["year"], pdf["count"] / 1e6,
                        alpha=0.15, color=PALETTE["primary"])
        ax.set_xlabel("Year", fontsize=12)
        ax.set_ylabel("Comments (Millions)", fontsize=12)
        ax.set_title("Annual Comment Volume (2015–2020)", fontsize=14)
        ax.set_xticks(pdf["year"])
        plt.tight_layout()
        path = os.path.join(output_dir, "yearly_trend.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_yearly_trend: {e}")


def save_controversiality_by_subreddit(df, output_dir: str):
    """Bar chart of controversiality rate (%) per subreddit."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        from pyspark.sql import functions as F
        pdf = _to_pandas(
            df.groupBy("subreddit_name")
            .agg(
                F.avg("controversiality").alias("controversy_rate"),
                F.count("*").alias("total"),
            )
            .orderBy("controversy_rate", ascending=False)
        )
        pdf["controversy_pct"] = pdf["controversy_rate"] * 100

        fig, ax = plt.subplots(figsize=(9, 5))
        bars = ax.bar(pdf["subreddit_name"], pdf["controversy_pct"],
                      color=[PALETTE["negative"], PALETTE["primary"], PALETTE["secondary"]])
        ax.set_ylabel("Controversiality Rate (%)", fontsize=12)
        ax.set_title("Controversiality Rate by Subreddit", fontsize=14)
        for bar, val in zip(bars, pdf["controversy_pct"]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                    f"{val:.1f}%", ha="center", fontsize=11)
        plt.tight_layout()
        path = os.path.join(output_dir, "controversiality_by_subreddit.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_controversiality_by_subreddit: {e}")


def save_score_distribution(df, output_dir: str):
    """Histogram of comment scores (log scale)."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = _to_pandas(df.select("score").filter("score > 0").limit(500_000))
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.hist(pdf["score"], bins=80, color=PALETTE["primary"],
                edgecolor="white", log=True)
        ax.set_xlabel("Comment Score (upvotes)", fontsize=12)
        ax.set_ylabel("Count (log scale)", fontsize=12)
        ax.set_title("Distribution of Comment Scores", fontsize=14)
        plt.tight_layout()
        path = os.path.join(output_dir, "score_distribution.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_score_distribution: {e}")


def save_sentiment_over_time(df, output_dir: str):
    """Stacked area chart of sentiment proportions per year."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        from pyspark.sql import functions as F
        pdf = _to_pandas(
            df.groupBy("year", "sentiment_label")
            .count()
            .orderBy("year")
            .filter("year >= 2015")
        )
        pivot = pdf.pivot(index="year", columns="sentiment_label", values="count").fillna(0)
        pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

        fig, ax = plt.subplots(figsize=(10, 5))
        pivot_pct.plot.area(
            ax=ax,
            color=[PALETTE["negative"], PALETTE["neutral"], PALETTE["positive"]],
            alpha=0.85,
        )
        ax.set_xlabel("Year", fontsize=12)
        ax.set_ylabel("Share of Comments (%)", fontsize=12)
        ax.set_title("Sentiment Composition Over Time", fontsize=14)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax.legend(title="Sentiment", loc="upper left")
        plt.tight_layout()
        path = os.path.join(output_dir, "sentiment_over_time.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}")
    except Exception as e:
        logger.error(f"Error in save_sentiment_over_time: {e}")


def save_length_vs_score_correlation(df, output_dir: str):
    """Scatter plot (sampled) of token_count vs score with correlation annotation."""
    os.makedirs(output_dir, exist_ok=True)
    try:
        pdf = _to_pandas(
            df.select("token_count", "score")
            .filter("score > 0 AND token_count > 0")
            .limit(50_000)
        )
        corr = pdf["token_count"].corr(pdf["score"])
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(pdf["token_count"], pdf["score"], alpha=0.05,
                   color=PALETTE["primary"], s=5)
        ax.set_xlabel("Comment Length (tokens)", fontsize=12)
        ax.set_ylabel("Score (upvotes)", fontsize=12)
        ax.set_title(
            f"Comment Length vs. Score  (r = {corr:.3f})", fontsize=14
        )
        ax.set_yscale("log")
        plt.tight_layout()
        path = os.path.join(output_dir, "length_vs_score.png")
        plt.savefig(path, dpi=150)
        plt.close()
        logger.info(f"Saved: {path}  (correlation = {corr:.3f})")
        # Also save correlation value as text
        with open(os.path.join(output_dir, "correlation.txt"), "w") as f:
            f.write(f"Pearson correlation (token_count vs score): {corr:.6f}\n")
    except Exception as e:
        logger.error(f"Error in save_length_vs_score_correlation: {e}")
