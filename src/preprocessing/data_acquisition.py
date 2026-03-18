#!/usr/bin/env python3
"""
data_acquisition.py
Download and decompress Pushshift Reddit comment archives by subreddit and year.

Usage:
    python src/preprocessing/data_acquisition.py \
        --subreddits worldnews technology science \
        --years 2015 2016 2017 2018 2019 2020 \
        --output data/raw/
"""

import os
import sys
import json
import gzip
import argparse
import logging
import zstandard as zstd
import urllib.request
from pathlib import Path
from datetime import datetime

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/data_acquisition.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Pushshift base URL for monthly comment dumps
PUSHSHIFT_BASE = "https://files.pushshift.io/reddit/comments"


def download_month(year: int, month: int, output_dir: str) -> str | None:
    """Download a single month's comment dump (.zst) from Pushshift."""
    filename = f"RC_{year}-{month:02d}.zst"
    url = f"{PUSHSHIFT_BASE}/{filename}"
    local_path = os.path.join(output_dir, filename)

    if os.path.exists(local_path):
        logger.info(f"Already downloaded: {local_path}")
        return local_path

    logger.info(f"Downloading {url} ...")
    try:
        urllib.request.urlretrieve(url, local_path)
        size_mb = Path(local_path).stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded {filename} ({size_mb:.1f} MB)")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")
        return None


def filter_by_subreddit(
    zst_path: str, subreddits: list[str], output_dir: str
) -> dict[str, str]:
    """
    Decompress a .zst dump and split comments by subreddit into separate
    newline-delimited JSON.gz files.

    Returns:
        dict mapping subreddit -> output file path
    """
    subreddit_set = {s.lower() for s in subreddits}
    writers: dict[str, gzip.GzipFile] = {}
    counts: dict[str, int] = {s: 0 for s in subreddit_set}

    out_paths: dict[str, str] = {}
    for sub in subreddit_set:
        out_path = os.path.join(output_dir, f"{sub}.json.gz")
        out_paths[sub] = out_path
        writers[sub] = gzip.open(out_path, "at", encoding="utf-8")

    logger.info(f"Filtering {zst_path} for subreddits: {subreddits}")

    dctx = zstd.ZstdDecompressor()
    total = 0
    try:
        with open(zst_path, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                buffer = b""
                while True:
                    chunk = reader.read(65536)
                    if not chunk:
                        break
                    buffer += chunk
                    lines = buffer.split(b"\n")
                    buffer = lines[-1]  # keep incomplete last line
                    for raw_line in lines[:-1]:
                        raw_line = raw_line.strip()
                        if not raw_line:
                            continue
                        total += 1
                        try:
                            obj = json.loads(raw_line)
                            sub = obj.get("subreddit", "").lower()
                            if sub in subreddit_set:
                                writers[sub].write(
                                    json.dumps(obj) + "\n"
                                )
                                counts[sub] += 1
                        except json.JSONDecodeError:
                            continue
    finally:
        for w in writers.values():
            w.close()

    logger.info(
        f"Processed {total:,} lines from {Path(zst_path).name}. "
        f"Kept: { {k: v for k, v in counts.items()} }"
    )
    return out_paths


def main():
    parser = argparse.ArgumentParser(
        description="Download and filter Reddit Pushshift comment archives."
    )
    parser.add_argument(
        "--subreddits",
        nargs="+",
        default=["worldnews", "technology", "science"],
        help="Subreddits to keep",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2018, 2019, 2020],
        help="Years to download",
    )
    parser.add_argument(
        "--output", default="data/raw", help="Local output directory"
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    raw_dump_dir = os.path.join(args.output, "dumps")
    os.makedirs(raw_dump_dir, exist_ok=True)

    for year in args.years:
        for month in range(1, 13):
            zst_path = download_month(year, month, raw_dump_dir)
            if zst_path:
                filter_by_subreddit(zst_path, args.subreddits, args.output)

    logger.info("Data acquisition complete.")


if __name__ == "__main__":
    main()
