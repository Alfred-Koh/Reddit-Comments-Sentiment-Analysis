#!/usr/bin/env python3
"""
hdfs_upload.py
Upload raw Reddit comment JSON.gz files to HDFS.
"""

import os
import subprocess
import logging
from pathlib import Path

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/hdfs_upload.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

HDFS_DIRS = [
    "/user/reddit_sentiment/raw",
    "/user/reddit_sentiment/processed",
    "/user/reddit_sentiment/mapreduce",
    "/user/reddit_sentiment/results",
    "/spark-logs",
]


def ensure_hdfs_dirs():
    logger.info("Creating HDFS directory structure...")
    for d in HDFS_DIRS:
        cmd = f"hdfs dfs -mkdir -p {d}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if result.returncode == 0:
            logger.info(f"  ✅ {d}")
        else:
            logger.warning(f"  ⚠️  {d}: {result.stderr.decode().strip()}")


def upload(local_path: str, hdfs_path: str) -> bool:
    if not os.path.exists(local_path):
        logger.error(f"Local file not found: {local_path}")
        return False

    size_mb = Path(local_path).stat().st_size / (1024 * 1024)
    logger.info(f"Uploading {local_path} ({size_mb:.1f} MB) → {hdfs_path}")

    result = subprocess.run(
        f"hdfs dfs -put -f {local_path} {hdfs_path}",
        shell=True, capture_output=True,
    )
    if result.returncode == 0:
        logger.info(f"  ✅ Uploaded {Path(local_path).name}")
        return True
    else:
        logger.error(f"  ❌ Upload failed: {result.stderr.decode().strip()}")
        return False


def upload_all():
    ensure_hdfs_dirs()
    raw_dir = "data/raw"
    hdfs_raw = "/user/reddit_sentiment/raw"

    uploaded, failed = 0, 0
    for fname in os.listdir(raw_dir):
        if fname.endswith(".json.gz"):
            ok = upload(
                os.path.join(raw_dir, fname),
                f"{hdfs_raw}/{fname}",
            )
            if ok:
                uploaded += 1
            else:
                failed += 1

    logger.info(f"Upload summary: {uploaded} succeeded, {failed} failed.")


if __name__ == "__main__":
    logger.info("Starting HDFS upload...")
    upload_all()
    logger.info("Done.")
