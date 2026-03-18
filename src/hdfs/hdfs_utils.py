#!/usr/bin/env python3
"""hdfs_utils.py — HDFS helper functions."""

import os
import subprocess
import logging

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/hdfs_utils.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def _run(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, capture_output=True)


def exists(hdfs_path: str) -> bool:
    return _run(f"hdfs dfs -test -e {hdfs_path}").returncode == 0


def size(hdfs_path: str) -> str | None:
    if not exists(hdfs_path):
        logger.error(f"Path not found: {hdfs_path}")
        return None
    r = _run(f"hdfs dfs -du -s -h {hdfs_path}")
    if r.returncode == 0:
        return r.stdout.decode().split()[0]
    return None


def ls(hdfs_path: str) -> list[dict]:
    if not exists(hdfs_path):
        return []
    r = _run(f"hdfs dfs -ls {hdfs_path}")
    files = []
    for line in r.stdout.decode().strip().split("\n")[1:]:
        parts = line.split()
        if len(parts) >= 8:
            files.append({
                "permissions": parts[0],
                "owner": parts[2],
                "size": parts[4],
                "date": f"{parts[5]} {parts[6]}",
                "path": parts[7],
            })
    return files


def download(hdfs_path: str, local_path: str) -> bool:
    if not exists(hdfs_path):
        logger.error(f"HDFS path not found: {hdfs_path}")
        return False
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    r = _run(f"hdfs dfs -get {hdfs_path} {local_path}")
    if r.returncode == 0:
        logger.info(f"Downloaded {hdfs_path} → {local_path}")
        return True
    logger.error(f"Download failed: {r.stderr.decode()}")
    return False


def delete(hdfs_path: str) -> bool:
    if not exists(hdfs_path):
        return True
    r = _run(f"hdfs dfs -rm -r {hdfs_path}")
    if r.returncode == 0:
        logger.info(f"Deleted {hdfs_path}")
        return True
    logger.error(f"Delete failed: {r.stderr.decode()}")
    return False
