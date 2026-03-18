#!/usr/bin/env python3
"""
mapreduce_helper.py
Utility to locate the Hadoop Streaming JAR and submit Hadoop Streaming jobs.
"""

import os
import subprocess
import logging
import time
import json
from pathlib import Path

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mapreduce_helper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def find_streaming_jar() -> str | None:
    """Locate hadoop-streaming-*.jar under $HADOOP_HOME."""
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        logger.error("HADOOP_HOME is not set.")
        return None

    search_dirs = [
        Path(hadoop_home) / "share" / "hadoop" / "tools" / "lib",
        Path(hadoop_home) / "share" / "hadoop" / "mapreduce",
    ]
    for d in search_dirs:
        matches = list(d.glob("hadoop-streaming-*.jar"))
        if matches:
            logger.info(f"Found streaming JAR: {matches[0]}")
            return str(matches[0])

    logger.error("hadoop-streaming-*.jar not found.")
    return None


def run_streaming_job(
    mapper: str,
    reducer: str,
    input_path: str,
    output_path: str,
    combiner: str | None = None,
    num_reducers: int = 5,
    extra_files: list[str] | None = None,
    job_name: str = "Reddit Sentiment MapReduce",
    streaming_jar: str | None = None,
) -> bool:
    """
    Submit a Hadoop Streaming job.

    Args:
        mapper:        Path to mapper script.
        reducer:       Path to reducer script.
        input_path:    HDFS input path.
        output_path:   HDFS output path (will be deleted if it already exists).
        combiner:      Optional path to combiner script.
        num_reducers:  Number of reduce tasks.
        extra_files:   Additional files to distribute to all nodes.
        job_name:      YARN job name.
        streaming_jar: Path to streaming JAR (auto-detected if None).

    Returns:
        True if job succeeded, False otherwise.
    """
    if streaming_jar is None:
        streaming_jar = find_streaming_jar()
    if streaming_jar is None:
        return False

    # Make scripts executable
    for script in filter(None, [mapper, reducer, combiner]):
        os.chmod(script, 0o755)

    # Remove existing output directory
    subprocess.run(f"hdfs dfs -rm -r -f {output_path}", shell=True, capture_output=True)

    # Build file list
    files = [mapper, reducer]
    if combiner:
        files.append(combiner)
    if extra_files:
        files.extend(extra_files)
    files_arg = ",".join(files)

    # Build command
    cmd_parts = [
        f"hadoop jar {streaming_jar}",
        f'-D mapred.job.name="{job_name}"',
        f"-D mapreduce.job.reduces={num_reducers}",
        "-D mapreduce.map.memory.mb=2048",
        "-D mapreduce.reduce.memory.mb=4096",
        "-D mapreduce.map.output.compress=true",
        "-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec",
        f'-files "{files_arg}"',
        f'-mapper "python3 {Path(mapper).name}"',
    ]
    if combiner:
        cmd_parts.append(f'-combiner "python3 {Path(combiner).name}"')
    cmd_parts += [
        f'-reducer "python3 {Path(reducer).name}"',
        f"-input {input_path}",
        f"-output {output_path}",
    ]
    cmd = " \\\n  ".join(cmd_parts)

    logger.info(f"Submitting job: {job_name}")
    logger.info(f"  Input:  {input_path}")
    logger.info(f"  Output: {output_path}")

    start = time.time()
    result = subprocess.run(cmd, shell=True, capture_output=True)
    elapsed = time.time() - start

    if result.returncode == 0:
        logger.info(f"✅ Job completed in {elapsed:.1f}s")
        # Save stats
        os.makedirs("data/mapreduce_results/stats", exist_ok=True)
        stats = {
            "job_name": job_name,
            "duration_seconds": elapsed,
            "status": "success",
            "input": input_path,
            "output": output_path,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        stats_file = (
            f"data/mapreduce_results/stats/"
            f"{Path(mapper).stem}_{int(time.time())}.json"
        )
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=2)
        return True
    else:
        logger.error(f"❌ Job failed:\n{result.stderr.decode()}")
        return False
