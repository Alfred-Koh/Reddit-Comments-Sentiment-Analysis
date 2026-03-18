#!/bin/bash
# run_preprocessing.sh — Submit PySpark preprocessing job.

LOG_FILE="logs/preprocessing_job.log"
mkdir -p logs
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
command -v "$SPARK_SUBMIT" &>/dev/null || SPARK_SUBMIT="spark-submit"

hdfs dfs -ls / &>/dev/null || { log "ERROR: HDFS not reachable."; exit 1; }

log "Submitting preprocessing job..."
$SPARK_SUBMIT \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.sql.shuffle.partitions=200 \
    src/preprocessing/data_preprocessing.py

if [ $? -eq 0 ]; then
    log "✅ Preprocessing complete."
else
    log "ERROR: Preprocessing failed."; exit 1
fi
