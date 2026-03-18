#!/bin/bash
# run_spark_analysis.sh — Submit Spark EDA analysis job.

export PYTHONPATH="$(pwd):$PYTHONPATH"
LOG_FILE="logs/spark_analysis.log"
mkdir -p logs
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
command -v "$SPARK_SUBMIT" &>/dev/null || SPARK_SUBMIT="spark-submit"

log "Submitting Spark EDA analysis..."
$SPARK_SUBMIT \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=200 \
    src/spark/jobs/run_analysis.py

if [ $? -eq 0 ]; then
    log "✅ EDA analysis complete. Results in data/spark_results/"
else
    log "ERROR: Spark analysis failed."; exit 1
fi
