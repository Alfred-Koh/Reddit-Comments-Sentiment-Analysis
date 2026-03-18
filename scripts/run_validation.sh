#!/bin/bash
# run_validation.sh — Submit PySpark validation job.

LOG_FILE="logs/validation_job.log"
mkdir -p logs
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
command -v "$SPARK_SUBMIT" &>/dev/null || SPARK_SUBMIT="spark-submit"

log "Submitting validation job..."
$SPARK_SUBMIT \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    src/preprocessing/data_validation.py

[ $? -eq 0 ] && log "✅ Validation complete." || { log "ERROR: Validation failed."; exit 1; }
