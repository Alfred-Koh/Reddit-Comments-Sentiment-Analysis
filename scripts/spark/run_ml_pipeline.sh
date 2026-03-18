#!/bin/bash
# run_ml_pipeline.sh — Submit Spark MLlib sentiment classification pipeline.

export PYTHONPATH="$(pwd):$PYTHONPATH"
LOG_FILE="logs/ml_pipeline.log"
mkdir -p logs
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
command -v "$SPARK_SUBMIT" &>/dev/null || SPARK_SUBMIT="spark-submit"

log "Submitting ML pipeline (Phase 1: TF-IDF + LogReg, Phase 2: W2V + GBT)..."
$SPARK_SUBMIT \
    --master local[*] \
    --driver-memory 6g \
    --executor-memory 6g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    src/spark/jobs/run_ml_pipeline.py

if [ $? -eq 0 ]; then
    log "✅ ML pipeline complete. Metrics in data/spark_results/ml/"
else
    log "ERROR: ML pipeline failed."; exit 1
fi
