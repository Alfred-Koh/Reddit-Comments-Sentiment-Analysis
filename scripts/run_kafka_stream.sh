#!/bin/bash
# run_kafka_stream.sh — Start Kafka producer and Spark Structured Streaming consumer.

LOG_FILE="logs/kafka_stream.log"
mkdir -p logs
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
command -v "$SPARK_SUBMIT" &>/dev/null || SPARK_SUBMIT="spark-submit"

# Start producer in background
log "Starting Kafka producer..."
python3 src/streaming/kafka_producer.py \
    --bootstrap-servers localhost:9092 \
    --subreddits worldnews technology science \
    --delay 0.001 &
PRODUCER_PID=$!
log "Producer PID: $PRODUCER_PID"

# Start Spark Structured Streaming consumer
log "Starting Spark Structured Streaming consumer..."
$SPARK_SUBMIT \
    --master local[4] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --driver-memory 4g \
    --executor-memory 4g \
    src/streaming/kafka_consumer.py

log "Streaming session ended. Stopping producer..."
kill $PRODUCER_PID 2>/dev/null
log "Done."
