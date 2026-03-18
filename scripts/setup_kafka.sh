#!/bin/bash
# setup_kafka.sh — Start Zookeeper + Kafka and create subreddit topics.

LOG_FILE="logs/kafka_setup.log"
mkdir -p logs

log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

if [ -z "$KAFKA_HOME" ]; then
    log "ERROR: KAFKA_HOME is not set."
    exit 1
fi
log "KAFKA_HOME = $KAFKA_HOME"

# Start Zookeeper
log "Starting Zookeeper..."
"$KAFKA_HOME/bin/zookeeper-server-start.sh" \
    "$KAFKA_HOME/config/zookeeper.properties" &
sleep 5

# Start Kafka broker
log "Starting Kafka broker..."
"$KAFKA_HOME/bin/kafka-server-start.sh" \
    "$KAFKA_HOME/config/server.properties" &
sleep 8

# Create one topic per subreddit
SUBREDDITS=("worldnews" "technology" "science")
for SUB in "${SUBREDDITS[@]}"; do
    TOPIC="reddit-${SUB}"
    log "Creating Kafka topic: $TOPIC"
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --create \
        --topic "$TOPIC" \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    log "  ✅ $TOPIC ready"
done

log "Listing all topics:"
"$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server localhost:9092

log "Kafka setup complete."
