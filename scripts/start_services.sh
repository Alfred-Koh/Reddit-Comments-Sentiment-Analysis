#!/bin/bash
# start_services.sh — Start HDFS + YARN + Spark History Server.

LOG_FILE="logs/services.log"
mkdir -p logs

log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

if [ -z "$HADOOP_HOME" ]; then log "ERROR: HADOOP_HOME not set."; exit 1; fi

# Start HDFS
log "Starting HDFS..."
"$HADOOP_HOME/sbin/start-dfs.sh"
jps | grep -q "NameNode" && log "✅ NameNode running" || { log "ERROR: NameNode not started."; exit 1; }

# Start YARN
log "Starting YARN..."
"$HADOOP_HOME/sbin/start-yarn.sh"
jps | grep -q "ResourceManager" && log "✅ ResourceManager running" || { log "ERROR: ResourceManager not started."; exit 1; }

# Create HDFS directories
log "Creating HDFS directory structure..."
DIRS=(
    "/user/reddit_sentiment/raw"
    "/user/reddit_sentiment/processed"
    "/user/reddit_sentiment/mapreduce"
    "/user/reddit_sentiment/results"
    "/user/reddit_sentiment/models"
    "/user/reddit_sentiment/streaming_output"
    "/user/reddit_sentiment/checkpoints"
    "/spark-logs"
)
for D in "${DIRS[@]}"; do
    hdfs dfs -mkdir -p "$D"
    log "  $D"
done
hdfs dfs -chmod -R 777 /user /spark-logs

# Spark History Server
if [ -n "$SPARK_HOME" ]; then
    log "Starting Spark History Server..."
    "$SPARK_HOME/sbin/start-history-server.sh" && log "✅ History Server started"
fi

log "All services started. Run 'jps' to verify."
