#!/bin/bash
# configure_hadoop.sh — Copy config files into $HADOOP_HOME and format NameNode.

LOG_FILE="logs/hadoop_setup.log"
mkdir -p logs

log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

log "Starting Hadoop configuration..."

if [ -z "$HADOOP_HOME" ]; then
    log "ERROR: HADOOP_HOME is not set. Please export it before running this script."
    exit 1
fi
log "HADOOP_HOME = $HADOOP_HOME"

# Create HDFS data directories
mkdir -p ~/hadoop_data/namenode
mkdir -p ~/hadoop_data/datanode
log "Created local HDFS data directories."

# Copy configuration files
cp conf/hadoop/core-site.xml   "$HADOOP_HOME/etc/hadoop/"
cp conf/hadoop/hdfs-site.xml   "$HADOOP_HOME/etc/hadoop/"
cp conf/hadoop/mapred-site.xml "$HADOOP_HOME/etc/hadoop/"
cp conf/hadoop/yarn-site.xml   "$HADOOP_HOME/etc/hadoop/"
log "Copied Hadoop config files to $HADOOP_HOME/etc/hadoop/"

# Format NameNode only if it has not been formatted yet
if [ ! -d ~/hadoop_data/namenode/current ]; then
    log "Formatting HDFS NameNode..."
    "$HADOOP_HOME/bin/hdfs" namenode -format -nonInteractive
    if [ $? -eq 0 ]; then
        log "NameNode formatted successfully."
    else
        log "ERROR: NameNode format failed."
        exit 1
    fi
else
    log "NameNode already formatted — skipping."
fi

log "Hadoop configuration complete."
