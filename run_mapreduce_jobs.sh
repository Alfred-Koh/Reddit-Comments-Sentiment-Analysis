#!/bin/bash
# run_mapreduce_jobs.sh
# Runs the full two-stage MapReduce pipeline:
#   Stage 1 — Word frequency per subreddit (mapper → combiner → reducer)
#   Stage 2 — Top-N words per subreddit (top_mapper → top_reducer)
#   Stage 3 — Sentiment count per subreddit (sentiment_mapper → sentiment_reducer)

STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming-*.jar" | head -1)
if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: hadoop-streaming-*.jar not found under \$HADOOP_HOME"
    exit 1
fi
echo "Using JAR: $STREAMING_JAR"

HDFS_IN="/user/reddit_sentiment/mapreduce/input"
HDFS_OUT_BASE="/user/reddit_sentiment/mapreduce"
LOCAL_OUT="data/mapreduce_results"
mkdir -p "$LOCAL_OUT"

MAPPER="src/mapreduce/mapper.py"
REDUCER="src/mapreduce/reducer.py"
COMBINER="src/mapreduce/combiner.py"
TOP_MAPPER="src/mapreduce/top_mapper.py"
TOP_REDUCER="src/mapreduce/top_reducer.py"
SENT_MAPPER="src/mapreduce/sentiment_mapper.py"
SENT_REDUCER="src/mapreduce/sentiment_reducer.py"

chmod +x $MAPPER $REDUCER $COMBINER $TOP_MAPPER $TOP_REDUCER $SENT_MAPPER $SENT_REDUCER

run_job() {
    local name=$1 input=$2 output=$3 mapper=$4 reducer=$5 combiner=$6
    echo ""
    echo "=== $name ==="
    hdfs dfs -rm -r -f "$output"
    ARGS=(
        hadoop jar "$STREAMING_JAR"
        -D "mapred.job.name=$name"
        -D "mapreduce.job.reduces=5"
        -D "mapreduce.map.memory.mb=2048"
        -D "mapreduce.reduce.memory.mb=4096"
        -D "mapreduce.map.output.compress=true"
    )
    FILES="$mapper,$reducer"
    if [ -n "$combiner" ]; then FILES="$FILES,$combiner"; fi
    ARGS+=(-files "$FILES")
    ARGS+=(-mapper "python3 $(basename $mapper)")
    if [ -n "$combiner" ]; then ARGS+=(-combiner "python3 $(basename $combiner)"); fi
    ARGS+=(-reducer "python3 $(basename $reducer)")
    ARGS+=(-input "$input" -output "$output")
    "${ARGS[@]}"
    if [ $? -eq 0 ]; then
        echo "✅ $name complete → $output"
        hdfs dfs -getmerge "$output" "$LOCAL_OUT/$(basename $output).txt" 2>/dev/null
    else
        echo "❌ $name FAILED"; exit 1
    fi
}

SUBREDDITS=("worldnews" "technology" "science" "all_subreddits")

for SUB in "${SUBREDDITS[@]}"; do
    SUB_IN="${HDFS_IN}/${SUB}"

    # Stage 1 — word frequency
    run_job \
        "WordFreq-${SUB}" \
        "$SUB_IN" \
        "${HDFS_OUT_BASE}/${SUB}_word_freq" \
        "$MAPPER" "$REDUCER" "$COMBINER"

    # Stage 2 — top-N words
    run_job \
        "TopWords-${SUB}" \
        "${HDFS_OUT_BASE}/${SUB}_word_freq" \
        "${HDFS_OUT_BASE}/${SUB}_top_words" \
        "$TOP_MAPPER" "$TOP_REDUCER" ""

    # Stage 3 — sentiment counts
    run_job \
        "SentimentCount-${SUB}" \
        "$SUB_IN" \
        "${HDFS_OUT_BASE}/${SUB}_sentiment_counts" \
        "$SENT_MAPPER" "$SENT_REDUCER" ""
done

echo ""
echo "=== All MapReduce jobs complete ==="
echo "Local results: $LOCAL_OUT"
ls -lh "$LOCAL_OUT"
