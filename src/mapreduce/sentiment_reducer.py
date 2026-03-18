#!/usr/bin/env python3
"""
sentiment_reducer.py — Count comments per (subreddit, sentiment) pair.
Output: subreddit \t sentiment \t count
"""
import sys

def main():
    current_key = None
    total = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        subreddit, sentiment, count = parts
        key = f"{subreddit}\t{sentiment}"
        try:
            count = int(count)
        except ValueError:
            continue

        if key == current_key:
            total += count
        else:
            if current_key is not None:
                sub, sent = current_key.split("\t", 1)
                print(f"{sub}\t{sent}\t{total}")
            current_key = key
            total = count

    if current_key is not None:
        sub, sent = current_key.split("\t", 1)
        print(f"{sub}\t{sent}\t{total}")

if __name__ == "__main__":
    main()
