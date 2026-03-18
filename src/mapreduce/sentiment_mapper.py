#!/usr/bin/env python3
"""
sentiment_mapper.py — Emit (subreddit\tsentiment_label, 1) for counting.
Input: subreddit \t body_clean \t sentiment_label
"""
import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        subreddit = parts[0].strip()
        sentiment = parts[2].strip()
        if sentiment in ("positive", "neutral", "negative"):
            print(f"{subreddit}\t{sentiment}\t1")

if __name__ == "__main__":
    main()
