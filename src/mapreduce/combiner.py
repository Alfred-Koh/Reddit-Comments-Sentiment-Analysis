#!/usr/bin/env python3
"""
combiner.py — Local aggregation to reduce shuffle traffic.
Same logic as reducer but runs on each mapper node.
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
        subreddit, word, count = parts[0], parts[1], parts[2]
        key = f"{subreddit}\t{word}"
        try:
            count = int(count)
        except ValueError:
            continue

        if key == current_key:
            total += count
        else:
            if current_key is not None:
                sub, wd = current_key.split("\t", 1)
                print(f"{sub}\t{wd}\t{total}")
            current_key = key
            total = count

    if current_key is not None:
        sub, wd = current_key.split("\t", 1)
        print(f"{sub}\t{wd}\t{total}")

if __name__ == "__main__":
    main()
