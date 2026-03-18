#!/usr/bin/env python3
"""
top_mapper.py — Re-emit word counts with type prefix for top-N ranking.
Input:  subreddit \t word \t count   (output of reducer.py)
Output: SUBREDDIT_{subreddit} \t word \t count
        or GLOBAL \t word \t count
"""
import sys

MIN_COUNT = 2  # ignore very rare words (lower threshold works for small test sets)


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        subreddit, word, count = parts[0], parts[1], parts[2]
        try:
            count = int(count)
        except ValueError:
            continue

        if count < MIN_COUNT:
            continue

        # Emit per-subreddit key
        print(f"SUBREDDIT_{subreddit}\t{word}\t{count}")
        # Also emit global key
        print(f"GLOBAL\t{word}\t{count}")


if __name__ == "__main__":
    main()
