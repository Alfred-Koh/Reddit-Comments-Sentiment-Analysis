#!/usr/bin/env python3
"""
top_reducer.py — Collect all word counts per key and output top-N.
Input:  key \t word \t count   (sorted by key)
Output: ranked top-N words per key group
"""
import sys
import heapq

TOP_N = 20


def emit_top(key: str, heap: list):
    top = heapq.nlargest(TOP_N, heap, key=lambda x: x[0])
    print(f"\n=== TOP {TOP_N} WORDS — {key} ===")
    for rank, (count, word) in enumerate(top, 1):
        print(f"{rank}\t{word}\t{count}")


def main():
    current_key = None
    heap: list[tuple[int, str]] = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        key, word, count = parts[0], parts[1], parts[2]
        try:
            count = int(count)
        except ValueError:
            continue

        if key != current_key:
            if current_key is not None:
                emit_top(current_key, heap)
            current_key = key
            heap = []

        heap.append((count, word))

    if current_key is not None:
        emit_top(current_key, heap)


if __name__ == "__main__":
    main()
