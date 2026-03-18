#!/usr/bin/env python3
"""
mapper.py — Emit (subreddit\tword, 1) pairs for word-frequency MapReduce.
Input: tab-delimited lines: subreddit \t body_clean \t sentiment_label
"""
import sys
import re

STOPWORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "is","was","are","were","be","been","being","have","has","had","do",
    "does","did","will","would","could","should","may","might","shall",
    "this","that","these","those","it","its","i","you","he","she","we",
    "they","not","no","so","if","as","by","from","up","out","about",
}

def tokenize(text: str) -> list[str]:
    return [
        w for w in re.findall(r"[a-z']+", text.lower())
        if len(w) > 2 and w not in STOPWORDS
    ]

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        subreddit = parts[0].strip()
        body = parts[1].strip() if len(parts) > 1 else ""
        for word in tokenize(body):
            print(f"{subreddit}\t{word}\t1")

if __name__ == "__main__":
    main()
