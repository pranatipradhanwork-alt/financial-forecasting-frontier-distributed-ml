#!/usr/bin/env python3
"""Job 1 reducer: average balance per job.
Input arrives sorted by key, so all lines for one job are next to each other."""
import sys

current, total, count = None, 0, 0

def flush():
    if current is not None:
        print(f"{current}\t{count}\t{total / count:.2f}")

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current:
        flush()
        current, total, count = key, 0, 0
    total += int(value)
    count += 1
flush()
