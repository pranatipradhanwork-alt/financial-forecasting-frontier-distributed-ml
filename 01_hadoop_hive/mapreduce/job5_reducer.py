#!/usr/bin/env python3
"""Job 5 reducer: for each age band print count, min, avg, median and max balance."""
import sys
from statistics import median

current, values = None, []

def flush():
    if current is not None:
        print(f"{current}\t{len(values)}\t{min(values)}\t"
              f"{sum(values) / len(values):.2f}\t{median(values):.1f}\t{max(values)}")

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current:
        flush()
        current, values = key, []
    values.append(int(value))
flush()
