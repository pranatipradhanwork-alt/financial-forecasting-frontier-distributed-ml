#!/usr/bin/env python3
"""Generic count reducer (used by jobs 2 and 3): sums the 1s for each key.
Keys look like  a|b  and are printed as two columns."""
import sys

current, count = None, 0

def flush():
    if current is not None:
        print("\t".join(current.split("|")) + f"\t{count}")

for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current:
        flush()
        current, count = key, 0
    count += int(value)
flush()
