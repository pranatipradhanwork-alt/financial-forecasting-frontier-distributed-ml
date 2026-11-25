#!/usr/bin/env python3
"""Job 5 mapper: put each client in a 10-year age band and emit  band \t balance"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse

for line in sys.stdin:
    row = parse(line)
    if row:
        age = int(row["age"])
        low = (age // 10) * 10
        print(f"{low:02d}-{low + 9}\t{row['balance']}")
