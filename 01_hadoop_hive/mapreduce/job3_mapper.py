#!/usr/bin/env python3
"""Job 3 mapper: emit  month|y \t 1   (month is prefixed with its number so it sorts Jan..Dec)"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse

ORDER = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

for line in sys.stdin:
    row = parse(line)
    if row:
        print(f"{ORDER[row['month']]:02d}-{row['month']}|{row['y']}\t1")
