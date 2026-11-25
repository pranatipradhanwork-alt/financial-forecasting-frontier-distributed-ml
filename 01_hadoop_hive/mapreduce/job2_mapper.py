#!/usr/bin/env python3
"""Job 2 mapper: emit  education|housing \t 1"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse

for line in sys.stdin:
    row = parse(line)
    if row:
        print(f"{row['education']}|{row['housing']}\t1")
