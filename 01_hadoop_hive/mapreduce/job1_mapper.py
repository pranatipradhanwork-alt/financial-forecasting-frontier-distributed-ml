#!/usr/bin/env python3
"""Job 1 mapper: emit  job \t balance"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse

for line in sys.stdin:
    row = parse(line)
    if row:
        print(f"{row['job']}\t{row['balance']}")
