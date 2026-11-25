#!/usr/bin/env python3
"""Job 4 mapper: emit  poutcome \t duration"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import parse

for line in sys.stdin:
    row = parse(line)
    if row:
        print(f"{row['poutcome']}\t{row['duration']}")
