"""Shared helpers for the Hadoop Streaming jobs.

Hadoop Streaming feeds each mapper raw CSV lines on stdin.
We skip the header line and split on commas (bank.csv has no quoted commas).
"""
COLUMNS = ["age", "job", "marital", "education", "default", "balance", "housing",
           "loan", "contact", "day", "month", "duration", "campaign", "pdays",
           "previous", "poutcome", "y"]
IDX = {c: i for i, c in enumerate(COLUMNS)}


def parse(line):
    """Return a dict for a data row, or None for the header / a broken row."""
    parts = line.strip().replace('"', "").split(",")
    if len(parts) != len(COLUMNS) or parts[0] == "age":
        return None
    return {c: parts[i] for c, i in IDX.items()}
