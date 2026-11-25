"""Simulated real-time feed of bank transactions.

Splits data/bank.csv into small chunks and drops one CSV file into 04_streaming/stream_input/
every --interval seconds. Spark Structured Streaming watches that folder (file source).

Each row gets:
  txn_id      running id
  event_time  when the transaction "happened". Each chunk covers 10 s of event time
              (time is compressed: 10 s of event time is sent every 2 s of wall-clock time)
Simulated real-world problems (so windows and watermarking have something to show):
  * --late-fraction of rows are LATE / OUT-OF-ORDER: their event_time is 15-90 s older than the chunk
  * one TRAFFIC BURST: chunk --burst-chunk carries 3x the normal number of rows
"""
import argparse
import csv
import os
import random
import time
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-size", type=int, default=150)
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between files")
    ap.add_argument("--late-fraction", type=float, default=0.05)
    ap.add_argument("--burst-chunk", type=int, default=12)
    ap.add_argument("--out", default=os.path.join(ROOT, "04_streaming", "stream_input"))
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    random.seed(args.seed)
    rows = list(csv.DictReader(open(os.path.join(ROOT, "data", "bank.csv"))))
    random.shuffle(rows)                       # mix clients so every chunk looks like live traffic
    os.makedirs(args.out, exist_ok=True)
    fields = ["txn_id", "event_time"] + list(rows[0].keys())

    now = datetime.now().replace(microsecond=0)
    base = now - timedelta(seconds=now.second % 10)      # align to 10 s so each chunk = one 10 s window
    pos, chunk, txn, late_total = 0, 0, 0, 0
    while pos < len(rows):
        size = args.chunk_size * (3 if chunk == args.burst_chunk else 1)
        part = rows[pos:pos + size]
        pos += size
        start = base + timedelta(seconds=10 * chunk)
        out_rows = []
        for r in part:
            txn += 1
            et = start + timedelta(seconds=random.uniform(0, 10))
            if random.random() < args.late_fraction:          # late + out-of-order event
                et -= timedelta(seconds=random.uniform(15, 90))
                late_total += 1
            out_rows.append({"txn_id": txn, "event_time": et.strftime("%Y-%m-%d %H:%M:%S"), **r})
        random.shuffle(out_rows)                               # out-of-order inside the file too
        tmp = os.path.join(args.out, f".chunk_{chunk:03d}.tmp")
        with open(tmp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(out_rows)
        os.rename(tmp, os.path.join(args.out, f"chunk_{chunk:03d}.csv"))   # atomic: Spark never sees half a file
        tag = "  <- BURST (3x traffic)" if chunk == args.burst_chunk else ""
        print(f"[producer] chunk {chunk:02d}: {len(out_rows):3d} rows, event time "
              f"{start.strftime('%H:%M:%S')}-{(start + timedelta(seconds=10)).strftime('%H:%M:%S')}{tag}", flush=True)
        chunk += 1
        time.sleep(args.interval)
    print(f"[producer] done: {txn} transactions in {chunk} files, {late_total} late/out-of-order", flush=True)


if __name__ == "__main__":
    main()
