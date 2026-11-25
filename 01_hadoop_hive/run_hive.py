"""Run every statement in hive/banking_analysis.hql on HiveServer2 and print the results.

Same HiveQL that you would type in beeline; we send it with PyHive (Thrift) because
beeline needs an interactive terminal on Apple-silicon Macs.
Usage:  python 01_hadoop_hive/run_hive.py   (cluster + HiveServer2 must be running)
"""
import getpass
import os
import re
import time

from pyhive import hive

HQL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hive", "banking_analysis.hql")


def statements(path):
    """Yield (kind, text): ('title', ...) for '-- ---------- Qx' lines, ('note', ...) for comments, (None, sql)."""
    buf = []
    for line in open(path):
        s = line.strip()
        if s.startswith("-- ----------"):
            yield ("title", s.strip("- ").strip())
            continue
        if s.startswith("--") or not s:
            if s.startswith("--") and buf == []:
                yield ("note", s[2:].strip())
            continue
        buf.append(line.rstrip())
        if s.endswith(";"):
            yield (None, "\n".join(buf).rstrip(";"))
            buf = []


def show(cursor, max_rows=None):
    rows = cursor.fetchall()
    cols = [d[0].split(".")[-1] for d in cursor.description]
    shown = rows if max_rows is None else rows[:max_rows]
    table = [cols] + [["NULL" if v is None else str(v) for v in r] for r in shown]
    widths = [max(len(r[i]) for r in table) for i in range(len(cols))]
    line = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(line)
    for i, r in enumerate(table):
        print("| " + " | ".join(v.ljust(w) for v, w in zip(r, widths)) + " |")
        if i == 0:
            print(line)
    print(line)
    print(f"{len(rows)} row(s)" + ("" if len(shown) == len(rows) else f"  (showing first {len(shown)})"))


def main():
    user = getpass.getuser()
    cur = hive.connect(host="localhost", port=10000, username=user).cursor()
    for title, sql in statements(HQL):
        if title == "note":
            print(f"   -- {sql}")
            continue
        if title == "title":
            print("\n" + "=" * 78 + f"\n{sql}\n" + "=" * 78)
            continue
        sql = sql.replace("${env:USER}", user)
        print("\nhive> " + "\n    > ".join(sql.split("\n")) + ";")
        start = time.time()
        cur.execute(sql)
        if cur.description:
            show(cur)
        print(f"(OK, {time.time() - start:.2f} s)")


if __name__ == "__main__":
    main()
