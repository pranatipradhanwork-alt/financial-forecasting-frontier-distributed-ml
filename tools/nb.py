"""Tiny helper: build a .ipynb from a list of ("md"|"code", text) cells and (optionally) execute it."""
import os
import sys
import nbformat
from nbclient import NotebookClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SETUP = '''import os, sys, time
# Spark needs Java 17; point to Homebrew's JDK if JAVA_HOME is not already set
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
os.environ["PYSPARK_PYTHON"] = sys.executable          # workers use this venv's Python
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")    # single machine: bind to localhost
os.chdir(os.path.dirname(os.path.abspath("__file__")) if os.path.exists("data") else "..")

from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder
         .appName("{app}")
         .master("local[4]")                          # 4 worker threads = 4 parallel tasks
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")  # small data -> fewer shuffle partitions
         .config("spark.hadoop.fs.defaultFS", "file:///")  # read local files even if HADOOP_CONF_DIR is set
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
DATA = "data/bank.csv"
print("Spark", spark.version, "| master:", spark.sparkContext.master,
      "| default parallelism:", spark.sparkContext.defaultParallelism)
print("Spark UI (while this notebook runs): http://localhost:4040")'''


def build(name, cells, execute=True, timeout=1800):
    nb = nbformat.v4.new_notebook()
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3 (.venv)", "language": "python"}
    for kind, text in cells:
        text = text.strip("\n")
        nb.cells.append(nbformat.v4.new_markdown_cell(text) if kind == "md" else nbformat.v4.new_code_cell(text))
    path = os.path.join(ROOT, name)
    if execute:
        NotebookClient(nb, timeout=timeout, kernel_name="python3",
                       resources={"metadata": {"path": ROOT}}).execute()
    nbformat.write(nb, path)
    print("wrote", path)
