"""Make Colab copies of notebooks 02, 03, 05 in colab/ (same analysis, Colab-friendly setup cell)."""
import os
import nbformat

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = "pranatipradhanwork-alt/financial-forecasting-frontier-distributed-ml"
RAW = f"https://raw.githubusercontent.com/{REPO}/main/data/bank.csv"

SETUP = '''# ---- Colab setup: Java 17 + PySpark 4.0.1 + bank.csv (about 1-2 minutes the first time) ----
import os, sys, subprocess, time, urllib.request
IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    subprocess.run("apt-get -qq update && apt-get -qq install -y openjdk-17-jdk-headless > /dev/null", shell=True, check=True)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    subprocess.run([sys.executable, "-m", "pip", "-q", "install", "pyspark==4.0.1"], check=True)
else:   # running outside Colab (e.g. VS Code on the Mac)
    os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
os.environ["PYSPARK_PYTHON"] = sys.executable

os.makedirs("data", exist_ok=True)
if not os.path.exists("data/bank.csv"):
    urllib.request.urlretrieve("{raw}", "data/bank.csv")
print("bank.csv:", sum(1 for _ in open("data/bank.csv")) - 1, "rows")

from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder
         .appName("{app}")
         .master("local[*]")                          # use every core of this machine as a parallel worker
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")  # small data -> fewer shuffle partitions
         .config("spark.ui.showConsoleProgress", "false"){extra}
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
DATA = "data/bank.csv"
print("Spark", spark.version, "| master:", spark.sparkContext.master,
      "| default parallelism:", spark.sparkContext.defaultParallelism)'''

NOTEBOOKS = [
    ("02_eda_spark.ipynb", "Bank-EDA", ""),
    ("03_spark_ml.ipynb", "Bank-SparkML", ""),
    ("05_data_parallelism.ipynb", "Bank-DataParallelism", '\n         .config("spark.scheduler.mode", "FAIR")      # lets concurrent jobs share the cores fairly'),
]


P05 = [   # notebook 05 text was written for the Mac; make it true on any machine (Colab usually has 2 cores)
 ("Machine: MacBook Air M2 (8 cores, 8 GB RAM). Spark runs `local[*]` → **4 executor threads = 4 tasks in parallel**.",
  "Spark runs `local[*]` → **one task thread per CPU core of this machine** (Colab normally gives 2 cores; the setup "
  "cell prints the number)."),
 ("**one** of the 4 cores would work on it.", "**one** of the cores would work on it."),
 ("into 8 partitions (2 × the 4 cores).**", "into 8 partitions (several per core).**"),
 ("* 8 partitions = 2 tasks per core, so", "* Several tasks per core, so"),
 ("train = train.repartition(4).cache()", "CORES = spark.sparkContext.defaultParallelism\ntrain = train.repartition(CORES).cache()"),
 ('a1.axhline(4, color="grey", lw=0.8, ls=":", label="local[*] limit")',
  'a1.axhline(spark.sparkContext.defaultParallelism, color="grey", lw=0.8, ls=":", label="cores available to Spark")'),
 ('{phase.spark_cores_used.max():.1f} of 4 |', '{phase.spark_cores_used.max():.1f} of {spark.sparkContext.defaultParallelism} |'),
 ("shares the 4 cores between them", "shares the available cores between them"),
 ("* **Right-sizing:** 8 partitions for 4 cores keeps every core busy without drowning in tiny tasks.",
  "* **Right-sizing:** a few partitions per core keeps every core busy without drowning in tiny tasks."),
]
P05_OBS = """**Observations** (read them against the table and chart above — the exact numbers depend on the machine)
* CPU is measured for the **Spark JVM itself** (JVM CPU% ÷ 100 = cores in use), so other processes do not distort it.
* Short actions on the single-partition input keep about one core busy; the repartitioned aggregation, the training
  and especially the 50× scaling experiment push Spark up to the number of cores it has (`local[*]`).
  Short peaks above that line are the JVM's own garbage-collection and JIT-compiler threads.
* Spark JVM memory grows in steps when data is `cache()`d and does not drop straight away after `unpersist()`
  (the JVM keeps its heap). Caching trades memory for speed, so each experiment releases its cache when done.
* GC time is small compared with total task time, so memory is not the bottleneck at this data size.
* In the scaling experiment, training is fastest when the number of partitions is close to the number of cores;
  far more partitions than cores (32) adds scheduling overhead and becomes slower again."""


def main():
    os.makedirs(os.path.join(ROOT, "colab"), exist_ok=True)
    for name, app, extra in NOTEBOOKS:
        nb = nbformat.read(os.path.join(ROOT, name), 4)
        out_name = name.replace(".ipynb", "_colab.ipynb")
        idx = next(i for i, c in enumerate(nb.cells) if c.cell_type == "code" and "SparkSession.builder" in c.source)
        nb.cells[idx] = nbformat.v4.new_code_cell(SETUP.replace("{raw}", RAW).replace("{app}", app).replace("{extra}", extra))
        badge = (f"[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)]"
                 f"(https://colab.research.google.com/github/{REPO}/blob/main/colab/{out_name})\n\n"
                 "**Financial Forecasting Frontier: Distributed ML** · Run with **Runtime → Run all**. "
                 "The first cell installs Java 17 + PySpark and downloads `bank.csv` from the GitHub repo.")
        nb.cells.insert(0, nbformat.v4.new_markdown_cell(badge))
        for c in nb.cells:
            c.source = (c.source.replace("local[4]", "local[*]")
                        .replace("4 worker threads", "all available worker threads")
                        .replace("4 parallel tasks", "parallel tasks"))
            if c.cell_type == "code":
                c.outputs, c.execution_count = [], None
        if name.startswith("05"):
            for c in nb.cells:
                for a, b in P05:
                    c.source = c.source.replace(a, b)
                if c.cell_type == "markdown" and c.source.startswith("**Observations**"):
                    c.source = P05_OBS
        nb.metadata = {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                       "language_info": {"name": "python"}, "colab": {"provenance": []}}
        nbformat.write(nb, os.path.join(ROOT, "colab", out_name))
        print("wrote colab/" + out_name)


if __name__ == "__main__":
    main()
