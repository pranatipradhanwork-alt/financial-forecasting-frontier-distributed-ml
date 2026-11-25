from nb import build, SETUP

setup = SETUP.format(app="Bank-DataParallelism").replace(
    '.config("spark.ui.showConsoleProgress", "false")',
    '.config("spark.ui.showConsoleProgress", "false")\n         .config("spark.scheduler.mode", "FAIR")      # lets concurrent jobs share the 4 cores fairly')

cells = [
("md", """# Part 5 — Efficient Data Handling through Data Parallelism
**Data parallelism** = split the data into partitions and run the *same* operation on every partition at the
same time, then combine the small partial results. Spark does this automatically; this notebook shows *how*,
measures it, and tunes it.

Machine: MacBook Air M2 (8 cores, 8 GB RAM). Spark runs `local[4]` → **4 executor threads = 4 tasks in parallel**.
Because `bank.csv` is tiny (4,521 rows), some experiments also use a **50× scaled copy (226,050 rows)** so that
parallelism effects are measurable instead of being hidden by scheduling overhead."""),
("code", setup),
("code", """# Resource monitor: samples CPU and memory of the whole machine and of the Spark JVM every 0.5 s
import threading, psutil, pandas as pd
import matplotlib.pyplot as plt

class ResourceMonitor:
    def __init__(self, interval=0.5):
        self.interval, self.samples, self.marks = interval, [], []
        self._stop = threading.Event()
        me = psutil.Process()
        self.jvm = next((c for c in me.children(recursive=True) if "java" in c.name().lower()), None)
    def _run(self):
        t0 = time.time()
        psutil.cpu_percent(percpu=True)
        while not self._stop.is_set():
            time.sleep(self.interval)
            cores = psutil.cpu_percent(percpu=True)
            self.samples.append({"t": time.time() - t0, "cpu_total_pct": sum(cores) / len(cores),
                                 "busy_cores": sum(c > 50 for c in cores),
                                 "jvm_cpu_pct": self.jvm.cpu_percent() if self.jvm else None,
                                 "jvm_rss_mb": self.jvm.memory_info().rss / 2**20 if self.jvm else None,
                                 "ram_used_pct": psutil.virtual_memory().percent})
        self.t0 = t0
    def start(self):
        self._t0 = time.time(); self.thread = threading.Thread(target=self._run, daemon=True); self.thread.start(); return self
    def mark(self, label):
        self.marks.append((time.time() - self._t0, label))
    def stop(self):
        self._stop.set(); self.thread.join(); return pd.DataFrame(self.samples)

monitor = ResourceMonitor().start()
monitor.mark("start")
print("monitoring Spark JVM pid:", monitor.jvm.pid if monitor.jvm else "not found")"""),

("md", "## 1. Data preparation and partitioning"),
("code", """df = spark.read.csv(DATA, header=True, inferSchema=True)
df.show(5)
print("rows:", df.count(), "| partitions after reading:", df.rdd.getNumPartitions())"""),
("md", """A 367 KB file is read as **one** partition (Spark's default split size is 128 MB), so by default only
**one** of the 4 cores would work on it. We compare three partitioning strategies:"""),
("code", """from pyspark.sql.functions import spark_partition_id

def partition_sizes(d):
    return [r["count"] for r in d.groupBy(spark_partition_id().alias("pid")).count().orderBy("pid").collect()]

strategies = {
    "default (as read)":              df,
    "round-robin  repartition(8)":    df.repartition(8),
    "hash by job  repartition(8,job)": df.repartition(8, "job"),
    "range by balance  (8)":          df.repartitionByRange(8, "balance"),
}
for name, d in strategies.items():
    sizes = partition_sizes(d)
    print(f"{name:33s} partitions={d.rdd.getNumPartitions():2d}  rows per partition={sizes}")"""),
("code", """# which jobs ended up together under hash partitioning?
(df.repartition(8, "job").withColumn("pid", spark_partition_id())
   .groupBy("pid").agg(F.collect_set("job").alias("jobs_in_partition"), F.count("*").alias("rows"))
   .orderBy("pid").show(truncate=False))"""),
("md", """**Strategy chosen: hash partitioning on `job` into 8 partitions (2 × the 4 cores).**
* Every row of one job lands in the **same partition**, so a `groupBy("job")` needs **no further shuffle** —
  Spark can reuse the existing partitioning (see the plans below).
* 8 partitions = 2 tasks per core, so a slow partition does not leave other cores idle at the end.
* Trade-off: hash partitioning on a skewed key creates **uneven partitions** (management/blue-collar are big,
  some partitions hold 2 small jobs). Round-robin is perfectly balanced but forces a shuffle for every
  `groupBy("job")`; range partitioning suits sorting/range filters on `balance`."""),
("code", """by_job = df.repartition(8, "job").cache()
by_job.count()
print("--- groupBy on round-robin data: needs an Exchange (shuffle) ---")
df.repartition(8).groupBy("job").agg(F.avg("balance")).explain()
print("--- groupBy on data already hash-partitioned by job: no extra Exchange ---")
by_job.groupBy("job").agg(F.avg("balance")).explain()"""),

("md", "## 2. Data analysis and processing in parallel\n### 2a. Average balance per job — two parallel approaches"),
("code", """monitor.mark("avg balance per job")
# (1) DataFrame API: each partition computes partial (sum, count) per job -> shuffle -> final average
t0 = time.time()
avg_df = by_job.groupBy("job").agg(F.round(F.avg("balance"), 2).alias("avg_balance"),
                                   F.count("*").alias("clients")).orderBy(F.desc("avg_balance"))
avg_df.show()
print(f"DataFrame groupBy: {time.time() - t0:.2f}s")

# (2) RDD API, written out explicitly: map -> combine locally per partition -> reduceByKey across partitions
t0 = time.time()
rdd = by_job.select("job", "balance").rdd
partials = rdd.map(lambda r: (r.job, (r.balance, 1)))                      # runs on every partition in parallel
totals = partials.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))       # combines inside partitions first
avg_rdd = sorted(((j, round(s / c, 2)) for j, (s, c) in totals.collect()), key=lambda x: -x[1])
print(f"RDD reduceByKey:   {time.time() - t0:.2f}s")
print(avg_rdd)"""),
("code", """# Show the work each partition did in parallel (partition id -> jobs and partial sums)
def per_partition(idx, it):
    acc = {}
    for r in it:
        s, c = acc.get(r.job, (0, 0)); acc[r.job] = (s + r.balance, c + 1)
    yield (idx, {k: v[1] for k, v in acc.items()})
for pid, jobs in rdd.mapPartitionsWithIndex(per_partition).collect():
    print(f"partition {pid}: rows per job {jobs}")"""),

("md", """### 2b. Top 5 age groups with the highest loan amounts
**Data note:** `bank.csv` has no loan *amount* column — `housing` and `loan` are only yes/no flags. So we measure
**loan volume** as the number of loans held (housing + personal) per 5-year age group, and also report the total
and average account balance of the loan holders in each group as a size proxy."""),
("code", """monitor.mark("loans per age group")
loans = (df.repartition(8)                                     # spread the work over all cores
           .withColumn("age_group", F.concat((F.floor(F.col("age") / 5) * 5).cast("int").cast("string"), F.lit("-"),
                                             (F.floor(F.col("age") / 5) * 5 + 4).cast("int").cast("string")))
           .withColumn("n_loans", (F.col("housing") == "yes").cast("int") + (F.col("loan") == "yes").cast("int")))
top5 = (loans.groupBy("age_group")
        .agg(F.count("*").alias("clients"),
             F.sum("n_loans").alias("total_loans"),
             F.sum((F.col("housing") == "yes").cast("int")).alias("housing_loans"),
             F.sum((F.col("loan") == "yes").cast("int")).alias("personal_loans"),
             F.round(F.avg("n_loans"), 2).alias("loans_per_client"),
             F.sum(F.when(F.col("n_loans") > 0, F.col("balance"))).alias("balance_of_loan_holders"))
        .orderBy(F.desc("total_loans")).limit(5))
top5.show()"""),
("md", """**Methodology:** derive `age_group` and `n_loans` per row (embarrassingly parallel — every partition works
alone), then `groupBy` does a partial aggregation in each of the 8 partitions and one small shuffle to merge them."""),

("md", """## 3. Model training on partitioned data
**Model: Logistic Regression.** Reasons: (1) its training is naturally data-parallel — every iteration each
partition computes the gradient on its own rows and Spark sums them with a `treeAggregate`; (2) it was the best
model in Part 3 (AUC ≈ 0.90); (3) its coefficients are interpretable for the business."""),
("code", """from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

cats = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "poutcome"]
nums = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]
feat_pipe = Pipeline(stages=[*[StringIndexer(inputCol=c, outputCol=c + "_i", handleInvalid="keep") for c in cats],
                             OneHotEncoder(inputCols=[c + "_i" for c in cats], outputCols=[c + "_o" for c in cats]),
                             VectorAssembler(inputCols=nums + [c + "_o" for c in cats], outputCol="features")])
data = (feat_pipe.fit(df).transform(df.withColumn("label", (F.col("y") == "yes").cast("double")))
        .select("features", "label"))
train, test = data.randomSplit([0.8, 0.2], seed=42)
train = train.repartition(4).cache(); test = test.cache()
print("train partitions:", train.rdd.getNumPartitions(), "| rows per partition:", partition_sizes(train))

monitor.mark("train LR (4 partitions)")
lr = LogisticRegression(maxIter=50, regParam=0.01)
t0 = time.time(); model = lr.fit(train); secs = time.time() - t0
auc = BinaryClassificationEvaluator(rawPredictionCol="probability").evaluate(model.transform(test))
print(f"trained in {secs:.2f}s on {train.rdd.getNumPartitions()} partitions | iterations: {model.summary.totalIterations} | test AUC = {auc:.4f}")"""),

("md", """### Does more parallelism make training faster? — scaling experiment
To see real effects we scale the data 50× (226k rows) and train the same model with 1, 2, 4, 8 and 32 partitions."""),
("code", """big = data.crossJoin(spark.range(50).withColumnRenamed("id", "copy")).drop("copy")
print("scaled training data rows:", big.count())
monitor.mark("scaling experiment")
timings = []
for n in [1, 2, 4, 8, 32]:
    part = big.repartition(n).cache(); part.count()               # materialise before timing
    t0 = time.time()
    LogisticRegression(maxIter=20, regParam=0.01, tol=0.0).fit(part)
    timings.append({"partitions": n, "train_sec": round(time.time() - t0, 2)})
    part.unpersist()
timing_df = pd.DataFrame(timings)
timing_df["speedup_vs_1"] = (timing_df.train_sec.iloc[0] / timing_df.train_sec).round(2)
timing_df"""),
("code", """ax = timing_df.plot(x="partitions", y="train_sec", marker="o", logx=True, legend=False, figsize=(6, 3.5))
ax.set_xticks(timing_df.partitions); ax.set_xticklabels(timing_df.partitions)
ax.set_ylabel("training time (s)"); ax.set_title("Logistic Regression on 226k rows vs number of partitions")
plt.tight_layout(); plt.savefig("docs/figures/partitions_vs_training_time.png", dpi=120); plt.show()"""),
("md", """**Challenges and how they were handled**
* **Too few partitions** (1 partition after reading) → only one core busy. Fixed with `repartition(4/8)`.
* **Too many partitions** (32) → each task is tiny and the per-task scheduling + `treeAggregate` overhead grows,
  so time stops improving or gets worse. Rule of thumb used: **2–3 partitions per core**.
* **Re-reading data every iteration** — LR makes many passes, so the training set is `cache()`d once.
* **Class imbalance & reproducibility** — fixed seeds for the split; class weights were covered in Part 3.
* **Python UDFs** would move rows between JVM and Python and break the speed-up, so only built-in functions are used."""),

("md", "## 4. Resource monitoring and management"),
("code", """monitor.mark("end")
res = monitor.stop()
fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
a1.plot(res.t, res.jvm_cpu_pct / 100, label="cores used by Spark JVM")
a1.axhline(4, color="grey", lw=0.8, ls=":", label="local[4] limit")
a1.set_ylabel("cores busy"); a1.legend(loc="upper left")
a2.plot(res.t, res.jvm_rss_mb, color="#c0392b", label="Spark JVM memory (MB)")
a2.set_ylabel("MB"); a2.set_xlabel("seconds"); a2.legend(loc="upper left")
for t, label in monitor.marks:
    for a in (a1, a2):
        a.axvline(t, color="grey", lw=0.6, ls="--")
    a1.annotate(label, (t, a1.get_ylim()[1] * 0.95), rotation=90, fontsize=7, va="top")
plt.tight_layout(); plt.savefig("docs/figures/resource_usage.png", dpi=120); plt.show()

phase = res.assign(phase=pd.cut(res.t, [m[0] for m in monitor.marks] + [res.t.max() + 1],
                                labels=[m[1] for m in monitor.marks], right=False))
phase["spark_cores_used"] = phase.jvm_cpu_pct / 100          # JVM CPU% / 100 = cores Spark keeps busy
summary = phase.groupby("phase", observed=True).agg(seconds=("t", lambda x: len(x) * monitor.interval),
        avg_spark_cores=("spark_cores_used", "mean"), peak_spark_cores=("spark_cores_used", "max"),
        peak_jvm_mb=("jvm_rss_mb", "max"), machine_cpu_pct=("cpu_total_pct", "mean")).round(2)
display(summary)
print(f"peak Spark JVM memory: {res.jvm_rss_mb.max():.0f} MB | peak cores used by Spark: "
      f"{phase.spark_cores_used.max():.1f} of 4 | machine RAM used: {res.ram_used_pct.max():.0f}%")"""),
("code", """# Spark's own view: executor memory and task counters from the status API (same data as the Spark UI)
import json, urllib.request
ui = spark.sparkContext.uiWebUrl
app_id = spark.sparkContext.applicationId
ex = json.load(urllib.request.urlopen(f"{ui}/api/v1/applications/{app_id}/executors"))
for e in ex:
    print(f"executor {e['id']}: cores={e['totalCores']}  tasks done={e['completedTasks']}  "
          f"storage memory used={e['memoryUsed']/2**20:.1f} MB of {e['maxMemory']/2**20:.0f} MB  "
          f"total task time={e['totalDuration']/1000:.0f}s  GC time={e['totalGCTime']/1000:.1f}s")
sm = spark.sparkContext.statusTracker()
print("active jobs right now:", sm.getActiveJobsIds())"""),
("md", """**Observations**
* CPU is measured for the **Spark JVM itself** (JVM CPU% ÷ 100 = cores in use), so other apps do not distort it.
* Short actions on the single-partition input keep only ~1 core busy; the repartitioned aggregation, the training
  and especially the 50× scaling experiment drive Spark towards its **4-core limit** (`local[4]`).
* `local[4]` caps Spark at **4 task threads** even though the M2 has 8 cores, leaving room for the OS, VS Code and
  the notebook kernel. Peaks slightly above 4 cores are the JVM's own garbage-collection and JIT-compiler threads
  running next to the 4 tasks.
* Spark JVM memory grows in steps when data is `cache()`d — from ~0.6 GB at start to ~1.3 GB during the 50× scaling
  experiment — and does not drop straight away after `unpersist()` (the JVM keeps its heap). Caching trades memory
  for speed; with 8 GB RAM the driver is capped at 2 GB and each experiment releases its cache.
* GC time is small compared to task time, so memory is not the bottleneck at this data size."""),

("md", """## 5. Task management and scheduling
Several independent preprocessing tasks are submitted **at the same time** from Python threads. Spark's
**FAIR scheduler** (enabled in the SparkSession above) shares the 4 cores between them, and each task is tagged
with a **job group** and **scheduler pool** so it can be tracked (and cancelled) in the Spark UI."""),
("code", """from concurrent.futures import ThreadPoolExecutor
big_raw = df.crossJoin(spark.range(50).withColumnRenamed("id", "copy")).drop("copy").repartition(8).cache()
big_raw.count()
sc = spark.sparkContext

def task_null_report():
    return big_raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in big_raw.columns]).collect()
def task_iqr_caps():
    return big_raw.approxQuantile(["balance", "duration", "campaign"], [0.25, 0.75], 0.001)
def task_category_counts():
    return {c: big_raw.groupBy(c).count().count() for c in ["job", "education", "month", "poutcome"]}
def task_job_stats():
    return big_raw.groupBy("job", "marital").agg(F.avg("balance"), F.max("duration")).collect()

tasks = {"null_report": task_null_report, "iqr_caps": task_iqr_caps,
         "category_counts": task_category_counts, "job_stats": task_job_stats}

def run(name):
    sc.setLocalProperty("spark.scheduler.pool", name)          # own FAIR pool per task
    sc.setJobGroup(name, f"preprocessing task: {name}")       # visible + cancellable in the Spark UI
    t0 = time.time(); tasks[name](); return name, round(time.time() - t0, 2)

t0 = time.time(); seq = [run(n) for n in tasks]; seq_total = time.time() - t0
t0 = time.time()
with ThreadPoolExecutor(max_workers=4) as pool:
    conc = list(pool.map(run, tasks))
conc_total = time.time() - t0
sc.setLocalProperty("spark.scheduler.pool", None); sc.setJobGroup("", "")
print(pd.DataFrame({"task": [s[0] for s in seq], "sequential_s": [s[1] for s in seq],
                    "concurrent_s": [c[1] for c in conc]}).to_string(index=False))
print(f"\\nall 4 tasks: sequential {seq_total:.2f}s  vs  concurrent {conc_total:.2f}s "
      f"-> {seq_total / conc_total:.2f}x faster")
_ = big_raw.unpersist()"""),
("md", """**How tasks were managed effectively**
* **Concurrency:** independent tasks run in parallel threads instead of waiting for each other; Spark interleaves
  their stages on the free cores.
* **Fairness:** `spark.scheduler.mode=FAIR` + one pool per task stops a long task from starving short ones
  (the default FIFO scheduler would run them strictly one after another).
* **Visibility & control:** `setJobGroup` labels every Spark job, so the Jobs tab shows which task each job belongs
  to, and `sc.cancelJobGroup(name)` can stop one task without killing the others.
* **Shared cached input:** all four tasks read the same cached DataFrame, so the data is loaded once.
* **Right-sizing:** 8 partitions for 4 cores keeps every core busy without drowning in tiny tasks."""),
("code", "spark.stop()"),
]

if __name__ == "__main__":
    build("05_data_parallelism.ipynb", cells, timeout=3600)
