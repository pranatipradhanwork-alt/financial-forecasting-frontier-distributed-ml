"""Part 4 - Real-time transaction analysis with Spark Structured Streaming.

    python 04_streaming/streaming_app.py --part 1   # live averages per job
    python 04_streaming/streaming_app.py --part 2   # live model predictions
    python 04_streaming/streaming_app.py --part 3   # 10 s and 1 min windows (trend analysis)
    python 04_streaming/streaming_app.py --part 4   # late / out-of-order data with watermarking

Use 04_streaming/run_streaming.sh <part>, which starts this app and the producer together.
"""
import argparse
import json
import os
import shutil
import sys
import time

os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STREAM_DIR = os.path.join(ROOT, "04_streaming", "stream_input")
CKPT = os.path.join(ROOT, "04_streaming", "_checkpoints")
RESULTS = os.path.join(ROOT, "docs", "streaming")

SCHEMA = T.StructType([
    T.StructField("txn_id", T.IntegerType()), T.StructField("event_time", T.TimestampType()),
    T.StructField("age", T.IntegerType()), T.StructField("job", T.StringType()),
    T.StructField("marital", T.StringType()), T.StructField("education", T.StringType()),
    T.StructField("default", T.StringType()), T.StructField("balance", T.IntegerType()),
    T.StructField("housing", T.StringType()), T.StructField("loan", T.StringType()),
    T.StructField("contact", T.StringType()), T.StructField("day", T.IntegerType()),
    T.StructField("month", T.StringType()), T.StructField("duration", T.IntegerType()),
    T.StructField("campaign", T.IntegerType()), T.StructField("pdays", T.IntegerType()),
    T.StructField("previous", T.IntegerType()), T.StructField("poutcome", T.StringType()),
    T.StructField("y", T.StringType()),
])


def banner(text):
    print("\n" + "=" * 90 + f"\n{text}\n" + "=" * 90, flush=True)


def read_stream(spark):
    # File source: every new CSV dropped in STREAM_DIR becomes part of the next micro-batch.
    # A streaming source needs an explicit schema (Spark cannot scan an unbounded folder to infer it).
    return (spark.readStream.schema(SCHEMA).option("header", True)
            .option("maxFilesPerTrigger", 1)          # 1 file per micro-batch -> visible, steady flow
            .csv(STREAM_DIR))


# ------------------------------------------------------------------ Part 1
def part1(spark, stream):
    banner("PART 1 - Real-time aggregation: average balance and call duration per job (updated every batch)")
    agg = (stream.groupBy("job")
           .agg(F.count("*").alias("transactions"),
                F.round(F.avg("balance"), 2).alias("avg_balance"),
                F.round(F.avg("duration"), 1).alias("avg_duration_sec")))

    def show(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        total = batch_df.agg(F.sum("transactions")).first()[0]
        print(f"\n----- micro-batch {batch_id}: running totals after {total} transactions -----", flush=True)
        batch_df.orderBy(F.desc("transactions")).show(15, truncate=False)
        batch_df.toPandas().to_csv(os.path.join(RESULTS, "part1_job_aggregates_latest.csv"), index=False)

    # complete mode = the whole (small) result table is re-emitted every batch
    return [agg.writeStream.outputMode("complete").foreachBatch(show)
            .option("checkpointLocation", f"{CKPT}/p1").trigger(processingTime="2 seconds").start()]


# ------------------------------------------------------------------ Part 2
def load_or_train_model(spark):
    """Use the tuned pipeline from Part 3; if it is missing, train a quick one on the historical CSV."""
    from pyspark.ml import PipelineModel
    path = os.path.join(ROOT, "models", "term_deposit_best")
    meta = os.path.join(ROOT, "models", "preprocessing.json")
    if os.path.exists(path) and os.path.exists(meta):
        info = json.load(open(meta))
        print(f"loaded model from Part 3: {info['model']}  (models/term_deposit_best)", flush=True)
        return PipelineModel.load(path), info["cap_fences"]

    print("Part 3 model not found -> training a Logistic Regression on data/bank.csv", flush=True)
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
    cats = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "poutcome"]
    nums = ["age", "balance", "day", "duration", "campaign", "pdays", "previous", "was_contacted_before"]
    hist = (spark.read.csv(os.path.join(ROOT, "data", "bank.csv"), header=True, inferSchema=True)
            .withColumn("was_contacted_before", (F.col("pdays") != -1).cast("double"))
            .withColumn("label", (F.col("y") == "yes").cast("double")))
    pipe = Pipeline(stages=[*[StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cats],
                            OneHotEncoder(inputCols=[c + "_idx" for c in cats], outputCols=[c + "_ohe" for c in cats]),
                            VectorAssembler(inputCols=nums + [c + "_ohe" for c in cats], outputCol="raw_features"),
                            StandardScaler(inputCol="raw_features", outputCol="features"),
                            LogisticRegression(labelCol="label", regParam=0.01)])
    return pipe.fit(hist), {}


def part2(spark, stream):
    banner("PART 2 - Real-time model predictions: every incoming transaction gets a subscribe/no prediction")
    model, fences = load_or_train_model(spark)
    feats = stream.withColumn("was_contacted_before", (F.col("pdays") != -1).cast("double"))
    for c, (lo, hi) in fences.items():           # same outlier caps as training
        feats = feats.withColumn(c, F.least(F.greatest(F.col(c).cast("double"), F.lit(lo)), F.lit(hi)))
    p_yes = F.udf(lambda v: float(v[1]), T.DoubleType())
    scored = (model.transform(feats)
              .withColumn("p_subscribe", F.round(p_yes("probability"), 3))
              .withColumn("predicted", F.when(F.col("prediction") == 1, "yes").otherwise("no"))
              .select("txn_id", "event_time", "age", "job", "balance", "duration", "y", "predicted", "p_subscribe"))

    totals = {"n": 0, "correct": 0, "tp": 0, "fp": 0, "fn": 0}

    def show(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df = batch_df.cache()
        m = batch_df.agg(F.count("*").alias("n"),
                         F.sum((F.col("y") == F.col("predicted")).cast("int")).alias("correct"),
                         F.sum(((F.col("y") == "yes") & (F.col("predicted") == "yes")).cast("int")).alias("tp"),
                         F.sum(((F.col("y") == "no") & (F.col("predicted") == "yes")).cast("int")).alias("fp"),
                         F.sum(((F.col("y") == "yes") & (F.col("predicted") == "no")).cast("int")).alias("fn")).first()
        for k in totals:
            totals[k] += m[k]
        rec = totals["tp"] / max(totals["tp"] + totals["fn"], 1)
        prec = totals["tp"] / max(totals["tp"] + totals["fp"], 1)
        print(f"\n----- micro-batch {batch_id}: {m['n']} new transactions scored | batch accuracy "
              f"{m['correct'] / m['n']:.1%} | running: {totals['n']} scored, accuracy "
              f"{totals['correct'] / totals['n']:.1%}, recall(yes) {rec:.1%}, precision(yes) {prec:.1%}", flush=True)
        batch_df.orderBy(F.desc("p_subscribe")).show(8, truncate=False)      # top leads in this batch
        batch_df.write.mode("append").option("header", True).csv(os.path.join(RESULTS, "part2_predictions"))
        batch_df.unpersist()

    return [scored.writeStream.outputMode("append").foreachBatch(show)
            .option("checkpointLocation", f"{CKPT}/p2").trigger(processingTime="2 seconds").start()]


# ------------------------------------------------------------------ Part 3
def part3(spark, stream):
    banner("PART 3 - Window operations: transaction count and average balance per 10 s and per 1 min window")
    wm = stream.withWatermark("event_time", "30 seconds")
    w10 = (wm.groupBy(F.window("event_time", "10 seconds"))
           .agg(F.count("*").alias("transactions"), F.round(F.avg("balance"), 1).alias("avg_balance"),
                F.round(100 * F.avg((F.col("y") == "yes").cast("int")), 1).alias("subscribe_pct")))
    w60 = (wm.groupBy(F.window("event_time", "1 minute", "20 seconds"))      # sliding: 1 min long, every 20 s
           .agg(F.count("*").alias("transactions"), F.round(F.avg("balance"), 1).alias("avg_balance")))

    def sink(name):
        def show(batch_df, batch_id):
            if batch_df.isEmpty():
                return
            out = (batch_df.select(F.date_format("window.start", "HH:mm:ss").alias("window_start"),
                                   F.date_format("window.end", "HH:mm:ss").alias("window_end"), "*")
                   .drop("window").orderBy("window_start"))
            print(f"\n----- {name} | micro-batch {batch_id} (windows changed in this batch) -----", flush=True)
            out.show(30, truncate=False)
            out.withColumn("batch_id", F.lit(batch_id)).toPandas().to_csv(
                os.path.join(RESULTS, f"part3_{name.split()[0]}.csv"), mode="a", index=False,
                header=not os.path.exists(os.path.join(RESULTS, f"part3_{name.split()[0]}.csv")))
        return show

    # update mode: only windows that changed are emitted; the watermark lets Spark finalise old windows
    return [w10.writeStream.outputMode("update").foreachBatch(sink("10s tumbling windows"))
            .option("checkpointLocation", f"{CKPT}/p3a").trigger(processingTime="4 seconds").start(),
            w60.writeStream.outputMode("update").foreachBatch(sink("1min sliding windows (slide 20s)"))
            .option("checkpointLocation", f"{CKPT}/p3b").trigger(processingTime="4 seconds").start()]


# ------------------------------------------------------------------ Part 4
def part4(spark, stream):
    banner("PART 4 - Late and out-of-order data: the same 10 s window count WITH and WITHOUT a watermark")
    with_wm = (stream.withWatermark("event_time", "30 seconds")
               .groupBy(F.window("event_time", "10 seconds")).agg(F.count("*").alias("transactions")))
    no_wm = stream.groupBy(F.window("event_time", "10 seconds")).agg(F.count("*").alias("transactions"))

    # append mode is only allowed WITH a watermark: a window is written once, when it is final
    final = (with_wm.select(F.date_format("window.start", "HH:mm:ss").alias("window_start"), "transactions")
             .writeStream.outputMode("append").format("memory").queryName("final_windows")
             .option("checkpointLocation", f"{CKPT}/p4a").trigger(processingTime="3 seconds").start())
    # without a watermark Spark must keep every window in state forever (complete mode)
    unbounded = (no_wm.select(F.date_format("window.start", "HH:mm:ss").alias("window_start"), "transactions")
                 .writeStream.outputMode("complete").format("memory").queryName("all_windows")
                 .option("checkpointLocation", f"{CKPT}/p4b").trigger(processingTime="3 seconds").start())
    return [final, unbounded]


def part4_report(spark, queries):
    final, unbounded = queries
    banner("PART 4 - results")
    p = final.lastProgress or {}
    ops = p.get("stateOperators", [{}])
    dropped = sum(o.get("numRowsDroppedByWatermark", 0) for o in ops)
    wm = p.get("eventTime", {}).get("watermark")
    if wm:
        from datetime import datetime, timezone
        wm = datetime.fromisoformat(wm.replace("Z", "+00:00")).astimezone().strftime("%H:%M:%S")
    print("final watermark = max event time seen - 30 s =", wm,
          "-> windows ending before this are final; rows older than this are dropped")
    print("state rows kept   WITH watermark:", sum(o.get("numRowsTotal", 0) for o in ops))
    print("state rows kept WITHOUT watermark:",
          sum(o.get("numRowsTotal", 0) for o in (unbounded.lastProgress or {}).get("stateOperators", [{}])))
    total_dropped = 0
    for q in [final]:
        for prog in q.recentProgress:
            total_dropped += sum(o.get("numRowsDroppedByWatermark", 0) for o in prog.get("stateOperators", []))
    print(f"late rows dropped by the watermark (too late to be counted): {total_dropped} (last batch: {dropped})")
    cmp = spark.sql("""
        SELECT a.window_start, a.transactions AS counted_without_watermark,
               f.transactions AS final_count_with_watermark,
               CASE WHEN f.transactions IS NULL THEN NULL
                    ELSE a.transactions - f.transactions END AS late_rows_dropped,
               CASE WHEN f.transactions IS NULL THEN 'open: watermark has not passed yet'
                    ELSE 'final' END AS status
        FROM all_windows a LEFT JOIN final_windows f USING (window_start)
        ORDER BY a.window_start""")
    cmp.show(60, truncate=False)
    cmp.toPandas().to_csv(os.path.join(RESULTS, "part4_watermark_comparison.csv"), index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", type=int, required=True, choices=[1, 2, 3, 4])
    ap.add_argument("--run-seconds", type=int, default=90)
    args = ap.parse_args()

    os.makedirs(RESULTS, exist_ok=True)
    shutil.rmtree(f"{CKPT}", ignore_errors=True)
    for f in os.listdir(RESULTS):
        if f.startswith(f"part{args.part}"):
            p = os.path.join(RESULTS, f)
            shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
    os.makedirs(STREAM_DIR, exist_ok=True)

    spark = (SparkSession.builder.appName(f"Bank-Streaming-Part{args.part}").master("local[4]")
             .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "Asia/Kolkata")
             .config("spark.hadoop.fs.defaultFS", "file:///")   # local files, even if HADOOP_CONF_DIR points at HDFS
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    stream = read_stream(spark)
    queries = {1: part1, 2: part2, 3: part3, 4: part4}[args.part](spark, stream)
    open(os.path.join(ROOT, "04_streaming", ".app_ready"), "w").write("ok")
    print(f"[app] {len(queries)} streaming quer{'y' if len(queries) == 1 else 'ies'} running; "
          f"Spark UI -> http://localhost:4040 (Structured Streaming tab)", flush=True)

    deadline = time.time() + args.run_seconds
    while time.time() < deadline and all(q.isActive for q in queries):
        time.sleep(1)
    for q in queries:
        q.processAllAvailable() if q.isActive else None
    if args.part == 4:
        part4_report(spark, queries)
    for q in queries:
        q.stop()
    spark.stop()
    print("[app] stopped", flush=True)


if __name__ == "__main__":
    main()
