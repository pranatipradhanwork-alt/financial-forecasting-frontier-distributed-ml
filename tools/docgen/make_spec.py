"""Collect code + real outputs for every question into spec.json (rendered by render.js)."""
import json, os, re
import nbformat

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
P = lambda *a: os.path.join(ROOT, *a)
read = lambda *a: open(P(*a)).read()
NOISE = re.compile(r"WARN|log4j|incubator|setLogLevel|SLF4J|NativeCodeLoader|Picked up")


def trim(text, max_lines=28, note=None):
    lines = [l.rstrip() for l in text.strip("\n").split("\n") if not NOISE.search(l)]
    if len(lines) > max_lines:
        cut = len(lines) - max_lines
        lines = lines[:max_lines] + [note or f"... ({cut} more lines - see screenshot / output file)"]
    return "\n".join(lines)


_nbs = {}
def cell(nb, contains, max_lines=28, which=0):
    """(code, output) of the notebook cell whose source contains `contains`."""
    if nb not in _nbs:
        _nbs[nb] = nbformat.read(P(nb), 4)
    hits = [c for c in _nbs[nb].cells if c.cell_type == "code" and contains in c.source]
    c = hits[which]
    out = []
    for o in c.outputs:
        if o.output_type == "stream" and o.name == "stdout":
            out.append(o.text)
        elif o.output_type in ("execute_result", "display_data"):
            t = o.get("data", {}).get("text/plain", "")
            if t and not t.startswith("<Figure"):
                out.append(t)
    return c.source, trim("\n".join(out), max_lines)


def q(title, answer, code=None, output=None, image=None, shot=None, code_label="Code", out_label="Output"):
    return {"title": title, "answer": answer, "code": code, "output": output, "image": image,
            "shot": shot, "code_label": code_label, "out_label": out_label}


docs = []

# =====================================================================================  HADOOP & HIVE
hout = read("docs", "outputs_hadoop.txt")
hsec = re.split(r"\n?================ ", hout)
hjob = {s.split(":")[0]: s for s in hsec if s.startswith("Job")}
def job_out(j):
    s = hjob[j]
    res = s.split("-- result:")[1].split("\n", 1)[1]
    head = [l.strip() for l in s.split("\n") if re.search(r"Launched map tasks|Map input records|completed successfully", l)]
    head = [re.sub(r"^.*INFO mapreduce.Job: ", "", l) for l in head]
    return "\n".join(head) + "\n\n" + trim(res)
mr = lambda f: read("01_hadoop_hive", "mapreduce", f).strip()
ingest_code = "\n".join(l for l in read("01_hadoop_hive", "run_hadoop.sh").split("\n")
                        if "hdfs dfs -mkdir" in l or "-put" in l or "hdfs dfs -ls" in l or "fsck" in l)
ingest_out = trim(hsec[1].split("DATA INGESTION ================")[-1] if "DATA INGESTION" in hsec[1] else hout.split("Job1")[0], 14)

hive_txt = read("docs", "outputs_hive.txt")
blocks = hive_txt.split("\nhive> ")[1:]
def hv(i, max_lines=30):
    b = blocks[i - 1]
    sql, _, rest = b.partition(";\n")
    sql = "hive> " + sql.replace("\n    > ", "\n    > ") + ";"
    rest = re.split(r"\n\s*--|\n=====", rest)[0]
    return sql, trim(rest, max_lines)
def hv_multi(idxs):
    codes, outs = [], []
    for i in idxs:
        c, o = hv(i, 22)
        codes.append(c); outs.append(o)
    return "\n\n".join(codes), "\n\n".join(outs)

H = []
H.append(q("HADOOP — Data Ingestion: create an HDFS directory and transfer bank.csv",
  "A single-node Hadoop cluster (NameNode, DataNode, ResourceManager, NodeManager) was started with "
  "./start_cluster.sh. The directory /user/<user>/banking/input was created in HDFS and bank.csv copied into it. "
  "The file is stored with a 128 KB block size so HDFS splits it into 3 blocks — like real big data — which later "
  "gives every MapReduce job 3 parallel map tasks.",
  ingest_code, ingest_out, shot="Terminal after ./start_cluster.sh (the 4 daemons) and the DATA INGESTION part of "
  "./01_hadoop_hive/run_hadoop.sh. Optional: HDFS web UI http://localhost:9870 → Utilities → Browse the file system."))
H.append(q("MapReduce — average account balance for each job type",
  "Mapper emits job → balance; Hadoop sorts by job; the reducer sums and counts per job and prints the average. "
  "Retired clients have the highest average balance (2,319.19) and blue-collar the lowest (1,085.16).",
  mr("job1_mapper.py") + "\n\n# ---- job1_reducer.py ----\n" + mr("job1_reducer.py"), job_out("Job1"),
  shot="Job1 section of the run_hadoop.sh output (job completed + result table)."))
H.append(q("MapReduce — individuals with and without a housing loan in each education category",
  "Mapper emits education|housing → 1; the generic count reducer sums the ones. Secondary-educated clients are "
  "most likely to hold a housing loan (1,430 yes vs 876 no, 62%); tertiary is about half and half.",
  mr("job2_mapper.py") + "\n\n# ---- count_reducer.py ----\n" + mr("count_reducer.py"), job_out("Job2"),
  shot="Job2 section of the run_hadoop.sh output."))
H.append(q("MapReduce — clients contacted in each month and their subscription status (y)",
  "Mapper emits month|y → 1 (month prefixed with its number so the output is in calendar order). May is by far the "
  "busiest month (1,305 no / 93 yes) but converts worst; March, October and December have few contacts but almost "
  "half of them subscribe.",
  mr("job3_mapper.py") + "\n\n# reducer: count_reducer.py (same as above)", job_out("Job3"),
  shot="Job3 section of the run_hadoop.sh output."))
H.append(q("Data Analysis with MapReduce — average contact duration per campaign outcome (poutcome)",
  "Mapper emits poutcome → duration; the averaging reducer from job 1 is reused. Clients whose previous campaign "
  "was a success stay on the phone longest (338.6 s) compared with failure (254.4 s).",
  mr("job4_mapper.py") + "\n\n# reducer: job1_reducer.py (average per key)", job_out("Job4"),
  shot="Job4 section of the run_hadoop.sh output."))
H.append(q("Data Analysis with MapReduce — relationship between age and balance (summarised)",
  "Mapper puts every client into a 10-year age band; the reducer reports count, min, average, median and max "
  "balance per band. Findings: average balance rises steadily with age up to 70-79 (3,280.87) and then falls; medians "
  "are far below averages in every band, so a few very rich clients pull the averages up; negative balances "
  "(overdrafts) appear only below age 70.",
  mr("job5_mapper.py") + "\n\n# ---- job5_reducer.py ----\n" + mr("job5_reducer.py"), job_out("Job5"),
  shot="Job5 section of the run_hadoop.sh output. Optional: YARN UI http://localhost:8088 showing the 5 finished jobs."))

hql = read("01_hadoop_hive", "hive", "banking_analysis.hql")
create_code = hql[hql.index("CREATE DATABASE"):hql.index("-- ---------- Q2")].strip()
_, describe_out = hv(6)
H.append(q("HIVE — Data Ingestion and Table Creation: database banking_data, table client_info, load bank.csv",
  "Hive runs as HiveServer2 (./start_hive.sh) with its warehouse on HDFS. run_hive.sh copies bank.csv inside HDFS "
  "without the header line, then the statements below create the database and the typed table and load the data. "
  "Numbers are INT, categories STRING; `default` needs back-ticks because it is a reserved word.",
  create_code, describe_out, shot="Terminal output of ./01_hadoop_hive/run_hive.sh — top part (CREATE / LOAD / DESCRIBE). "
  "Optional: open beeline (beeline -u jdbc:hive2://localhost:10000 -n $USER) and run SHOW TABLES; DESCRIBE client_info;"))
for title, ans, idx in [
  ("Basic Data Exploration — total number of clients", "The table holds 4,521 clients.", [7]),
  ("Basic Data Exploration — first 10 rows of the dataset", "SELECT * with LIMIT 10.", [8]),
  ("Data Filtering and Sorting — married clients who have a personal loan",
   "453 clients are married and have a personal loan; the second query lists every record (first rows shown).", [9, 10]),
  ("Data Filtering and Sorting — top 10 clients with the highest balance (job, marital status, balance)",
   "The richest client is a married retired person with 71,188; 3 of the top 10 are retired.", [11]),
  ("Data Aggregation and Grouping — average age of clients for each job category",
   "Retired clients are the oldest (61.9 years on average), students the youngest (26.8).", [12]),
  ("Data Aggregation and Grouping — clients who defaulted on credit, per education level",
   "Only 76 clients are in default; most have secondary education (46).", [13]),
  ("Complex Queries — top 5 jobs by average balance and % of them who subscribed",
   "Retired clients have the highest average balance (2,319) and the highest subscription rate in the top 5 (23.5%); "
   "students are 5th by balance but subscribe at 22.6%.", [14]),
  ("Complex Queries — month with the most contacts and its success rate",
   "May has the most contacts (1,398) but only a 6.65% success rate.", [15]),
  ("Correlation Analysis — correlation between age and balance",
   "CORR(age, balance) = 0.0838: a very weak positive linear relationship.", [16]),
  ("Trend Analysis — year-over-year trend in the number of clients contacted",
   "bank.csv has day and month but NO year column, so a true year-over-year trend cannot be computed from this file. "
   "The query shows the trend along the time axis the data does have — contacts and success rate per calendar month, "
   "with LAG() giving the change versus the previous month. Contacts jump from 293 in April to 1,398 in May (+1,105) "
   "and then fall; the success rate moves opposite to volume (6.65% in May vs 42.9% in March and 46.3% in October).", [17]),
  ("Anomaly Detection — unusual patterns in average yearly balance across education levels",
   "Step 1 shows the spread per education level, step 2 counts outliers (> mean + 3 std) and negative balances. "
   "Unusual patterns: primary education has the largest spread (std 3,711) and the single largest balance (71,188) "
   "despite a low median; tertiary has the most high outliers (41) and the lowest minimum (-3,313); secondary has the "
   "most overdrawn clients (228). In every group the mean is 3-4x the median — balance is heavily right-skewed.", [18, 19]),
  ("Advanced Analysis — subscription rate for each previous-campaign outcome (poutcome)",
   "A previous success is the strongest signal: 64.3% of those clients subscribe again, against 9.1% for clients "
   "never contacted before (unknown).", [20]),
  ("Advanced Analysis — average contact duration: subscribed vs not subscribed",
   "Subscribers' calls last 552.7 s on average versus 226.4 s for non-subscribers (medians 442 s vs 167 s).", [21]),
]:
    c, o = hv_multi(idx) if len(idx) > 1 else hv(idx[0])
    H.append(q("HIVE — " + title, ans, c, o, shot="This query and its result table in the run_hive.sh output "
               "(or run it yourself in beeline)."))
docs.append({"file": "1_Hadoop_Hive_Answers.docx", "title": "Data Analysis and Management using Hadoop & Hive",
             "intro": "Single-node Hadoop 3.5.0 cluster (HDFS + YARN) and Hive 4.2.1 (HiveServer2) on a MacBook Air M2, "
                      "run from the VS Code terminal. Scripts: 01_hadoop_hive/. Full output: docs/outputs_hadoop.txt, "
                      "docs/outputs_hive.txt.", "sections": H})

# =====================================================================================  EDA
E, NB = [], "02_eda_spark.ipynb"
def e(title, ans, contains, shot=None, image=None, extra=None, max_lines=28):
    c, o = cell(NB, contains, max_lines)
    if extra:
        c2, o2 = cell(NB, extra, max_lines)
        c, o = c + "\n\n" + c2, (o + "\n\n" + o2).strip()
    E.append(q(title, ans, c, o, image, shot or "The notebook cell(s) for this question in VS Code — code and output visible."))
E.append(q("Spark session (setup used by every question)",
  "Spark 4.0.1 runs in local mode with 4 worker threads (local[4]): the DataFrame is split into partitions and each "
  "partition is processed by a separate task in parallel — the same code runs unchanged on a cluster.",
  *cell(NB, "SparkSession.builder"), shot="First cell of 02_eda_spark.ipynb with its output."))
e("Data Loading and Basic Inspection — load bank.csv into a Spark DataFrame, show rows, schema, summary",
  "4,521 rows and 17 columns loaded with schema inference and cached in memory. 7 columns are integers and 10 are "
  "strings; there are no nulls (categorical gaps are written as 'unknown').", 'option("inferSchema", True)',
  extra="df.printSchema()")
e("Basic Inspection — summary of numerical columns", "Balance is very skewed (mean 1,423, median 444, max 71,188); "
  "pdays is -1 for clients never contacted before.", 'summary("count"')
e("Data Filtering — clients with balance greater than 1000",
  "1,481 of 4,521 clients have a balance above 1,000 (threshold kept in a variable).", "THRESHOLD = 1000")
e("Column Operations — month number and quarter of the year",
  "The month name is mapped to its number with create_map, then quarter = ceil(month/3). Q2 has the most contacts "
  "(2,222) because of May.", "month_map = F.create_map")
e("GroupBy and Aggregation — average balance and median age per job",
  "Retired: highest average balance (2,319) and median age 59; students: median age 26.", 'F.median("age")')
e("GroupBy and Aggregation — clients per marital status who subscribed",
  "Married 277, single 167, divorced 77 subscribers.", 'groupBy("marital").count()')
e("How Spark executes a groupBy (distributed plan)",
  "explain() shows partial aggregation inside every partition, a shuffle (Exchange) by job, then the final "
  "aggregation — only small partial results move between tasks.", '.explain()', max_lines=22)
e("UDF to Categorise Age Groups ('<30', '30-60', '>60')",
  "Clients over 60 subscribe at 37.8% — almost 4x the 30-60 group (10.2%). A built-in when() version gives the same "
  "result and is faster because it stays inside the JVM.", "def age_group(age)", extra="df_builtin")
e("Advanced Transformations — subscription rate for each education level",
  "Tertiary 14.3%, secondary 10.6%, unknown 10.2%, primary 9.4%.", 'groupBy("education")')
e("Advanced Transformations — top 3 professions with the highest loan default rate",
  "Entrepreneur 4.17%, unemployed 2.34%, self-employed 2.19%.", "default_rate_pct")
e("String Manipulation — job_marital column and upper-case contact",
  "job and marital joined with concat_ws; contact converted with upper().", 'F.concat_ws("_", "job", "marital")')
e("Data Visualisation — convert to pandas and bar plot of clients by job",
  "Only the small aggregated table is converted for the chart (a full toPandas() on billions of rows would crash "
  "the driver); the full DataFrame is also converted as the task asks (fine at 4.5k rows).", 'job_counts = df.groupBy("job")',
  extra="pdf = df.toPandas()", image="clients_by_job.png")
e("Complex Queries — month with the most contacts and its success rate",
  "May: 1,398 contacts, 6.65% success. Low-volume months convert best (Oct 46.3%, Dec 45%, Mar 42.9%).",
  "Busiest month")
E[-1]["image"] = "month_contacts_vs_success.png"
e("Complex Queries — average contact duration: subscribed vs not",
  "552.7 s vs 226.3 s — subscribers' calls are about 2.4x longer.", 'F.round(F.avg("duration"), 1)')
e("Correlation Between Age and Balance",
  "Pearson r = 0.0838 — almost no linear relationship.", 'df.stat.corr("age", "balance")', image="age_vs_balance.png")
e("Exploring Loan Defaults — proportion of defaulters and chart",
  "98.3% no default (4,445) vs 1.7% default (76).", "defaults = (df.groupBy", image="defaults.png")
e("Contact Method Analysis — success rate per contact method",
  "Telephone 14.62% and cellular 14.36% convert about equally; unknown contact only 4.61%.", 'groupBy("contact")')
e("Data Processing with Spark SQL — temporary view, avg balance per age group and most common jobs",
  "Clients over 60 have the highest average balance (2,676); management and blue-collar are the most common jobs.",
  "SELECT age_group", extra="ORDER BY clients DESC")
docs.append({"file": "2_EDA_Spark_Answers.docx", "title": "Exploratory Data Analysis (EDA) with Spark",
             "intro": "Apache Spark 4.0.1 (PySpark) in VS Code, local[4] = 4 parallel tasks. Notebook: 02_eda_spark.ipynb "
                      "(run in VS Code, not Colab).", "sections": E})

# =====================================================================================  ML
M, NB = [], "03_spark_ml.ipynb"
def m(title, ans, contains, shot=None, image=None, extra=None, max_lines=30):
    c, o = cell(NB, contains, max_lines)
    if extra:
        c2, o2 = cell(NB, extra, max_lines)
        c, o = c + "\n\n" + c2, (o + "\n\n" + o2).strip()
    M.append(q(title, ans, c, o, image, shot or "The notebook cell(s) for this step in VS Code — code and output visible."))
m("Data Loading and Initial Exploration — schema and first rows",
  "4,521 rows; target y is imbalanced: 4,000 no (88.5%) vs 521 yes (11.5%).", "df = spark.read.csv(DATA",
  extra='df.groupBy("y").count()')
m("Data Preprocessing — handle missing values",
  "No column has nulls; 'unknown' is kept as a real category (for poutcome it means 'never contacted'). A fill step "
  "(median for numbers, 'unknown' for categories) still protects the pipeline from future nulls.", "null_counts = df.select")
m("Data Preprocessing — handle outliers",
  "IQR fences from exact quartiles. balance (506 outliers), duration (330) and campaign (318) are capped instead of "
  "deleted so no real client is thrown away; pdays/previous are left alone because -1/0 are codes.", "cap_cols = ")
m("Data Preprocessing — convert categorical variables (StringIndexer + OneHotEncoder)",
  "9 categorical columns: StringIndexer (handleInvalid='keep') → OneHotEncoder. Also added was_contacted_before, the "
  "0/1 label and balanced class weights (yes 4.34, no 0.57) because only 11.5% are positives.", "was_contacted_before")
m("Feature Engineering — feature vector with VectorAssembler",
  "8 numeric columns + 9 one-hot vectors → one 61-dimensional vector, then StandardScaler. All steps form one Pipeline.",
  "feature_inputs = num_cols")
m("Model Training and Selection — models, train/test split, training",
  "80/20 split with seed 42 (3,662 train / 859 test). Four classifiers trained with class weights: Logistic Regression, "
  "Decision Tree, Random Forest and Gradient-Boosted Trees.", "train, test = df.randomSplit", max_lines=8)
m("Model Evaluation — metrics on the test set",
  "Accuracy is misleading here (always 'no' = 88.5%), so models are compared on AUC-ROC and on precision, recall and "
  "F1 for the 'yes' class. Logistic Regression is best before tuning (AUC 0.892, recall 0.849).",
  "results, fitted = [], {}", extra="confusion matrix")
m("Hyperparameter Tuning — ParamGridBuilder and CrossValidator",
  "3-fold cross-validation on the training set, 4 parameter combinations trained in parallel. Best LR: regParam 0.01, "
  "elasticNet 0.5; best RF: 50 trees, depth 10, minInstancesPerNode 5. Tuned Logistic Regression wins on the test "
  "set: AUC-ROC 0.897, recall 0.83, F1 0.54.", "lr_grid = (ParamGridBuilder()", extra="comparison = pd.concat",
  image="roc_curves.png")
m("Business view — choosing the decision threshold",
  "At threshold 0.5 the bank would call 203 of 859 clients and reach 82 of the 99 real subscribers; a higher "
  "threshold means fewer calls but more missed subscribers.", "for t in [0.3, 0.4")
m("Advanced Analysis — feature importances (Random Forest)",
  "Call duration dominates (0.42), followed by age, balance, day, contact = unknown and poutcome = success.",
  "rf_model = tuned", image="feature_importance_rf.png")
m("Advanced Analysis — coefficients (Logistic Regression)",
  "Positive: duration, poutcome = success, months Oct/Jun/Mar, retired and student. Negative: contact = unknown, "
  "blue-collar, more calls in this campaign, May, personal loan.", "coef = pd.Series")
m("Advanced Analysis — leakage check without call duration",
  "duration is only known after the call, so it cannot be used to choose whom to call. Without it AUC drops from "
  "0.897 to 0.690 — this honest pre-call model is the one to use for targeting.", "prep_nodur")
m("Saving the best model (used by the streaming application)",
  "The tuned pipeline and the outlier caps are saved and reloaded by the Spark Streaming app.", "best_model.write()")
docs.append({"file": "3_Spark_ML_Answers.docx", "title": "Predictive Modeling for Banking Trends — Machine Learning with Spark ML",
             "intro": "Goal: predict whether a client subscribes to a term deposit (binary classification). Notebook: "
                      "03_spark_ml.ipynb, run in VS Code with Spark 4.0.1.", "sections": M})

# =====================================================================================  STREAMING
app = read("04_streaming", "streaming_app.py")
def fn(name, end_marker):
    s = app.index(f"def {name}(")
    return app[s:app.index(end_marker, s)].rstrip()
def sout(part, start=None, max_lines=40):
    t = read("docs", f"outputs_streaming_part{part}.txt")
    if start:
        t = t[t.index(start):]
    return trim(t, max_lines)
S = []
S.append(q("Simulated real-time feed — producer (breaks bank.csv into chunks)",
  "producer.py shuffles bank.csv, cuts it into 29 files of 150 rows and writes one file every 2 seconds into a folder "
  "that Spark watches. Each row gets txn_id and event_time (each file = 10 s of event time). To make it realistic, "
  "about 5% of rows are late / out-of-order (15-90 s old) and one file is a 3x traffic burst. Files are written to a "
  "temp name and renamed so Spark never reads half a file.",
  read("04_streaming", "producer.py").split('def main():')[1].split("if __name__")[0].strip()[:3200],
  trim("\n".join(l for l in read("docs", "outputs_streaming_part1.txt").split("\n") if l.startswith("[producer]")), 14),
  shot="Terminal running ./04_streaming/run_streaming.sh 1 showing [producer] lines."))
S.append(q("1. Stream Processing and Data Aggregation — average balance and duration per job, in real time",
  "Spark Structured Streaming (the current streaming API in Spark 4) reads the folder as a stream with an explicit "
  "schema. groupBy(job) keeps a running count, average balance and average duration; complete mode re-emits the "
  "table after every micro-batch. After the last batch the numbers equal the batch Hadoop/Hive results exactly "
  "(e.g. management 1,766.93, retired 2,319.19) — streaming and batch agree.",
  "def read_stream(spark):" + app.split("def read_stream(spark):")[1].split("# ----")[0].rstrip() + "\n\n\n" + fn("part1", "# ----"),
  sout(1, "----- micro-batch 0", 22) + "\n\n...\n\n" + trim(read("docs", "outputs_streaming_part1.txt").split("----- micro-batch 28")[1], 18),
  shot="Terminal output of ./04_streaming/run_streaming.sh 1 — one early micro-batch and the last one."))
S.append(q("2. Real-Time Model Predictions — trained model integrated into the stream",
  "The model is the tuned Logistic Regression pipeline trained on the historical bank.csv in Part 3 (if it is missing, "
  "the app trains one on the CSV first). The same outlier caps are applied, and every incoming transaction gets "
  "'predicted' (yes/no) and p_subscribe. Each batch prints its most promising leads and running metrics. After all "
  "4,521 transactions: accuracy 82.9%, recall (yes) 82.0%, precision (yes) 38.5%. (The stream replays bank.csv, so "
  "most rows were also training rows; the Part 3 test set is the unbiased estimate.)",
  fn("load_or_train_model", "def part2") + "\n\n\n" + fn("part2", "# ----"),
  sout(2, "loaded model", 26) + "\n\n...\n" + "\n".join(l for l in read("docs", "outputs_streaming_part2.txt").split("\n") if "micro-batch 2" in l and "scored" in l)[-600:],
  shot="Terminal output of ./04_streaming/run_streaming.sh 2 — a micro-batch with its prediction table and running accuracy."))
S.append(q("3. Window Operations and Trend Analysis — 10 s and 1 min windows",
  "With a 30 s watermark, two windowed aggregations run at once: 10-second tumbling windows (count, average balance, "
  "% subscribed) and 1-minute sliding windows that move every 20 s (count, average balance). Update mode prints only "
  "the windows that changed in each batch; all results are also saved to docs/streaming/part3_*.csv.",
  fn("part3", "# ----"), sout(3, "----- 10s tumbling windows | micro-batch 1", 34),
  shot="Terminal output of ./04_streaming/run_streaming.sh 3 — a 10s window table and a 1min window table."))
S.append(q("3. Brief analysis of the trends observed",
  "• Steady flow: the 28 normal 10-second windows hold 142-151 transactions (median 146), average balance 1,415 ± 265, "
  "subscription rate 7-16%.\n"
  "• Traffic spike: one window carried 429 transactions (about 3x normal). The 1-minute sliding windows covering it "
  "rise from about 900 to about 1,170 transactions and fall back after it slides out — a sliding window spreads a "
  "spike over 3 windows, a tumbling window isolates it.\n"
  "• Balance anomalies: two windows had an average balance more than 2 standard deviations above normal (2,000 and "
  "2,272 vs 1,415), caused by a few very rich clients — a live dashboard would flag these.\n"
  "• Out-of-order data: tiny windows before the first real window (1-6 rows, balances 0 to 4,799) are late events that "
  "arrived with old timestamps; they land in the correct historical window, not the current one.\n"
  "• The final window has only 20 rows because the producer stopped mid-window.",
  shot=None))
S.append(q("4. Handling Late and Out-of-Order Data — watermarking",
  "Approach: the same 10-second window count runs twice. WITH withWatermark('event_time', '30 seconds') in append "
  "mode, Spark writes each window once, when it is final, and drops rows that arrive more than 30 s after their "
  "window closed. WITHOUT a watermark (complete mode) Spark must keep every window in memory forever.\n"
  "Result: 62 of the 224 late rows were too late and were dropped; the other 162 arrived within the 30 s allowance and "
  "were counted in their correct window even though they arrived out of order. Spark kept only 4 windows in state "
  "with the watermark versus 35 without. The last windows are 'open' because the watermark had not passed them when "
  "the stream ended.\n"
  "Why it improves accuracy: event-time windows put each transaction where it happened, not where it arrived, so "
  "delayed transactions still count in the right time bucket. The watermark is the explicit trade-off — wait up to "
  "30 s for stragglers (accuracy), then close the window to free memory and publish a final number (latency).",
  fn("part4", "def part4_report") + "\n\n\n" + fn("part4_report", "def main"),
  sout(4, "PART 4 - results", 32),
  shot="Terminal output of ./04_streaming/run_streaming.sh 4 — the PART 4 results table. Optional: Spark UI "
  "http://localhost:4040 → Structured Streaming tab while it runs."))
docs.append({"file": "4_Streaming_Answers.docx", "title": "Real-Time Machine Learning with Spark Streaming",
             "intro": "Spark 4.0.1 Structured Streaming, run locally from the VS Code terminal. Code: 04_streaming/ "
                      "(producer.py, streaming_app.py, run_streaming.sh). Full output: docs/outputs_streaming_part1-4.txt.",
             "sections": S})

# =====================================================================================  PARALLELISM
D, NB = [], "05_data_parallelism.ipynb"
def d(title, ans, contains, shot=None, image=None, extra=None, max_lines=30):
    c, o = cell(NB, contains, max_lines)
    if extra:
        c2, o2 = cell(NB, extra, max_lines)
        c, o = c + "\n\n" + c2, (o + "\n\n" + o2).strip()
    D.append(q(title, ans, c, o, image, shot or "The notebook cell(s) for this step in VS Code — code and output visible."))
d("Data Preparation — load bank.csv and inspect the first rows",
  "The 367 KB file is read as ONE partition (Spark's default split size is 128 MB), so by default only one of the 4 "
  "cores would work.", "df = spark.read.csv(DATA")
d("Partitioning — divide the dataset into partitions for parallel processing",
  "Strategy used: hash partitioning on job into 8 partitions (2 per core). All rows of one job land in the same "
  "partition, so groupBy(job) needs no extra shuffle — the plan for round-robin data has an Exchange, the plan for "
  "job-partitioned data does not. 8 partitions keep all 4 cores busy. Trade-off: the job key is uneven, so partitions "
  "are uneven (round-robin is perfectly balanced but needs a shuffle for every groupBy(job)).",
  "def partition_sizes", extra="collect_set")
d("Partitioning — proof: shuffle removed from the plan", "Compare the two physical plans.", "by_job = df.repartition(8",
  max_lines=34)
d("Parallel Processing — average balance for each job category",
  "Two parallel approaches with identical results: the DataFrame groupBy (partial aggregate per partition → shuffle → "
  "final average) and an explicit RDD map → reduceByKey that combines (sum, count) inside each partition before "
  "shuffling. mapPartitionsWithIndex shows exactly which jobs each partition processed.",
  "avg_df = by_job.groupBy", extra="def per_partition")
d("Parallel Processing — top 5 age groups with the highest loan amounts",
  "Data note: bank.csv has no loan-amount column (housing and loan are yes/no), so loan volume = number of loans "
  "(housing + personal) per 5-year age group, plus the total balance of loan holders as a size proxy. Methodology: "
  "derive age_group and n_loans per row (every partition works alone), then groupBy does a partial aggregation per "
  "partition and one small shuffle. Top group: 30-34 with 807 loans.", "loans = (df.repartition(8)")
d("Model Training on Partitioned Data — model choice and training on partitions",
  "Logistic Regression: each iteration computes gradients on every partition in parallel and Spark sums them with "
  "treeAggregate; it was also the best model in Part 3 and is interpretable. The training set is repartitioned into "
  "4 balanced partitions and cached; test AUC 0.882.", "feat_pipe = Pipeline")
d("Model Training — scaling experiment (1 to 32 partitions on 226k rows)",
  "On a 50x copy (226,050 rows), 4 partitions trained 2.1x faster than 1, but 32 partitions was no faster than 1 "
  "because tiny tasks add scheduling and aggregation overhead. Challenges and fixes: one input partition (repartition), "
  "repeated passes over data (cache), too many partitions (2-3 per core), Python UDFs avoided in hot paths.",
  "big = data.crossJoin", image="partitions_vs_training_time.png")
d("Resource Monitoring — CPU and memory during processing and training",
  "A background psutil thread samples the Spark JVM every 0.5 s, and Spark's status REST API reports executor memory, "
  "tasks and GC time. Observations: Spark averages 2.4-3 cores on small steps and reaches its 4-core limit in the "
  "aggregation and scaling phases (short peaks above 4 are JVM garbage-collection/JIT threads); JVM memory grows in "
  "steps as data is cached (about 620 MB → 1.44 GB) with the driver capped at 2 GB; GC time (0.3 s) is tiny compared "
  "with task time (16 s), so memory is not the bottleneck.", "res = monitor.stop()", extra="api/v1/applications",
  image="resource_usage.png")
d("Task Management and Scheduling — multiple parallel preprocessing tasks",
  "Four independent preprocessing tasks run first one after another, then concurrently from 4 Python threads. Each "
  "task has its own FAIR scheduler pool and job group (visible and cancellable in the Spark UI) and all share the same "
  "cached DataFrame. Result: 1.02 s sequential vs 0.27 s concurrent (3.8x faster).", "from concurrent.futures")
docs.append({"file": "5_Data_Parallelism_Answers.docx", "title": "Efficient Data Handling through Data Parallelism",
             "intro": "Spark 4.0.1 in VS Code, local[4] on a MacBook Air M2 (8 cores, 8 GB). Notebook: "
                      "05_data_parallelism.ipynb. Some experiments use a 50x scaled copy (226,050 rows) so parallelism "
                      "effects are measurable.", "sections": D})

json.dump({"figures": P("docs", "figures"), "docs": docs}, open(os.path.join(os.path.dirname(__file__), "spec.json"), "w"), indent=1)
print({x["file"]: len(x["sections"]) for x in docs})
