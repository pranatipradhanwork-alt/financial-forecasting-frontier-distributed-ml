# Financial Forecasting Frontier: Distributed ML — Project Report

**Dataset:** `bank.csv` — 4,521 clients, 17 columns, target `y` (subscribed to a term deposit: 521 yes / 4,000 no).
**Environment:** MacBook Air M2, 8 GB RAM, macOS · Hadoop 3.5.0 (single-node HDFS + YARN) · Hive 4.2.1
(HiveServer2, Derby metastore) · Apache Spark 4.0.1 (PySpark, `local[4]`) · Python 3.13 · VS Code.

Full console output of every step is in `docs/outputs_hadoop.txt`, `docs/outputs_hive.txt` and
`docs/outputs_streaming_part1-4.txt`. The notebooks contain their own outputs.

---

## 1. Data Analysis and Management — Hadoop & Hive

### 1.1 Hadoop

**Cluster.** A pseudo-distributed cluster runs the four real Hadoop daemons on one machine: NameNode +
DataNode (HDFS) and ResourceManager + NodeManager (YARN). `start_cluster.sh` starts them without SSH, and
the config lives in `hadoop-conf/`.

**Data ingestion** (`01_hadoop_hive/run_hadoop.sh`)
```bash
hdfs dfs -mkdir -p /user/$USER/banking/input
hdfs dfs -D dfs.blocksize=131072 -put -f data/bank.csv /user/$USER/banking/input/
```
The file is stored with a 128 KB block size, so HDFS splits it into **3 blocks** (131,072 + 131,072 +
113,398 bytes). A 367 KB file would normally fit in a single 128 MB block. Splitting it makes it behave
like real big data: every MapReduce job gets **3 input splits and runs 3 map tasks in parallel**.

**MapReduce jobs.** All five are Hadoop Streaming jobs in Python: mapper → shuffle/sort by key → reducer.

| Job | Mapper emits | Reducer computes |
|---|---|---|
| 1 Avg balance per job | `job → balance` | count + average |
| 2 Housing loan per education | `education\|housing → 1` | count |
| 3 Contacts per month × y | `month\|y → 1` | count |
| 4 Avg duration per poutcome | `poutcome → duration` | count + average |
| 5 Age vs balance | `10-year age band → balance` | count, min, avg, median, max |

**Results**

Job 1 — average balance per job: retired 2,319.19 · housemaid 2,083.80 · management 1,766.93 ·
entrepreneur 1,645.12 · student 1,543.82 · unknown 1,501.71 · self-employed 1,392.41 · technician 1,331.00 ·
admin. 1,226.74 · services 1,103.96 · unemployed 1,089.42 · blue-collar 1,085.16.

Job 2 — housing loans per education:

| education | housing = no | housing = yes |
|---|---|---|
| primary | 295 | 383 |
| secondary | 876 | 1,430 |
| tertiary | 687 | 663 |
| unknown | 104 | 83 |

Secondary-educated clients are the most likely to hold a housing loan (62%). Tertiary is roughly half and half.

Job 3 — contacts per month (no / yes): May 1,305 / 93 is by far the busiest month. Mar 28 / 21, Oct 43 / 37
and Dec 11 / 9 have few contacts but almost half of them subscribe.

Job 4 — average contact duration by previous outcome: success 338.6 s · other 273.8 s · unknown 262.1 s ·
failure 254.4 s. Clients whose previous campaign succeeded stay on the phone longest.

Job 5 — age vs balance:

| age band | clients | min | avg | median | max |
|---|---|---|---|---|---|
| 10-19 | 4 | 0 | 393.50 | 202.5 | 1,169 |
| 20-29 | 478 | -1,202 | 1,063.66 | 357.0 | 22,171 |
| 30-39 | 1,808 | -1,053 | 1,298.15 | 427.5 | 27,359 |
| 40-49 | 1,203 | -1,746 | 1,474.69 | 474.0 | 42,045 |
| 50-59 | 854 | -3,313 | 1,547.42 | 455.5 | 27,069 |
| 60-69 | 113 | -396 | 2,619.78 | 768.0 | 71,188 |
| 70-79 | 47 | 0 | 3,280.87 | 1,164.0 | 26,452 |
| 80-89 | 14 | 0 | 2,071.14 | 550.5 | 8,304 |

**Finding:** average balance rises steadily with age up to 70-79, then falls. Medians are much lower than
averages in every band, so a few very rich clients pull the averages up. Negative balances (overdrafts)
occur only below 70.

### 1.2 Hive

Hive runs as **HiveServer2**, the production setup (`start_hive.sh`), with a local Derby metastore and the
warehouse on HDFS. All queries are in `01_hadoop_hive/hive/banking_analysis.hql`. `run_hive.sh` stages the
CSV without its header and sends the statements to HiveServer2. You can also type the same statements into
`beeline -u jdbc:hive2://localhost:10000`.

| Question | Answer |
|---|---|
| Database / table | `banking_data.client_info`, 17 typed columns (INT for numbers, STRING for categories), loaded with `LOAD DATA INPATH` from HDFS |
| Total clients | **4,521** |
| First 10 rows | `SELECT * … LIMIT 10` (see output file) |
| Married with personal loan | **453** clients, all records listed |
| Top 10 balances | retired/married 71,188 · entrepreneur/married 42,045 · technician/single 27,733 · … |
| Avg age per job | retired 61.9 (oldest) … student 26.8 (youngest) |
| Defaulters per education | secondary 46 · tertiary 17 · primary 10 · unknown 3 |
| Top 5 jobs by avg balance + % subscribed | retired 2,319 (23.5%) · housemaid 2,084 (12.5%) · management 1,767 (13.5%) · entrepreneur 1,645 (8.9%) · student 1,544 (22.6%) |
| Month with most contacts | **May — 1,398 contacts, 93 subscribed, 6.65% success** |
| corr(age, balance) | **0.0838**: a very weak positive relationship |
| Subscription rate by poutcome | success **64.3%** · other 19.3% · failure 12.9% · unknown 9.1% |
| Avg call duration | subscribed **552.7 s** vs not subscribed 226.4 s (medians 442 vs 167) |

**Trend analysis.** `bank.csv` has `day` and `month` but **no year column**, so a true year-over-year
trend cannot be computed from this file. The query therefore shows the trend along the time axis the data
does have: contacts and success rate per calendar month, with `LAG()` giving the change from the previous
month. Contacts jump from 293 in April to 1,398 in May (+1,105), then fall to 531 in June. The success rate
moves the opposite way to volume: 6.65% in May, but 42.9% in March, 46.3% in October and 45% in December.

**Anomaly detection** (balance = average yearly balance)

| education | clients | avg | median | std | min | max | outliers > mean + 3σ | negative balances |
|---|---|---|---|---|---|---|---|---|
| tertiary | 1,350 | 1,775 | 584 | 3,459 | -3,313 | 42,045 | 41 | 71 |
| unknown | 187 | 1,701 | 574 | 2,973 | -715 | 21,244 | 4 | 10 |
| primary | 678 | 1,412 | 405 | 3,711 | -966 | **71,188** | 9 | 57 |
| secondary | 2,306 | 1,197 | 386 | 2,419 | -2,082 | 26,452 | 34 | **228** |

Unusual patterns:
1. Primary education has the **largest spread** (std 3,711) and the single largest balance (71,188), even though its median is low. A few extreme accounts make its average misleading.
2. Tertiary has the most high outliers (41) and the lowest minimum (-3,313), so the extremes sit on both sides.
3. Secondary has the **most overdrawn clients** (228, about 10% of the group).
4. In every group the mean is 3–4× the median, so balance is heavily right-skewed. That is why Part 3 caps outliers.

---

## 2. Exploratory Data Analysis with Spark (`02_eda_spark.ipynb`)

Spark runs with `local[4]`: the DataFrame is split into partitions and each partition is a task on one of
the 4 worker threads. `explain()` shows that a `groupBy` runs as a partial aggregation inside every
partition, then a shuffle (`Exchange`), then the final aggregation. Only small results come back to the
driver. Changing the master URL to a cluster is the only change needed to scale out.

| Task | Result |
|---|---|
| Load + inspect | 4,521 rows; schema inferred (7 int, 10 string); `summary()` of numeric columns; no nulls (categories use `unknown`) |
| balance > 1000 | 1,481 clients (code uses a `THRESHOLD` variable) |
| quarter column | month name → number (`create_map`) → `Q1…Q4`; Q2 2,222 · Q3 1,391 · Q4 489 · Q1 419 |
| Avg balance + median age per job | e.g. retired 2,319 / 59 · student 1,544 / 26 · blue-collar 1,085 / 39 |
| Subscribers by marital status | married 277 · single 167 · divorced 77 |
| UDF age groups | `<30` 482 clients (15.4% subscribe) · `30-60` 3,912 (10.2%) · `>60` 127 (**37.8%**). A built-in `when()` version gives the same result and runs faster |
| Subscription rate by education | tertiary 14.3% · secondary 10.6% · unknown 10.2% · primary 9.4% |
| Top 3 default rates | entrepreneur 4.17% · unemployed 2.34% · self-employed 2.19% |
| String functions | `job_marital` via `concat_ws`; `contact` upper-cased (CELLULAR / TELEPHONE / UNKNOWN) |
| Visualisation | only aggregated results go to pandas for plotting; `docs/figures/clients_by_job.png` |
| Busiest month | May, 1,398 contacts, 6.65% success |
| Duration yes vs no | 552.7 s vs 226.3 s |
| corr(age, balance) | 0.0838 |
| Defaults | no 4,445 (98.3%) · yes 76 (1.7%), bar chart in `defaults.png` |
| Contact method | telephone 14.62% · cellular 14.36% · unknown 4.61% |
| Spark SQL | temp view `bank`; avg balance per age group (>60: 2,676 · 30-60: 1,427 · <30: 1,058); top 5 jobs |

**Key insights:** the target is imbalanced (11.5% yes). Call duration is the strongest signal. A
previous success predicts a new one. Peak months are over-called. Clients over 60 subscribe almost 4× as
often as middle-aged clients.

---

## 3. Predictive Modelling with Spark ML (`03_spark_ml.ipynb`)

**Problem:** predict `y` (binary). **Business use:** rank clients so the call centre phones the likeliest
subscribers first.

**Preprocessing**
* *Missing values:* none are null. `unknown` is kept as a real category (for `poutcome` it means "never
  contacted"). A fill step (median for numbers, `unknown` for categories) still protects the pipeline
  from future nulls.
* *Outliers:* IQR fences computed with `approxQuantile`. `balance` (506 outliers), `duration` (330) and
  `campaign` (318) are **capped, not deleted**, so rare but real clients stay in the data.
  `pdays`/`previous` are left as they are, because -1/0 are codes.
* *Encoding:* `StringIndexer` (handleInvalid = keep) → `OneHotEncoder` for 9 categorical columns.
* *New feature:* `was_contacted_before` = (pdays ≠ -1).
* *Imbalance:* balanced class weights (yes 4.34, no 0.57) via `weightCol`.

**Feature vector:** `VectorAssembler` combines 8 numeric columns and 9 one-hot vectors into a **61-dimensional**
vector, then `StandardScaler` scales it. Everything is one `Pipeline`.

**Training:** 80/20 random split (seed 42): 3,662 train / 859 test.

| Model (test set) | AUC-ROC | AUC-PR | Accuracy | Precision (yes) | Recall (yes) | F1 (yes) |
|---|---|---|---|---|---|---|
| **Logistic Regression (tuned)** | **0.897** | 0.469 | 0.839 | 0.404 | **0.828** | **0.543** |
| Random Forest (tuned) | 0.893 | 0.483 | 0.864 | 0.439 | 0.657 | 0.526 |
| Logistic Regression | 0.892 | 0.463 | 0.836 | 0.400 | 0.849 | 0.544 |
| GBT | 0.889 | 0.503 | 0.829 | 0.379 | 0.758 | 0.505 |
| Random Forest | 0.884 | 0.478 | 0.856 | 0.420 | 0.667 | 0.516 |
| Decision Tree | 0.813 | 0.498 | 0.807 | 0.344 | 0.747 | 0.471 |

**Why these metrics:** a model that always says "no" scores 88.5% accuracy and is useless. So the models
are ranked by AUC-ROC (ranking quality), and AUC-PR, recall and F1 are reported for the rare "yes" class.

**Hyperparameter tuning:** `ParamGridBuilder` + 3-fold `CrossValidator` with `parallelism=4`, so 4
parameter combinations train at the same time.
* LR grid: regParam {0, 0.01, 0.1} × elasticNet {0, 0.5, 1}. Best: regParam 0.01, elasticNet 0.5 (CV AUC 0.890).
* RF grid: numTrees {50, 150} × maxDepth {6, 10} × minInstancesPerNode {1, 5}. Best: 50 trees, depth 10, min 5 (CV AUC 0.888).

**Business view:** at threshold 0.5 the model flags 203 of 859 test clients and reaches 82 of the 99 real
subscribers. At 0.7 it flags 122 calls and finds 60. The bank can pick the threshold that matches its
call-centre capacity.

**Most influential features**
* Random Forest importance: duration (0.42) far ahead, then age, balance, day, contact = unknown,
  poutcome = success, pdays.
* LR coefficients, positive: duration, poutcome = success, month = oct / jun / mar, job = retired / student.
  Negative: contact = unknown, job = blue-collar, campaign (more calls → less likely), month = may, loan = yes.

**Leakage check:** `duration` is known only after the call ends. Without it, AUC falls from **0.897 to
0.690**. That honest pre-call model is the one to use for choosing whom to call.

The best pipeline is saved to `models/term_deposit_best`, and the outlier caps go to
`models/preprocessing.json`. The streaming app reuses both.

---

## 4. Real-Time Transaction Analysis (`04_streaming/`)

**Design.** The pipeline uses **Spark Structured Streaming**, the current Spark streaming API (the older
DStream API is deprecated in Spark 4).
* `producer.py` simulates live transactions. It shuffles `bank.csv`, cuts it into 29 files of 150 rows and
  writes one file every 2 s. Writes are atomic: write a temp file, then rename it.
* Each row gets a `txn_id` and an `event_time`. Each file covers 10 s of event time.
* To test robustness the producer simulates two real-world problems. About 5% of rows are **late and
  out of order** (event time 15–90 s in the past, shuffled inside the file). One file is a **traffic burst**
  with 3× the normal rows.
* `streaming_app.py` reads the folder as a stream (file source, explicit schema, 1 file per micro-batch,
  2 s triggers).

**4.1 Stream processing and aggregation.** `groupBy("job")` gives a running count, average balance and
average duration in complete output mode, printed after every micro-batch. The table updates live: after
batch 0 it shows 150 transactions, after batch 28 all 4,521. The final streaming averages
(management 1,766.93, retired 2,319.19, …) are **identical to the batch MapReduce and Hive results**,
which shows that the streaming and batch paths agree.

**4.2 Real-time predictions.** The app loads the tuned pipeline from Part 3 and applies the same outlier
caps. Every incoming transaction gets `predicted` (yes/no) and `p_subscribe`. Each batch prints its 8
highest-probability leads, plus running accuracy, precision and recall. After 4,521 transactions:
**accuracy 82.9%, recall (yes) 82.0%, precision (yes) 38.5%**, in line with the test-set results. The
stream replays `bank.csv`, so most of these rows were also training rows; the test set from Part 3 is
the unbiased estimate. If no saved model exists, the app trains one from the historical CSV first.

**4.3 Window operations.** A watermark of 30 s is applied, then:
* **10 s tumbling windows**: count, average balance, % subscribed
* **1 min sliding windows** (slide 20 s): count, average balance

Results print in update mode and are stored in `docs/streaming/part3_*.csv`.

*Trend analysis (run of 25 Nov 2026):*
* **Steady flow:** the 28 normal 10 s windows hold 142–151 transactions each (median 146), with an
  average balance of 1,415 ± 265 and a subscription rate of 7–16%.
* **Traffic spike:** one window carried **429 transactions (about 3× normal)**. The 1 min sliding windows
  that cover it rise from about 900 to about 1,170 transactions, then fall back once it slides out. A
  sliding window spreads a spike over 3 consecutive windows, while a tumbling window isolates it.
* **Balance anomalies:** two windows had an average balance more than 2σ above normal (2,000 and 2,272
  vs 1,415), caused by a few very rich clients in those 10 s. A live dashboard would flag them.
* **Out-of-order data:** small windows *before* the first real window (1–6 transactions, average
  balances from 0 to 4,799) are late events that arrived with old timestamps. They land in the
  **correct historical window** rather than the current one.
* **End of stream:** the last window has only 20 rows, because the producer stopped mid-window.

**4.4 Late and out-of-order data with watermarking.** The same 10 s window count runs twice:
* **With** `withWatermark("event_time", "30 seconds")` in append mode. Each window is written once, when final.
* **Without** a watermark, in complete mode. Spark must keep every window forever.

Results:
* The final watermark is the newest event time seen minus 30 s. Windows that end before it are closed.
* **62 rows arrived later than 30 s** after their window and were dropped by the watermark.
  Of the 224 late rows the producer created, the other 162 arrived within the 30 s allowance and **were counted in their correct
  window**, even though they arrived out of order.
* State size: **4 windows kept with the watermark vs 35 without**. Without a watermark, memory grows
  without limit in a stream that runs for days.
* The comparison table shows, per window, the count without the watermark, the final count with it, and
  the late rows dropped. The last 4 windows are still `open`: the watermark has not passed them yet, and
  they would be finalised as soon as newer data arrived.

**Why this improves accuracy:** event-time windows put a transaction where it *happened*, not where it
*arrived*, so a delayed transaction still lands in the right time bucket. The watermark is the explicit
trade-off: wait up to 30 s for stragglers (accuracy), then close the window to free memory and publish a
final number (latency). A fraud-alert stream would choose a short watermark for fast alerts. A
daily-reporting stream would choose a long one so that almost nothing is missed.

---

## 5. Efficient Data Handling through Data Parallelism (`05_data_parallelism.ipynb`)

**5.1 Partitioning.** The CSV is read as **1 partition**, so only 1 of the 4 cores works. Strategies compared:

| Strategy | Rows per partition |
|---|---|
| default | [4521] |
| round-robin `repartition(8)` | 565–566 each (perfectly balanced) |
| hash `repartition(8, "job")` | 1447, 230, 667, 1129, 880, 168 (2 partitions empty) |
| range `repartitionByRange(8, "balance")` | 445–723 |

**Chosen: hash partitioning by `job`, 8 partitions** (2 per core). All rows of a job sit in one
partition, so `groupBy("job")` needs **no extra shuffle**: the physical plan loses its `Exchange`
compared with round-robin data. The trade-off is skew: the key is uneven, so partitions are uneven.
Round-robin is better when the next operation is not keyed by job.

**5.2 Parallel analysis**
* Average balance per job, computed two ways with identical results. The DataFrame `groupBy` does a
  partial aggregate per partition, then a shuffle. The RDD version uses `map` → `reduceByKey`, which
  combines inside each partition before shuffling. `mapPartitionsWithIndex` shows exactly which jobs
  each partition processed.
* Top 5 age groups by loans. The dataset has **no loan-amount column**, so loan volume = number of
  loans (housing + personal) per 5-year group: **30-34: 807 loans** · 35-39: 614 · 40-44: 515 ·
  45-49: 395 · 25-29: 306. The total balance of loan holders is reported as a size proxy.

**5.3 Model training on partitioned data.** Logistic Regression was chosen because each iteration
computes gradients per partition and sums them with `treeAggregate` (naturally data-parallel), it was the
best model in Part 3, and it is interpretable. It trained on 4 balanced partitions with test AUC 0.882.
*Scaling experiment* on a 50× copy (226,050 rows):

| partitions | 1 | 2 | 4 | 8 | 32 |
|---|---|---|---|---|---|
| train time (s) | 1.15 | 0.63 | **0.54** | 0.75 | 1.18 |
| speed-up | 1.0 | 1.83 | **2.13** | 1.53 | 0.97 |

The best result comes from matching partitions to cores. Too many tiny partitions add scheduling and
aggregation overhead and end up slower than 1 partition.
Challenges: 1 input partition (fixed by `repartition`), repeated passes over the data (fixed with
`cache()`), the overhead of too many partitions (rule of thumb: 2–3 per core), and Python UDFs (avoided
in hot paths).

**5.4 Resource monitoring.** A background `psutil` thread samples CPU and memory of the Spark JVM every
0.5 s, and the Spark status REST API (the same data the Spark UI shows) reports executor memory, task
counts and GC time. Observations:
* Spark averages 2.4–3 cores on the small-data steps and reaches its **4-core limit** during the aggregation and
  scaling phases.
  Short peaks above 4 come from JVM garbage-collection and JIT threads.
* JVM memory grows in steps as data is cached, from about 620 MB to about 1.44 GB. The driver is capped
  at 2 GB to protect the 8 GB laptop.
* GC time is about 0.3 s against 16 s of task time, so memory is not the bottleneck.

**5.5 Task management and scheduling.** Four independent preprocessing tasks (null report, IQR caps,
category counts, job statistics) run on the same cached 226k-row DataFrame, first sequentially and then
concurrently from 4 Python threads. Each task gets its own **FAIR scheduler pool** and **job group**.
Result: **1.02 s sequential vs 0.27 s concurrent, 3.8× faster**. FAIR scheduling stops one long task from
blocking the others, job groups label every job in the Spark UI (and allow `cancelJobGroup`), and the
cached input is shared by all four tasks.

---

## 6. Innovation beyond the brief
* **Realistic HDFS splitting** (128 KB blocks), so even a small file produces parallel map tasks.
* **HiveServer2** instead of the embedded CLI, the way Hive is used in production.
* **Leakage analysis** of `duration`, giving an honest pre-call model (AUC 0.69 vs 0.90).
* **Threshold table** that turns model output into a call-centre decision.
* **Simulated late data and a traffic burst** in the stream, so windows and watermarking are actually tested.
* **Batch vs streaming consistency check:** the streaming per-job averages equal the batch results exactly.
* **Measured scaling curve** and **sequential vs concurrent timing** instead of only describing parallelism.

## 7. Limitations and next steps
* A single machine stands in for a cluster. The code runs unchanged on YARN or Kubernetes by changing the
  master URL, and a real cluster would use replication 3 and many DataNodes.
* There is no year column, so a true year-over-year trend needs the full dated dataset.
* The streaming source is a folder. In production it would be Kafka, writing to a dashboard or an alerting sink.
* Next steps: tune the decision threshold on a validation fold, try GBT tuning and resampling, add model
  monitoring for drift, and deploy on a cloud cluster (EMR, Dataproc or Databricks).
