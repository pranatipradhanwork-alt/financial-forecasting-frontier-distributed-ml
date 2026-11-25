# Financial Forecasting Frontier: Distributed ML

**Distributed machine learning on banking data.** Hadoop · Hive · Apache Spark · Spark ML · Spark Structured Streaming — on `bank.csv` (4,521 bank clients
from a term-deposit marketing campaign). Everything runs **locally in VS Code** on a MacBook Air M2 (8 GB).

```
bank.csv ──► HDFS (3 blocks) ──► MapReduce jobs on YARN ──► batch answers
                │
                └──► Hive table client_info (HiveServer2) ──► HiveQL analysis
                │
Spark (local[4], 4 parallel tasks)
   ├── 02 EDA notebook ───────────► patterns, anomalies, charts
   ├── 03 Spark ML notebook ──────► tuned model ──► models/term_deposit_best
   ├── 04 Structured Streaming ◄── producer (chunks of bank.csv) ──► live aggregates,
   │                                live predictions, windows, watermarking
   └── 05 Data parallelism notebook ► partitioning, scaling, monitoring, FAIR scheduling
```

## Project layout

| Path | What it is |
|---|---|
| `01_hadoop_hive/mapreduce/` | 5 Hadoop Streaming jobs in Python (mappers + reducers) |
| `01_hadoop_hive/run_hadoop.sh` | HDFS ingestion + runs all 5 MapReduce jobs on YARN |
| `01_hadoop_hive/hive/banking_analysis.hql` | every Hive question in HiveQL |
| `01_hadoop_hive/run_hive.sh` / `run_hive.py` | loads the table and runs the HiveQL through HiveServer2 |
| `02_eda_spark.ipynb` | Part 2 — EDA with Spark |
| `03_spark_ml.ipynb` | Part 3 — predictive modelling with Spark ML |
| `04_streaming/` | Part 4 — `producer.py`, `streaming_app.py`, `run_streaming.sh` |
| `05_data_parallelism.ipynb` | Part 5 — data parallelism |
| `colab/` | Colab versions of notebooks 02, 03, 05 |
| `docs/REPORT.md` | answers to every project question, with results |
| `docs/VIDEO_SCRIPT.md` | script for the 15-minute video + follow-up interview answers |
| `docs/outputs_*.txt` | saved console output of every run |
| `docs/figures/` | all charts · `docs/streaming/` — streaming results as CSV |
| `hadoop-conf/`, `hive-conf/` | single-node cluster configuration (kept inside the project) |
| `tools/` | scripts that generate the notebooks |

## One-time setup (already done on this Mac)

```bash
brew install hadoop hive          # also installs Java 17 (Hadoop) and Java 21 (Hive)
python3 -m venv .venv
.venv/bin/pip install pyspark==4.0.1 pandas matplotlib psutil ipykernel "pyhive[hive_pure_sasl]" nbformat nbclient
```

Open the folder in VS Code: **File → Open Folder… → `bank-distributed-ml`**. VS Code picks the `.venv`
interpreter automatically (`.vscode/settings.json`). For notebooks choose the kernel **.venv (Python 3.13)**.

## How to run

Every step is also a VS Code task: **Terminal → Run Task…** and pick it from the list.
Run one heavy part at a time — 8 GB of RAM is not enough for Hadoop + Hive + Spark together.

### Part 1 — Hadoop and Hive (terminal)

```bash
source env.sh
```

```bash
./start_cluster.sh
```

```bash
./01_hadoop_hive/run_hadoop.sh
```

```bash
./start_hive.sh
```

```bash
./01_hadoop_hive/run_hive.sh
```

```bash
./stop_hive.sh && ./stop_cluster.sh
```

Web UIs while the cluster runs: HDFS http://localhost:9870 · YARN http://localhost:8088 ·
HiveServer2 http://localhost:10002.
To type Hive queries yourself: `beeline -u jdbc:hive2://localhost:10000 -n $USER`, then `USE banking_data;`.

### Parts 2, 3, 5 — notebooks

Open `02_eda_spark.ipynb`, `03_spark_ml.ipynb`, `05_data_parallelism.ipynb` and click **Run All**.
Run `03` before the streaming part, because it saves the model the streaming app uses.
While a notebook runs, the Spark UI is at http://localhost:4040.

### Parts 2, 3, 5 in Google Colab (no local setup)

| Notebook | Open |
|---|---|
| EDA with Spark | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/pranatipradhanwork-alt/financial-forecasting-frontier-distributed-ml/blob/main/colab/02_eda_spark_colab.ipynb) |
| Spark ML | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/pranatipradhanwork-alt/financial-forecasting-frontier-distributed-ml/blob/main/colab/03_spark_ml_colab.ipynb) |
| Data parallelism | [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/pranatipradhanwork-alt/financial-forecasting-frontier-distributed-ml/blob/main/colab/05_data_parallelism_colab.ipynb) |

Click a badge, then **Runtime → Run all**. The first cell installs Java 17 + PySpark 4.0.1 and downloads
`bank.csv` from this repo (about 1–2 minutes). Colab usually has 2 CPU cores, so the timing numbers in the
parallelism notebook will differ from the Mac run; the analysis and ML results are the same.

### Part 4 — streaming (terminal)

```bash
./04_streaming/run_streaming.sh 1
```

Use `2`, `3` or `4` for the other parts. Each run takes about 80 s: Spark starts, the producer sends 29 files
(4,521 transactions) two seconds apart, and results print after every micro-batch.

## Headline results

| Part | Result |
|---|---|
| Hadoop | `bank.csv` stored as 3 HDFS blocks → every MapReduce job ran **3 map tasks in parallel** on YARN |
| Hive | 21 HiveQL statements; best month for volume (May, 1,398 calls) is the worst for conversion (6.65%) |
| EDA | subscribers' calls last 2.4× longer; previous-campaign success → 64% subscribe; corr(age, balance) = 0.08 |
| Spark ML | tuned Logistic Regression: **AUC-ROC 0.897**, recall 83% on "yes"; without `duration` AUC 0.69 |
| Streaming | live per-job averages match the batch results exactly; 82.9% live accuracy; watermark dropped 62 late rows and kept 4 state rows instead of 35 |
| Parallelism | 226k-row training is **2.1× faster on 4 partitions** than on 1; 32 partitions is no faster than 1; 4 concurrent preprocessing tasks finish **3.8× faster** with the FAIR scheduler |
