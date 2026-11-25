# Video Script — Distributed ML on Banking Data (~15 min)

Speak naturally. Lines marked **[SHOW]** tell you what to have on screen.

---

## 1. Introduction (1.5 min)

**[SHOW]** README.md, architecture diagram at the top.

"Hi, I'm Pranati. In this project I built a **distributed data system for banking data**. It stores,
queries, analyses and predicts on bank-customer data, and it also processes transactions in real time.

The domain is **banking**: the dataset has 4,521 customers from a term-deposit marketing campaign, with
age, job, balance, loans, call details, and whether each customer subscribed.

Why distributed machine learning? A real bank has millions of customers and a continuous flow of
transactions. One computer can't store all of that, can't analyse it fast enough, and can't react in real
time. A distributed system splits the data across many machines and processes the pieces in parallel.

My tech stack:
- **Hadoop**: HDFS for storage, MapReduce on YARN for batch processing
- **Hive**: SQL queries over the stored data
- **Apache Spark**: fast in-memory analysis
- **Spark ML**: scalable model training
- **Spark Structured Streaming**: real-time processing

Everything runs on my laptop as a single-node cluster, but it's the same code you'd run on a real cluster."

## 2. Problem Understanding (2 min)

"Why isn't traditional processing enough? Think of a Python script with pandas on one machine. It loads
the whole dataset into memory, so it breaks as soon as the data is bigger than RAM, and it uses only one
CPU core at a time.

Banking data is hard in three ways, the 3 Vs:
- **Volume**: years of history for millions of customers is terabytes of data.
- **Variety**: numbers like balance and duration, categories like job and education, plus yes/no flags,
  all from different systems.
- **Velocity**: transactions never stop arriving, and some decisions can't wait for a nightly batch job.

So banks need distributed systems for two things. First, **insight and prediction at scale**: who will
buy a product, who might default. Second, **timeliness**. Fraud has to be caught while the transaction is
happening, not the next morning. That's why real-time analysis matters: the value of an insight drops
the longer you wait for it."

## 3. Data Storage & Processing Architecture (2 min)

**[SHOW]** HDFS UI (localhost:9870) with bank.csv in 3 blocks → YARN UI (localhost:8088) → the diagram.

"Here's the pipeline: **ingestion → storage → analysis → prediction → streaming**.

1. **Ingestion and storage — HDFS.** I copy bank.csv into HDFS. HDFS cuts files into blocks and spreads
   them over machines. I set a small block size, so even this small file splits into **3 blocks**, like
   real big data.
2. **Batch processing — MapReduce on YARN.** I wrote 5 MapReduce jobs in Python, for example average
   balance per job. YARN schedules them, and because the file has 3 blocks, each job runs **3 map tasks
   in parallel**.
3. **Querying — Hive.** Hive puts a table on top of the HDFS files, so analysts can use SQL instead of
   writing MapReduce code. I run it as HiveServer2, the way companies run it in production.
4. **Analysis and ML — Spark.** Spark keeps data in memory, which makes it much faster than MapReduce
   for repeated work like EDA and model training.
5. **Streaming — Spark Structured Streaming** for transactions as they arrive.

The separation is important: **HDFS stores** data, **Hive answers SQL**, **Spark runs batch analysis and
ML**, and **Structured Streaming handles live data**. Each layer can scale on its own."

## 4. Exploratory Analysis with Spark (2.5 min)

**[SHOW]** 02_eda_spark.ipynb: the `explain()` output, then the charts.

"I did the EDA with Spark DataFrames. This isn't a pandas notebook: Spark splits the DataFrame into
**partitions**, and each partition is processed by a separate task. Here I use 4 parallel workers.

This `explain()` plan shows how a groupBy really runs. Every partition first computes a partial result,
then a **shuffle** brings matching keys together, then Spark combines them. Only the small final table
comes back to my notebook. For plotting I convert just that aggregated result to pandas, never the full
dataset.

What I found:
- **The target is imbalanced**: only 11.5% of customers subscribed.
- **Call duration** is the strongest signal: subscribers' calls last 553 seconds on average, against 226
  seconds for everyone else.
- **Past success repeats**: if the previous campaign succeeded, 64% subscribe again.
- **An anomaly in calling strategy**: May has the most calls, about 1,400, but the lowest success rate,
  6.65%. March, October and December have very few calls but succeed 40 to 46% of the time. The bank is
  over-calling in its peak months.
- **Customers over 60** subscribe at 38%, almost 4 times the rate of middle-aged customers.
- **Age and balance** are barely related: the correlation is 0.08.

The fields I relied on most were y, duration, poutcome, month, age, balance and contact. The advantage
of Spark is that this exact code would run on a billion rows, just with more partitions and machines."

## 5. Predictive Modeling with Spark ML (2 min)

**[SHOW]** 03_spark_ml.ipynb: the comparison table, the threshold table, feature importance.

"The prediction problem: **will this customer subscribe to a term deposit?** That's binary classification.

I built a Spark ML **Pipeline**: fill missing values, cap outliers using the IQR rule, turn categories
into numbers with StringIndexer and OneHotEncoder, combine everything with VectorAssembler into a
61-feature vector, then scale it. Because only 11.5% are 'yes', I used **class weights** so the model
doesn't just predict 'no' for everyone.

I compared 4 models: Logistic Regression, Decision Tree, Random Forest and Gradient-Boosted Trees. Then I
tuned the best ones with **CrossValidator**: 3-fold cross-validation, training 4 parameter combinations in
parallel.

On evaluation: accuracy is misleading here. Always saying 'no' already gives 88.5%. So I used **AUC-ROC**
and recall and F1 for the 'yes' class. The winner was tuned Logistic Regression, with **AUC 0.897**,
finding **83% of the real subscribers**.

The business impact: at a 0.5 threshold, the bank would call 203 of 859 customers and reach 82 of the 99
who would actually subscribe. That's far fewer wasted calls.

One honest point: call duration is only known *after* the call. Without it, AUC drops to 0.69. So for
deciding *whom to call*, the bank should use that version.

Spark ML helps because every pipeline step is a distributed operation. Logistic Regression computes its
gradients on each partition in parallel and sums them up."

## 6. Real-Time Streaming & Data Parallelism (2 min)

**[SHOW]** A terminal running `./04_streaming/run_streaming.sh 2`, then the part 4 table, then the scaling chart.

"For streaming I used **Spark Structured Streaming**. A producer script cuts bank.csv into small files
and sends one every 2 seconds, like live transactions. I also made it realistic on purpose: about 5% of
the events arrive late and out of order, and there's one traffic burst with 3 times the normal volume.

What the streaming app does:
1. **Live aggregates**: average balance and call duration per job, updated every micro-batch. At the end
   they matched my batch Hadoop results exactly.
2. **Live predictions**: the model from Spark ML scores every incoming customer, with about 83% running accuracy.
3. **Windows**: counts and average balance per 10-second and per 1-minute window. The burst showed up as
   a window of 429 transactions against a normal 146, and two windows had unusually high average
   balances. Those are exactly the kinds of spikes an alert system should catch.
4. **Watermarking**: with a 30-second watermark, late events still land in their correct time window, but
   events more than 30 seconds late are dropped. Spark kept only 4 windows in memory instead of 35.

In a bank this is how you'd do **fraud detection, alerts, and live monitoring dashboards**.

For **data parallelism**, I compared partitioning strategies and chose hash partitioning by job, because
it removes a shuffle from the groupBy. Then I measured the effect: on 226,000 rows, training on 4
partitions was **2.1 times faster** than on 1. But 32 partitions was no faster than 1, because of overhead.
Running 4 preprocessing tasks at the same time with the FAIR scheduler was **3.8 times faster** than
running them one after another."

## 7. Challenges, Optimization & Trade-offs (1.5 min)

"Challenges:
- **Cluster setup on a Mac**: Hadoop normally needs SSH, so I started each daemon directly. Hive 4 needs a
  different Java version than Hadoop. And the Hive command-line client couldn't run inside scripts on
  Apple Silicon, so I used HiveServer2 with a Python client.
- **Only 8 GB of RAM**: I ran one heavy system at a time and capped Spark's memory at 2 GB.
- **Streaming logic**: handling late data correctly. The watermark solved it.
- **Performance tuning**: the file was read as a single partition, so only one core worked until I repartitioned.

Optimizations: **partitioning** (8 partitions for 4 cores, hashed by key), **caching** data that's reused,
**built-in functions instead of Python UDFs**, and fewer shuffle partitions for small data.

Trade-offs: more partitions means more parallelism but also more overhead. A longer watermark is more
accurate but uses more memory and slows final results. A full cluster gives more power, but more setup
and cost than a single machine."

## 8. Learnings & Practical Value (1.5 min)

"What I learned: distributed ML isn't just 'more computers'. You need to think about **where the data
lives, how it's partitioned, and when it moves**, because shuffles and overhead can cancel out the gain
from parallelism. I also learned to choose metrics for the business problem, not just accuracy.

Distributed tools matter because data in modern banks only grows, and decisions have to be faster.

To take this to production:
- run it on a **cloud cluster** like AWS EMR, Google Dataproc or Databricks
- replace the folder source with **Kafka** for real transaction streams
- **tune the model more**, with threshold tuning and resampling, and monitor it for drift
- send streaming alerts to a **dashboard** or an alerting system

Thank you!"

---

## Follow-up interview questions

**1. Why is Spark preferred for large-scale data analysis?**
Spark processes data **in memory** and spreads it across a cluster as partitions, so many tasks run in
parallel. MapReduce writes to disk after every step, so Spark is much faster, especially for repeated work
like ML training or interactive analysis. It also has one engine for SQL, ML, streaming and graphs, and
an optimizer (Catalyst) that plans queries efficiently. In my project, the same Spark code did EDA, ML and
streaming, and each MapReduce job's 3 map tasks became just a few lines of DataFrame code.

**2. How does Spark ML differ from traditional local ML workflows?**
In scikit-learn, all the data must fit in one machine's memory and training mostly runs in one process.
In Spark ML the data stays distributed: every step (encoding, assembling, training) runs on all
partitions in parallel, and the model combines partial results, for example by summing per-partition
gradients. It also uses **Pipelines**, so the exact same preprocessing runs in training and in
streaming. In my project, the pipeline trained in the notebook was loaded unchanged by the streaming app.

**3. What is the role of Spark Streaming in banking systems?**
It processes events **continuously as they arrive**, in small micro-batches, instead of waiting for a
daily batch. Banks use it for fraud detection, real-time alerts like large withdrawals or unusual
locations, live dashboards, and scoring customers instantly with an ML model. Event-time windows and
watermarks keep results correct even when events arrive late. In my project, every incoming transaction
was scored live, and late events were still counted in the right time window.

**4. How does data parallelism improve performance?**
The data is split into partitions, the **same operation runs on every partition at the same time** on
different cores or machines, and the small partial results are combined. That uses all the hardware
instead of one core. It only helps if partitions are balanced and not too small, and if shuffles are
kept low. In my project, 4 partitions trained 2.1× faster than 1, while 32 partitions was no faster than 1
because of overhead.

**5. How would you scale this project further in production?**
- Run Spark on a managed cloud cluster (EMR, Dataproc, Databricks) with autoscaling.
- Store data in HDFS or S3 as **Parquet**, partitioned by date.
- Use a proper Hive metastore (MySQL or Postgres) instead of Derby.
- Replace the file stream with **Kafka**, and write results to a database or dashboard.
- Schedule the batch jobs with Airflow.
- Track models with MLflow, retrain on new data, and monitor for drift.
- Set partitions and executors to match the data size, and add security: Kerberos, encryption,
  access control.
