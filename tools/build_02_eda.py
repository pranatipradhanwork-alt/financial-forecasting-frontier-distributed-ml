from nb import build, SETUP

cells = [
("md", """# Part 2 — Exploratory Data Analysis with Apache Spark
**Dataset:** `data/bank.csv` — 4,521 bank clients contacted in a term-deposit marketing campaign.

Spark runs here in **local mode with 4 worker threads** (`local[4]`). The code is exactly what would run on a
real cluster: the DataFrame is split into **partitions**, each partition is processed by a separate task in
parallel, and only small aggregated results come back to the driver. Changing `.master(...)` to a YARN or
standalone cluster URL is the only change needed to scale out."""),
("code", SETUP.format(app="Bank-EDA")),

("md", "## 1. Data loading and basic inspection"),
("code", """df = (spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(DATA))
df.cache()                                  # reused many times below -> keep it in memory
print("rows:", df.count(), "| columns:", len(df.columns), "| partitions:", df.rdd.getNumPartitions())
df.show(5)"""),
("code", "df.printSchema()"),
("code", """num_cols = [c for c, t in df.dtypes if t == "int"]
df.select(num_cols).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()"""),
("code", """# Missing values check (Spark computes all column counts in ONE distributed pass)
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
# 'unknown' is used instead of null for categorical columns:
df.select([F.sum((F.col(c) == "unknown").cast("int")).alias(c)
           for c, t in df.dtypes if t == "string"]).show()"""),

("md", "## 2. Data filtering and column operations"),
("code", """THRESHOLD = 1000
rich = df.filter(F.col("balance") > THRESHOLD)
print(f"clients with balance > {THRESHOLD}: {rich.count()} of {df.count()}")
rich.select("age", "job", "marital", "balance", "y").orderBy(F.desc("balance")).show(5)"""),
("code", """# month is text ('jan'..'dec') -> month number -> quarter
months = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
month_map = F.create_map(*[x for i, m in enumerate(months, 1) for x in (F.lit(m), F.lit(i))])

df = (df.withColumn("month_num", month_map[F.col("month")])
        .withColumn("quarter", F.concat(F.lit("Q"), F.ceil(F.col("month_num") / 3).cast("int"))))
df.select("month", "month_num", "quarter").distinct().orderBy("month_num").show(12)
df.groupBy("quarter").count().orderBy("quarter").show()"""),

("md", "## 3. GroupBy and aggregation"),
("code", """(df.groupBy("job")
   .agg(F.count("*").alias("clients"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.median("age").alias("median_age"))
   .orderBy(F.desc("avg_balance"))
   .show())"""),
("code", """(df.filter(F.col("y") == "yes")
   .groupBy("marital").count().withColumnRenamed("count", "subscribed_clients")
   .orderBy(F.desc("subscribed_clients")).show())"""),
("code", """# How Spark actually executes a groupBy: partial aggregation inside each partition,
# then a shuffle (Exchange) that brings each job's partial sums together, then the final aggregation.
df.groupBy("job").agg(F.avg("balance")).explain()"""),

("md", "## 4. UDF to categorise age groups"),
("code", """@F.udf(returnType=T.StringType())
def age_group(age):
    if age is None:
        return None
    if age < 30:
        return "<30"
    if age <= 60:
        return "30-60"
    return ">60"

df = df.withColumn("age_group", age_group("age"))
df.groupBy("age_group").agg(F.count("*").alias("clients"),
                            F.round(F.avg("balance"), 2).alias("avg_balance"),
                            F.round(100 * F.avg((F.col("y") == "yes").cast("int")), 2).alias("subscribed_pct")
                           ).orderBy("age_group").show()"""),
("md", """> A Python UDF ships every row to a Python worker, which is slower than built-in functions.
> The same column using only Spark expressions (runs inside the JVM, can be optimised by Catalyst):"""),
("code", """df_builtin = df.withColumn("age_group_builtin",
    F.when(F.col("age") < 30, "<30").when(F.col("age") <= 60, "30-60").otherwise(">60"))
print("UDF and built-in versions agree:",
      df_builtin.filter(F.col("age_group") != F.col("age_group_builtin")).count() == 0)"""),

("md", "## 5. Advanced data transformations"),
("code", """(df.groupBy("education")
   .agg(F.count("*").alias("clients"),
        F.sum((F.col("y") == "yes").cast("int")).alias("subscribed"))
   .withColumn("subscription_rate_pct", F.round(100 * F.col("subscribed") / F.col("clients"), 2))
   .orderBy(F.desc("subscription_rate_pct")).show())"""),
("code", """(df.groupBy("job")
   .agg(F.count("*").alias("clients"),
        F.sum((F.col("default") == "yes").cast("int")).alias("defaulters"))
   .withColumn("default_rate_pct", F.round(100 * F.col("defaulters") / F.col("clients"), 2))
   .orderBy(F.desc("default_rate_pct"))
   .limit(3).show())"""),

("md", "## 6. String manipulation"),
("code", """df = (df.withColumn("job_marital", F.concat_ws("_", "job", "marital"))
        .withColumn("contact", F.upper("contact")))
df.select("job", "marital", "job_marital", "contact").show(5, truncate=False)
df.groupBy("job_marital").count().orderBy(F.desc("count")).show(5, truncate=False)"""),

("md", """## 7. Data visualisation
Only the **small aggregated result** is converted to pandas — the heavy work stays in Spark.
(Converting a full billion-row table with `toPandas()` would crash the driver.)"""),
("code", """import matplotlib.pyplot as plt
os.makedirs("docs/figures", exist_ok=True)

job_counts = df.groupBy("job").count().orderBy(F.desc("count")).toPandas()
ax = job_counts.plot.bar(x="job", y="count", legend=False, figsize=(10, 4), color="#3b6ea5")
ax.set_title("Clients by job type"); ax.set_xlabel(""); ax.set_ylabel("clients")
for p in ax.patches:
    ax.annotate(int(p.get_height()), (p.get_x() + p.get_width() / 2, p.get_height()),
                ha="center", va="bottom", fontsize=8)
plt.xticks(rotation=45, ha="right"); plt.tight_layout()
plt.savefig("docs/figures/clients_by_job.png", dpi=120); plt.show()"""),
("code", """# The task also asks for converting the Spark DataFrame itself to pandas (fine at 4.5k rows)
pdf = df.toPandas()
print(type(pdf), pdf.shape)
pdf.head()"""),

("md", "## 8. Complex queries for insights"),
("code", """monthly = (df.groupBy("month", "month_num")
             .agg(F.count("*").alias("contacts"),
                  F.round(100 * F.avg((F.col("y") == "yes").cast("int")), 2).alias("success_rate_pct"))
             .orderBy(F.desc("contacts")))
monthly.show(12)
top = monthly.first()
print(f"Busiest month: {top['month']} with {top['contacts']} contacts, success rate {top['success_rate_pct']}%")"""),
("code", """(df.groupBy("y")
   .agg(F.count("*").alias("clients"),
        F.round(F.avg("duration"), 1).alias("avg_duration_sec"),
        F.median("duration").alias("median_duration_sec"))
   .show())"""),
("code", """mp = monthly.orderBy("month_num").toPandas()
fig, ax1 = plt.subplots(figsize=(10, 4))
ax1.bar(mp["month"], mp["contacts"], color="#9db4d0", label="contacts")
ax1.set_ylabel("contacts")
ax2 = ax1.twinx()
ax2.plot(mp["month"], mp["success_rate_pct"], color="#c0392b", marker="o", label="success rate %")
ax2.set_ylabel("success rate %")
ax1.set_title("More calls ≠ more success: contacts vs success rate per month")
fig.tight_layout(); plt.savefig("docs/figures/month_contacts_vs_success.png", dpi=120); plt.show()"""),

("md", "## 9. Correlation between age and balance"),
("code", """r = df.stat.corr("age", "balance")
print(f"Pearson correlation(age, balance) = {r:.4f}")
print("=> very weak positive linear relationship" if abs(r) < 0.2 else "")"""),
("code", """sample = df.select("age", "balance").sample(fraction=0.5, seed=1).toPandas()
plt.figure(figsize=(7, 4))
plt.scatter(sample["age"], sample["balance"], s=6, alpha=0.4)
plt.title(f"Age vs balance (r = {r:.3f})"); plt.xlabel("age"); plt.ylabel("balance")
plt.tight_layout(); plt.savefig("docs/figures/age_vs_balance.png", dpi=120); plt.show()"""),

("md", "## 10. Exploring loan defaults"),
("code", """defaults = (df.groupBy("default").count()
              .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(
                  __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy()), 2)))
defaults.show()
dp = defaults.toPandas()
ax = dp.plot.bar(x="default", y="count", legend=False, color=["#3b6ea5", "#c0392b"], figsize=(5, 4))
ax.set_title("Clients with credit in default"); ax.set_xlabel("default"); ax.set_ylabel("clients")
for p in ax.patches:
    ax.annotate(int(p.get_height()), (p.get_x() + p.get_width() / 2, p.get_height()), ha="center", va="bottom")
plt.xticks(rotation=0); plt.tight_layout(); plt.savefig("docs/figures/defaults.png", dpi=120); plt.show()"""),

("md", "## 11. Contact method analysis"),
("code", """(df.groupBy("contact")
   .agg(F.count("*").alias("clients"),
        F.sum((F.col("y") == "yes").cast("int")).alias("subscribed"))
   .withColumn("success_rate_pct", F.round(100 * F.col("subscribed") / F.col("clients"), 2))
   .orderBy(F.desc("success_rate_pct")).show())"""),

("md", "## 12. Data processing with Spark SQL"),
("code", """df.createOrReplaceTempView("bank")
spark.sql('''
    SELECT age_group,
           COUNT(*)                AS clients,
           ROUND(AVG(balance), 2)  AS avg_balance
    FROM bank
    GROUP BY age_group
    ORDER BY avg_balance DESC
''').show()"""),
("code", """spark.sql('''
    SELECT job, COUNT(*) AS clients, ROUND(AVG(balance), 2) AS avg_balance
    FROM bank
    GROUP BY job
    ORDER BY clients DESC
    LIMIT 5
''').show()"""),

("md", """## Key findings
* **Imbalanced target:** only ~11.5% of clients subscribed — accuracy alone will be misleading in Part 3.
* **Call duration** is the strongest signal: subscribers talk ~2.4× longer (553 s vs 226 s on average).
* **Previous success matters:** clients whose previous campaign outcome was `success` subscribe ~64% of the time.
* **May is the busiest month but the worst converter** (≈1,400 calls, ≈6.7% success), while low-volume
  months (Mar, Sep, Oct, Dec) convert at 33–46% — a sign of over-calling in peak months.
* **Retired clients** have the highest average balance and one of the highest subscription rates.
* **Age vs balance** correlation is ≈0.08 — almost no linear relationship.
* **Contact method:** telephone (14.6%) and cellular (14.4%) convert about equally; clients with an `unknown`
  contact type convert at only 4.6% — the channel being recorded at all is what matters.
* Defaults are rare (~1.7%), so default prediction would need resampling/class weights."""),
("code", "spark.stop()"),
]

if __name__ == "__main__":
    build("02_eda_spark.ipynb", cells)
