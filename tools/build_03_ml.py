from nb import build, SETUP

cells = [
("md", """# Part 3 — Predictive Modeling with Spark ML
**Goal:** predict whether a client will subscribe to a term deposit (`y` = yes/no) — a binary classification problem.

**Business value:** the bank calls thousands of clients per campaign. A model that ranks clients by
probability of subscribing lets the call centre phone the most promising clients first — fewer wasted calls,
higher conversion, and less annoyance for clients who will never subscribe.

Everything below uses **Spark ML Pipelines**: every step (indexing, encoding, assembling, scaling, training)
is a distributed transformation over partitions, so the same code trains on 4 thousand or 400 million rows."""),
("code", SETUP.format(app="Bank-SparkML")),

("md", "## 1. Data loading and initial exploration"),
("code", """df = spark.read.csv(DATA, header=True, inferSchema=True).cache()
print("rows:", df.count(), "| partitions:", df.rdd.getNumPartitions())
df.printSchema()
df.show(5)"""),
("code", """df.groupBy("y").count().withColumn("pct", F.round(100 * F.col("count") / df.count(), 2)).show()
df.select("age", "balance", "duration", "campaign", "pdays", "previous").describe().show()"""),

("md", """## 2. Data preprocessing
### 2a. Missing values
There are no nulls, but several categorical columns use the word `unknown`. We keep `unknown` as its own
category — for `poutcome` it means "never contacted before", which is real information, not noise.
The fill step is still written so the pipeline is safe if future data arrives with nulls."""),
("code", """null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

num_cols = ["age", "balance", "day", "duration", "campaign", "pdays", "previous"]
cat_cols = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "poutcome"]

# safety net: numeric nulls -> median, categorical nulls -> 'unknown'
medians = dict(zip(num_cols, df.approxQuantile(num_cols, [0.5], 0.01)))
df = df.fillna({c: float(m[0]) for c, m in medians.items()}).fillna("unknown", subset=cat_cols)
print("rows after handling missing values:", df.count())"""),

("md", """### 2b. Outliers
`balance`, `duration` and `campaign` are heavily right-skewed (e.g. balance up to 71,188 while the median is 444).
We **cap** (winsorise) them at the IQR fences instead of deleting rows — deleting would throw away rare but
real wealthy clients, and the target is already imbalanced. `pdays`/`previous` are left alone because
`-1`/`0` are meaningful codes ("never contacted"), not errors."""),
("code", """cap_cols = ["balance", "duration", "campaign"]
q = df.approxQuantile(cap_cols, [0.25, 0.75], 0.0)       # exact quartiles, computed in parallel
fences = {}
for c, (q1, q3) in zip(cap_cols, q):
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    n_out = df.filter((F.col(c) < lo) | (F.col(c) > hi)).count()
    fences[c] = (lo, hi)
    print(f"{c:9s} Q1={q1:8.1f} Q3={q3:8.1f}  fences=[{lo:9.1f}, {hi:9.1f}]  outliers={n_out}")

for c, (lo, hi) in fences.items():
    df = df.withColumn(c, F.when(F.col(c) < lo, lo).when(F.col(c) > hi, hi).otherwise(F.col(c)).cast("double"))
df.select(cap_cols).describe().show()"""),

("md", """### 2c. Feature engineering + categorical encoding
* `was_contacted_before` = 1 if `pdays != -1` (turns the magic number -1 into a clear flag)
* `label` = 1.0 if `y == 'yes'`
* `class_weight` — only ~11.5% are positives, so positives get a larger weight (balanced weighting) to stop
  the model from simply predicting "no" for everyone.
* Categorical columns → `StringIndexer` (text → index) → `OneHotEncoder` (index → sparse 0/1 vector)."""),
("code", """df = (df.withColumn("was_contacted_before", (F.col("pdays") != -1).cast("double"))
        .withColumn("label", (F.col("y") == "yes").cast("double")))

n, pos = df.count(), df.filter("label = 1").count()
w_pos, w_neg = n / (2.0 * pos), n / (2.0 * (n - pos))
df = df.withColumn("class_weight", F.when(F.col("label") == 1, w_pos).otherwise(w_neg))
print(f"class weights -> yes: {w_pos:.3f}   no: {w_neg:.3f}")

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat_cols]
encoder = OneHotEncoder(inputCols=[c + "_idx" for c in cat_cols],
                        outputCols=[c + "_ohe" for c in cat_cols], handleInvalid="keep")"""),

("md", "## 3. Feature vector with VectorAssembler"),
("code", """feature_inputs = num_cols + ["was_contacted_before"] + [c + "_ohe" for c in cat_cols]
assembler = VectorAssembler(inputCols=feature_inputs, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=False, withStd=True)
prep = [*indexers, encoder, assembler, scaler]

preview = Pipeline(stages=prep).fit(df).transform(df)
preview.select("age", "job", "job_idx", "job_ohe", "features", "label").show(3, truncate=60)
print("feature vector length:", preview.first()["features"].size)"""),

("md", """## 4. Model training and selection
Train/test split **80/20** (fixed seed so results are reproducible). The training set is cached in memory
because every model and every cross-validation fold re-reads it.

Candidate models:
| Model | Why consider it |
|---|---|
| Logistic Regression | fast, interpretable coefficients, strong baseline, distributed gradient computation |
| Decision Tree | captures non-linear rules (e.g. *duration > X and poutcome = success*) |
| Random Forest | many trees trained in parallel → robust, less overfitting than one tree |
| Gradient-Boosted Trees | usually the most accurate on tabular data, but trees are built one after another |"""),
("code", """train, test = df.randomSplit([0.8, 0.2], seed=42)
train = train.cache(); test = test.cache()
print("train:", train.count(), " test:", test.count(),
      " | positives train/test:", train.filter("label=1").count(), "/", test.filter("label=1").count())

from pyspark.ml.classification import (LogisticRegression, DecisionTreeClassifier,
                                       RandomForestClassifier, GBTClassifier)
from pyspark.ml.evaluation import BinaryClassificationEvaluator

auc_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability", metricName="areaUnderROC")
auc_pr  = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability", metricName="areaUnderPR")

def scores(pred, threshold=0.5):
    \"\"\"All metrics from one distributed pass: AUCs + confusion matrix for the positive class.\"\"\"
    p1 = F.udf(lambda v: float(v[1]), T.DoubleType())
    pred = pred.withColumn("p_yes", p1("probability")).withColumn("pred_t", (F.col("p_yes") >= threshold).cast("double"))
    cm = pred.groupBy().agg(
        F.sum(((F.col("label") == 1) & (F.col("pred_t") == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("label") == 0) & (F.col("pred_t") == 1)).cast("int")).alias("fp"),
        F.sum(((F.col("label") == 1) & (F.col("pred_t") == 0)).cast("int")).alias("fn"),
        F.sum(((F.col("label") == 0) & (F.col("pred_t") == 0)).cast("int")).alias("tn")).first()
    tp, fp, fn, tn = cm["tp"], cm["fp"], cm["fn"], cm["tn"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"AUC_ROC": auc_roc.evaluate(pred), "AUC_PR": auc_pr.evaluate(pred),
            "accuracy": (tp + tn) / (tp + tn + fp + fn), "precision_yes": precision,
            "recall_yes": recall, "F1_yes": f1, "TP": tp, "FP": fp, "FN": fn, "TN": tn}

models = {
    "LogisticRegression": LogisticRegression(labelCol="label", weightCol="class_weight", maxIter=100),
    "DecisionTree":       DecisionTreeClassifier(labelCol="label", weightCol="class_weight", maxDepth=6, seed=42),
    "RandomForest":       RandomForestClassifier(labelCol="label", weightCol="class_weight", numTrees=100,
                                                 maxDepth=8, seed=42),
    "GBT":                GBTClassifier(labelCol="label", weightCol="class_weight", maxIter=50, maxDepth=4, seed=42),
}"""),
("code", """import pandas as pd
results, fitted = [], {}
for name, clf in models.items():
    t0 = time.time()
    fitted[name] = Pipeline(stages=prep + [clf]).fit(train)
    secs = time.time() - t0
    r = scores(fitted[name].transform(test)); r["model"] = name; r["train_sec"] = round(secs, 1)
    results.append(r)
baseline = pd.DataFrame(results).set_index("model").round(4)
baseline[["AUC_ROC", "AUC_PR", "accuracy", "precision_yes", "recall_yes", "F1_yes", "train_sec"]]"""),

("md", """## 5. Model evaluation
**Why these metrics:** with 88.5% "no", a model that always says "no" gets 88.5% accuracy and is useless.
So we rank models by **AUC-ROC** (how well it ranks yes above no) and **AUC-PR / F1 for the "yes" class**
(how well it finds the rare subscribers). Class weighting deliberately trades some precision for much higher
recall — for a marketing call list, missing a likely subscriber costs more than one extra call."""),
("code", """for name in models:
    r = baseline.loc[name]
    print(f"{name:18s} confusion matrix  TP={int(r.TP):3d}  FP={int(r.FP):3d}  FN={int(r.FN):3d}  TN={int(r.TN):4d}")"""),

("md", """## 6. Hyperparameter tuning — ParamGridBuilder + CrossValidator
3-fold cross-validation on the training set only (the test set stays untouched until the end).
`parallelism=4` lets Spark train 4 parameter combinations **at the same time** — this is model-level
parallelism on top of the data parallelism inside each fit."""),
("code", """from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

lr = LogisticRegression(labelCol="label", weightCol="class_weight", maxIter=100)
lr_grid = (ParamGridBuilder()
           .addGrid(lr.regParam, [0.0, 0.01, 0.1])
           .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
           .build())

rf = RandomForestClassifier(labelCol="label", weightCol="class_weight", seed=42)
rf_grid = (ParamGridBuilder()
           .addGrid(rf.numTrees, [50, 150])
           .addGrid(rf.maxDepth, [6, 10])
           .addGrid(rf.minInstancesPerNode, [1, 5])
           .build())

tuned = {}
for name, est, grid in [("LogisticRegression", lr, lr_grid), ("RandomForest", rf, rf_grid)]:
    cv = CrossValidator(estimator=Pipeline(stages=prep + [est]), estimatorParamMaps=grid,
                        evaluator=auc_roc, numFolds=3, parallelism=4, seed=42)
    t0 = time.time()
    cvm = cv.fit(train)
    best_i = max(range(len(grid)), key=lambda i: cvm.avgMetrics[i])
    best_params = {p.name: v for p, v in grid[best_i].items()}
    tuned[name] = cvm.bestModel
    print(f"{name}: {len(grid)} combos x 3 folds in {time.time() - t0:.0f}s | "
          f"best CV AUC={cvm.avgMetrics[best_i]:.4f} | best params {best_params}")"""),
("code", """rows = []
for name, m in tuned.items():
    r = scores(m.transform(test)); r["model"] = name + " (tuned)"; rows.append(r)
comparison = pd.concat([baseline, pd.DataFrame(rows).set_index("model").round(4)])
comparison[["AUC_ROC", "AUC_PR", "accuracy", "precision_yes", "recall_yes", "F1_yes"]].sort_values("AUC_ROC", ascending=False)"""),
("code", """import matplotlib.pyplot as plt, numpy as np
os.makedirs("docs/figures", exist_ok=True)

def roc_points(model):
    pdf = (model.transform(test)
           .select("label", F.udf(lambda v: float(v[1]), T.DoubleType())("probability").alias("p"))
           .toPandas().sort_values("p", ascending=False))
    tpr = np.cumsum(pdf.label) / pdf.label.sum()
    fpr = np.cumsum(1 - pdf.label) / (1 - pdf.label).sum()
    return np.r_[0, fpr], np.r_[0, tpr]

plt.figure(figsize=(6, 5))
for name, m in [("LogReg tuned", tuned["LogisticRegression"]), ("RF tuned", tuned["RandomForest"]),
                ("GBT", fitted["GBT"]), ("DecisionTree", fitted["DecisionTree"])]:
    fpr, tpr = roc_points(m)
    plt.plot(fpr, tpr, label=f"{name} (AUC={np.trapezoid(tpr, fpr):.3f})")
plt.plot([0, 1], [0, 1], "k--", lw=0.8)
plt.xlabel("False positive rate"); plt.ylabel("True positive rate"); plt.title("ROC curves on the test set")
plt.legend(); plt.tight_layout(); plt.savefig("docs/figures/roc_curves.png", dpi=120); plt.show()"""),

("md", "### Business view: choosing the decision threshold"),
("code", """best_name = comparison["AUC_ROC"].idxmax()
best_model = tuned.get(best_name.replace(" (tuned)", ""), fitted.get(best_name))
print("Best model by test AUC-ROC:", best_name)
pred = best_model.transform(test).cache()
rows = []
for t in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]:
    s = scores(pred, t)
    rows.append({"threshold": t, "calls_made (pred yes)": s["TP"] + s["FP"], "subscribers_found": s["TP"],
                 "subscribers_missed": s["FN"], "precision_yes": round(s["precision_yes"], 3),
                 "recall_yes": round(s["recall_yes"], 3)})
pd.DataFrame(rows)"""),
("md", """A higher threshold means fewer calls but more missed subscribers. The bank can pick the row that fits its
call-centre capacity — the model gives a ranked list, not just a yes/no."""),

("md", "## 7. Advanced analysis — which features drive the prediction?"),
("code", """def feature_names(model_df):
    attrs = model_df.schema["raw_features"].metadata["ml_attr"]["attrs"]   # scaler output drops names; same order
    names = sorted([a for group in attrs.values() for a in group], key=lambda a: a["idx"])
    return [a["name"].replace("_ohe_", "=") for a in names]

rf_model = tuned["RandomForest"]
names = feature_names(rf_model.transform(test.limit(1)))
imp = pd.Series(rf_model.stages[-1].featureImportances.toArray(), index=names).sort_values(ascending=False)
print("Random Forest - top 15 feature importances")
display(imp.head(15).round(4).to_frame("importance"))

ax = imp.head(15)[::-1].plot.barh(figsize=(7, 5), color="#3b6ea5")
ax.set_title("Top 15 features (Random Forest importance)"); plt.tight_layout()
plt.savefig("docs/figures/feature_importance_rf.png", dpi=120); plt.show()"""),
("code", """lr_model = tuned["LogisticRegression"]
coef = pd.Series(lr_model.stages[-1].coefficients.toArray(), index=names)
print("Logistic Regression (standardised features): strongest positive and negative coefficients")
display(pd.concat([coef.sort_values(ascending=False).head(8), coef.sort_values().head(8)]).round(3).to_frame("coefficient"))"""),

("md", """### Leakage check: what if we do not know the call duration?
`duration` is only known **after** the call ends — so it cannot be used to decide *whom to call*.
We retrain the best pipeline without it to get an honest estimate for pre-call targeting."""),
("code", """prep_nodur = [*indexers, encoder,
              VectorAssembler(inputCols=[c for c in feature_inputs if c != "duration"], outputCol="raw_features"),
              scaler]
best_clf = best_model.stages[-1]                       # fitted model -> rebuild the same estimator
est_cls = {"LogisticRegressionModel": LogisticRegression, "RandomForestClassificationModel": RandomForestClassifier,
           "GBTClassificationModel": GBTClassifier, "DecisionTreeClassificationModel": DecisionTreeClassifier
          }[type(best_clf).__name__]
clf_copy = est_cls()
for p, v in best_clf.extractParamMap().items():
    if clf_copy.hasParam(p.name):
        clf_copy.set(clf_copy.getParam(p.name), v)
print("re-training", est_cls.__name__, "without duration")
nodur = Pipeline(stages=prep_nodur + [clf_copy]).fit(train)
s_with, s_without = scores(pred), scores(nodur.transform(test))
pd.DataFrame({"with duration": s_with, "without duration (pre-call)": s_without}).loc[
    ["AUC_ROC", "AUC_PR", "precision_yes", "recall_yes", "F1_yes"]].round(4)"""),

("md", "## 8. Save the best model (re-used by the streaming app in Part 4)"),
("code", """import json
best_model.write().overwrite().save("models/term_deposit_best")
# the IQR caps are applied before the pipeline, so save them for the streaming app too
json.dump({"cap_fences": fences, "model": best_name}, open("models/preprocessing.json", "w"), indent=2)
print("saved:", best_name, "-> models/term_deposit_best  (+ models/preprocessing.json)")"""),

("md", """## Summary
* **Problem:** binary classification of term-deposit subscription with an 11.5% positive rate.
* **Pipeline:** missing-value safety net → IQR capping → StringIndexer + OneHotEncoder → VectorAssembler →
  StandardScaler → classifier, with **balanced class weights**.
* **Models compared:** Logistic Regression, Decision Tree, Random Forest, GBT; LR and RF tuned with
  `ParamGridBuilder` + 3-fold `CrossValidator` (4 combinations trained in parallel).
* **Metrics:** AUC-ROC (ranking quality), AUC-PR and F1 for the "yes" class (because of imbalance) — not accuracy.
* **Result:** tuned Logistic Regression is best — **AUC-ROC 0.897**, finds **83% of subscribers** (recall) with
  F1 0.54 for the "yes" class. Tuned Random Forest is close (AUC 0.893) and more precise (0.44 vs 0.40).
  At threshold 0.5 the bank would call 203 of 859 test clients and reach 82 of the 99 real subscribers.
* **Drivers:** call `duration`, previous campaign `success`, contact type (`unknown` is strongly negative),
  month (Mar/Jun/Oct positive, May negative), number of calls this campaign (negative) and personal loan.
* **Honest deployment note:** without `duration` AUC drops from 0.897 to 0.690 — weaker, but that is the
  version that can actually be used *before* calling a client."""),
("code", "spark.stop()"),
]

if __name__ == "__main__":
    build("03_spark_ml.ipynb", cells, timeout=3600)
