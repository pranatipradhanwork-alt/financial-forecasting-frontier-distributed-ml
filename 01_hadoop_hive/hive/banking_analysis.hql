-- =====================================================================
-- Part 1b - HIVE : banking_data analysis on bank.csv
-- Run with:  01_hadoop_hive/run_hive.sh
-- =====================================================================

-- ---------- Q1. Data ingestion and table creation ----------
CREATE DATABASE IF NOT EXISTS banking_data
COMMENT 'Bank marketing clients (bank.csv)';
USE banking_data;

DROP TABLE IF EXISTS client_info;
CREATE TABLE client_info (
  age INT,
  job STRING,
  marital STRING,
  education STRING,
  `default` STRING,
  balance INT,
  housing STRING,
  loan STRING,
  contact STRING,
  day INT,
  month STRING,
  duration INT,
  campaign INT,
  pdays INT,
  previous INT,
  poutcome STRING,
  y STRING
)
COMMENT 'One row per client contacted in the term-deposit campaign'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- run_hive.sh copies bank.csv from HDFS input to HDFS staging without its header line, then we load it
LOAD DATA INPATH '/user/${env:USER}/banking/staging/bank.csv' OVERWRITE INTO TABLE client_info;
DESCRIBE client_info;

-- ---------- Q2. Basic exploration ----------
-- total clients
SELECT COUNT(*) AS total_clients FROM client_info;
-- first 10 rows
SELECT * FROM client_info LIMIT 10;

-- ---------- Q3. Filtering and sorting ----------
-- married clients who have a personal loan (how many, then every record)
SELECT COUNT(*) AS married_with_personal_loan FROM client_info
WHERE marital = 'married' AND loan = 'yes';
SELECT * FROM client_info
WHERE marital = 'married' AND loan = 'yes';

-- top 10 clients by balance
SELECT job, marital, balance FROM client_info
ORDER BY balance DESC LIMIT 10;

-- ---------- Q4. Aggregation and grouping ----------
-- average age per job
SELECT job, ROUND(AVG(age), 2) AS avg_age, COUNT(*) AS clients
FROM client_info GROUP BY job ORDER BY avg_age DESC;

-- clients with credit in default, per education level
SELECT education, COUNT(*) AS defaulted_clients
FROM client_info WHERE `default` = 'yes'
GROUP BY education ORDER BY defaulted_clients DESC;

-- ---------- Q5. Complex queries ----------
-- top 5 jobs by average balance + % of them who subscribed
SELECT job,
  ROUND(AVG(balance), 2) AS avg_balance,
  COUNT(*) AS clients,
  ROUND(100 * SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS subscribed_pct
FROM client_info
GROUP BY job
ORDER BY avg_balance DESC
LIMIT 5;

-- month with the most contacts + its success rate
SELECT month,
  COUNT(*) AS contacts,
  SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) AS subscribed,
  ROUND(100 * SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
FROM client_info
GROUP BY month
ORDER BY contacts DESC
LIMIT 1;

-- ---------- Q6. Correlation ----------
SELECT ROUND(CORR(age, balance), 4) AS corr_age_balance FROM client_info;

-- ---------- Q7. Trend analysis ----------
-- bank.csv has no year column (only day + month), so a true year-over-year
-- trend cannot be computed. We show the trend along the time axis we DO have:
-- contacts and success rate for each calendar month, in calendar order.
SELECT month_no, month, contacts, success_rate_pct,
  contacts - LAG(contacts) OVER (ORDER BY month_no) AS change_vs_prev_month
FROM (
  SELECT CASE month WHEN 'jan' THEN 1 WHEN 'feb' THEN 2 WHEN 'mar' THEN 3 WHEN 'apr' THEN 4
  WHEN 'may' THEN 5 WHEN 'jun' THEN 6 WHEN 'jul' THEN 7 WHEN 'aug' THEN 8
  WHEN 'sep' THEN 9 WHEN 'oct' THEN 10 WHEN 'nov' THEN 11 ELSE 12 END AS month_no,
  month,
  COUNT(*) AS contacts,
  ROUND(100 * SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct
  FROM client_info GROUP BY month
) m
ORDER BY month_no;

-- ---------- Q8. Anomaly detection ----------
-- balance = average yearly balance. Step 1: spread of balance per education level.
SELECT education, COUNT(*) AS clients,
  ROUND(AVG(balance), 2) AS avg_bal,
  PERCENTILE(balance, 0.5) AS median_bal,
  ROUND(STDDEV_POP(balance), 2) AS std_bal,
  MIN(balance) AS min_bal, MAX(balance) AS max_bal
FROM client_info GROUP BY education ORDER BY avg_bal DESC;

-- Step 2: outliers = clients more than 3 std devs above the overall mean
SELECT c.education,
  SUM(IF(c.balance > o.mu + 3 * o.sd, 1, 0)) AS high_outliers,
  SUM(IF(c.balance < 0, 1, 0)) AS negative_bal,
  ROUND(AVG(c.balance) - MAX(o.mu), 2) AS gap_vs_overall
FROM client_info c
CROSS JOIN (SELECT AVG(balance) mu, STDDEV_POP(balance) sd FROM client_info) o
GROUP BY c.education ORDER BY high_outliers DESC;

-- ---------- Q9. Advanced analysis ----------
-- subscription rate per previous-campaign outcome
SELECT poutcome,
  COUNT(*) AS clients,
  SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) AS subscribed,
  ROUND(100 * SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS subscription_rate_pct
FROM client_info
GROUP BY poutcome
ORDER BY subscription_rate_pct DESC;

-- average call duration: subscribed vs not
SELECT y AS subscribed,
  COUNT(*) AS clients,
  ROUND(AVG(duration), 2) AS avg_duration_sec,
  PERCENTILE(duration, 0.5) AS median_duration_sec
FROM client_info
GROUP BY y;
