-- 03_build_gold.sql

-- 1) gl_balance_daily: daily balance by account/cost_center/posting_date
CREATE OR REPLACE TABLE curated.gl_balance_daily
USING DELTA
PARTITIONED BY (year_month)
AS
SELECT
  account_id,
  account_code,
  cost_center,
  date(trunc(posting_date, 'DD')) as posting_date,
  YEAR(posting_date) as year,
  MONTH(posting_date) as month,
  date_format(posting_date,'yyyy-MM') as year_month,
  SUM(signed_amount) as balance_amount
FROM refined.gl_transaction_clean
GROUP BY account_id, account_code, cost_center, trunc(posting_date,'DD');

-- 2) gl_variance_monthly: month-over-month variance % for each account/cost center
CREATE OR REPLACE TABLE curated.gl_variance_monthly
USING DELTA
PARTITIONED BY (year)
AS
SELECT
  account_id,
  account_code,
  cost_center,
  year,
  month,
  month_total,
  month_total - lag(month_total) OVER (PARTITION BY account_id, cost_center ORDER BY year, month) as month_diff,
  CASE WHEN lag(month_total) OVER (PARTITION BY account_id, cost_center ORDER BY year, month) IS NULL THEN NULL
       WHEN lag(month_total) = 0 THEN NULL
       ELSE (month_total - lag(month_total) OVER (PARTITION BY account_id, cost_center ORDER BY year, month)) / abs(lag(month_total) OVER (PARTITION BY account_id, cost_center ORDER BY year, month)) END as pct_change
FROM (
  SELECT
    account_id,
    account_code,
    cost_center,
    YEAR(posting_date) as year,
    MONTH(posting_date) as month,
    SUM(signed_amount) as month_total
  FROM refined.gl_transaction_clean
  GROUP BY account_id, account_code, cost_center, YEAR(posting_date), MONTH(posting_date)
) t;

-- 3) gl_exceptions: consolidate quarantine + account mapping failures
CREATE OR REPLACE TABLE curated.gl_exceptions
USING DELTA
AS
SELECT * FROM meta.quarantine_gl_transactions
UNION ALL
SELECT *, current_timestamp() as quarantine_ts, 'MISSING_ACCOUNT' as error_code, 'account_code missing in coa_master' as error_message
FROM (
  SELECT * FROM refined.gl_transaction_clean WHERE account_id IS NULL
);
