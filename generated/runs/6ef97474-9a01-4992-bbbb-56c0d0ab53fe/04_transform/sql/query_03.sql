-- monthly totals
CREATE OR REPLACE TEMP VIEW v_monthly AS
SELECT date_trunc('month', posting_date) as month, account_id, account_code, cost_center, SUM(amount_signed) as month_total
FROM silver.gl_transaction_clean
GROUP BY date_trunc('month', posting_date), account_id, account_code, cost_center;

-- variance month over month
CREATE OR REPLACE TEMP VIEW v_mom AS
SELECT cur.month as month,
       cur.account_id,
       cur.account_code,
       cur.cost_center,
       cur.month_total as amount_current,
       prev.month_total as amount_prev,
       (cur.month_total - coalesce(prev.month_total, 0)) as variance_amount,
       CASE WHEN coalesce(prev.month_total,0) = 0 THEN NULL ELSE (cur.month_total - prev.month_total) / prev.month_total END as variance_pct
FROM v_monthly cur
LEFT JOIN v_monthly prev
  ON cur.account_id = prev.account_id
  AND cur.cost_center = prev.cost_center
  AND add_months(prev.month, 1) = cur.month;

-- write to gold.gl_variance_monthly (MERGE upsert)
MERGE INTO gold.gl_variance_monthly tgt
USING v_mom src
ON tgt.month = src.month AND tgt.account_id = src.account_id AND tgt.cost_center = src.cost_center
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
