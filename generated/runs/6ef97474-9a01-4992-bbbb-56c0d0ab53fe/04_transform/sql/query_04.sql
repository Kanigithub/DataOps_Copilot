INSERT INTO meta.gl_exceptions
SELECT
  transaction_id, _raw_row_id, posting_date, account_code, account_id, cost_center, amount_signed, description,
  CASE
    WHEN posting_date IS NULL THEN 'MISSING_POSTING_DATE'
    WHEN account_id IS NULL THEN 'INVALID_ACCOUNT'
    WHEN cost_center IS NULL THEN 'MISSING_COST_CENTER'
    ELSE 'OTHER'
  END as error_code,
  current_timestamp() as detected_at
FROM silver.gl_transaction_clean
WHERE posting_date IS NULL OR account_id IS NULL OR cost_center IS NULL;
