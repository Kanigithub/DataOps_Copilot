-- 99_tests.sql
-- 1) Posting date NOT NULL
SELECT COUNT(*) as cnt_missing_posting_date
FROM refined.gl_transaction_clean
WHERE posting_date IS NULL;

-- expected: 0

-- 2) transaction_id uniqueness
SELECT COUNT(*) as total_rows, COUNT(DISTINCT transaction_id) as distinct_tx
FROM refined.gl_transaction_clean;

-- expected: total_rows == distinct_tx

-- 3) Amount precision/truncation check: detect values outside decimal(38,14)
SELECT transaction_id, signed_amount
FROM refined.gl_transaction_clean
WHERE signed_amount IS NULL OR (signed_amount != CAST(signed_amount AS DECIMAL(38,14)));

-- expected: zero rows where casting changes value (monitor)

-- 4) account_code exists in coa_master
SELECT COUNT(*) as missing_account_code
FROM refined.gl_transaction_clean t
LEFT JOIN refined.coa_master c ON t.account_code = c.account_code
WHERE c.account_code IS NULL;

-- expected: 0 (exceptions should be in curated.gl_exceptions)
