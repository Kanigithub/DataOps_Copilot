- **Clarifying Questions**
  1. What is the initial upload format & landing location for the dataset (CSV/Parquet/Excel; DBFS/ADLS/S3 path)? If unknown, may I assume CSV uploads to a known DBFS path?
  2. What is the future database source (RDBMS type, JDBC, or a specific system)? Will CDC be available from that source?
  3. What are the canonical column names & required keys for GL transactions? If none provided, may I propose a minimal contract (transaction_id, posting_date, account_code, amount, debit_credit, cost_center, currency)?
  4. Which sign convention do you want as canonical for amounts (debits positive/credits negative or vice versa)?
  5. Is there a canonical Chart-of-Accounts (CoA) lookup table (account_code → normalized_account_id/name)? Who owns/maintains it and where will it live (Delta table)?
  6. Desired SLA / freshness for reporting marts (e.g., daily within 1 hour after close, or near-real-time)?
  7. Should historical corrections be modeled as SCD Type 1 (overwrite) or Type 2 (versioned) for transactions and accounts?
  8. Alerts/notification channels for pipeline failures (email, PagerDuty, Slack)? Any required retention period for raw data?
  9. Do you have Unity Catalog / Table Lineage enabled, or should we capture lineage in a custom audit/lineage Delta table?
  10. Who are primary consumers of gold marts (BI tool names: PowerBI/Tableau/Looker) and are there expected partitioning keys they rely on (account, cost_center, date)?

If you cannot answer, I will proceed with safe defaults (see Assumptions).

---

- **Final Pipeline Blueprint**

Summary
- Platform: Databricks + Delta Lake.
- Delivery artifact: Notebooks (Databricks Python), SQL scripts, Delta tables, Databricks Asset Bundle (DAB), GitHub Actions CI/CD.
- Stages: Bronze (raw append-only), Silver (clean/enriched: gl_transaction_clean), Gold (gl_balance_daily, gl_variance_monthly, gl_exceptions).
- Drift-tolerant: schema-evolution on write, mapping table for renamed fields, automatic datatype widening, quarantine for invalid rows.
- Observability: audit/lineage Delta table, job metrics, alerting hooks.

1) Sources & Targets (ownership)
- Uploaded dataset (owner: Data Provider) → Bronze: delta.`bronze.gl_transactions_raw`
- Future DB source (owner: Source System Team) via JDBC or CDC → same Bronze
- CoA lookup table (owner: Finance) → delta.`reference.coa_master` (canonical)
- Silver clean/main table (owner: Data Platform) → delta.`silver.gl_transaction_clean`
- Gold marts (consumers/BI owners):
  - delta.`gold.gl_balance_daily`
  - delta.`gold.gl_variance_monthly`
  - delta.`gold.gl_exceptions`
- Metadata/audit tables (owner: Data Platform) → delta.`meta.ingest_audit`, delta.`meta.column_mapping`

2) Refresh mode, schedule, SLA
- Bronze: append-only batch ingestion. Trigger on upload or scheduled job. Default schedule: every 15 minutes for uploaded batches; support ad-hoc runs. SLA default: Bronze available within 15 min of upload.
- Silver: batch ETL job runs hourly (or after scheduled daily close). SLA: silver updated within 1 hour of source availability.
- Gold: daily aggregates scheduled once per day post-close (e.g., 02:00 AM). SLA: gold tables ready within 2 hours after close. (Adjustable based on Q6.)
- Backfills: parameterized one-time jobs.

3) Data Contracts (proposed minimal contract — explicit if unknown)
- Target table: silver.gl_transaction_clean (canonical columns)
  - transaction_id: STRING NOT NULL (business unique id if available; otherwise synthetic + source_file + row_number)
  - posting_date: DATE NOT NULL (ISO date)
  - accounting_period: STRING NULL (derived)
  - account_code: STRING NOT NULL
  - account_id: STRING NULL (mapped from CoA)
  - amount: DECIMAL(38,10) NOT NULL (signed: canonical sign convention)
  - debit_credit: STRING(1) NULL (D/C) — optional if amount sign used
  - cost_center: STRING NULL
  - currency: STRING(3) NULL (ISO)
  - source_system: STRING NULL
  - posting_batch_id: STRING NULL
  - raw_source_file: STRING NULL
  - raw_row_id: STRING NULL
  - ingest_ts: TIMESTAMP NOT NULL
- Constraints & keys:
  - Unique key: transaction_id if present; else composite (source_system, posting_batch_id, raw_row_id).
  - posting_date must be within allowed range (1900-01-01 to now+1 day).
  - account_code must be present and map to CoA unless exception.
  - amount must be numeric; cast to DECIMAL(38,10) to handle precision widening.

4) Transformations / Business rules (plain language + pseudologic)
- Bronze ingestion: write incoming file(s) as-is to bronze Delta with schema merge enabled. Capture ingest metadata.
  - Pseudocode (python):
    ```python
    # Python: read raw file(s) and write append to bronze with schema evolution
    df = spark.read.option("header", "true").csv(input_path)
    df = df.withColumn("ingest_ts", current_timestamp()).withColumn("raw_source_file", lit(input_path))
    df.write.format("delta").mode("append").option("mergeSchema","true").save(bronze_path)
    ```
- Schema drift handling:
  - New columns: allowed and persisted into bronze automatically.
  - Datatype widening: when reading bronze to silver, coerce numeric types to DECIMAL(38,10).
  - Renamed fields: maintain delta.`meta.column_mapping` table containing mappings (old_name → new_name → active_flag). Apply mapping in silver ingestion.
  - Example mapping application (python):
    ```python
    mapping = spark.table("meta.column_mapping").filter("active = true").collect()
    # apply rename mapping before other transforms
    ```
- Silver cleaning (create gl_transaction_clean):
  - Normalize CoA: map account_code → account_id/name using reference.coa_master. If account_code missing or unmapped → flag exception.
  - Validate posting_date: parse, cast to DATE, if invalid -> send to quarantine with error_code 'INVALID_DATE'.
  - Standardize sign convention:
    - Rule: if debit_credit provided: amount_signed = amount * (1 if debit_credit == "D" else -1)
    - Else if sign ambiguous: follow rule canonical_debit_positive (default: debits positive).
    - After conversion ensure amount is DECIMAL(38,10).
  - Populate ingest_ts and provenance columns.
  - SQL example:
    ```sql
    -- SQL (silver transformation)
    SELECT
      COALESCE(transaction_id, concat(source_system, '_', posting_batch_id, '_', raw_row_id)) as transaction_id,
      to_date(coalesce(posting_date, post_date), 'yyyy-MM-dd') as posting_date,
      account_code,
      coa.account_id as account_id,
      CASE
        WHEN debit_credit IS NOT NULL THEN CAST(amount * CASE WHEN upper(debit_credit) = 'D' THEN 1 ELSE -1 END AS DECIMAL(38,10))
        WHEN amount < 0 THEN amount
        ELSE amount
      END as amount,
      cost_center,
      currency,
      source_system,
      posting_batch_id,
      ingest_ts
    FROM bronze.gl_transactions_raw raw
    LEFT JOIN reference.coa_master coa ON raw.account_code = coa.account_code
    WHERE posting_date IS NOT NULL
    ```
- Exceptions: any row failing validation (invalid date, missing account_code, missing cost_center if mandatory) goes into delta.`gold.gl_exceptions` with columns (transaction_id, error_code, error_message, raw_row, processed_ts).
- Gold marts:
  - gl_balance_daily: compute daily ending balance per account_id x cost_center:
    - group by posting_date, account_id, cost_center: sum(amount) as daily_balance; also include running balance if required.
    - Partition by posting_date (date) and account_id for performance.
  - gl_variance_monthly: month-over-month variance:
    - For each account_id & cost_center & month, compute sum(amount) as month_total; compute variance = month_total - prev_month_total and percent_change.
  - gl_exceptions: as above, store raw and normalized failing rows with error metadata.

5) Incremental strategy / SCD
- Bronze: append-only; each ingest adds rows with ingest_ts.
- Silver: incremental processing using watermark on ingest_ts (e.g., process rows where ingest_ts > last_processed_ts OR where file <= last_processed_file and not marked processed). Use job-run metadata in meta.ingest_audit to identify delta.
- Merge pattern for idempotent writes:
  - Use MERGE INTO silver.gl_transaction_clean using staging delta (staging table) keyed on transaction_id or synthetic key.
  - SCD: transactions = SCD Type 1 (overwrite) by default (historical corrections will replace existing records). If the business needs SCD2, change to Type 2 and add valid_from/valid_to columns. Default is Type 1.
- Gold: recompute aggregates daily from silver; incremental by date partitions (processing only new/updated posting_date ranges using change detection via silver.audit_run_id).

6) Error handling, quarantine, backfill
- Quarantine / Dead-letter Queue:
  - delta.`meta.quarantine_gl_transactions` with columns: raw_row (struct), error_code, error_message, first_seen_ts, source_path, job_run_id.
  - Failed rows and schema-mismatched rows are directed here. Create a remediation notebook for manual/automated fixes.
- Data quality rejection:
  - Non-fatal: row persists to silver with flagged columns (e.g., unmapped account) and also recorded in exceptions.
  - Fatal (e.g., cannot parse posting_date): move to quarantine and do not insert.
- Backfill:
  - Parameterized "backfill" notebook that accepts date range / source file list and reprocesses Bronze -> Silver -> Gold using a forced full-run for the date range.
  - Use snapshot isolation / staging table and atomic MERGE to avoid double-counting.
- Retry policy:
  - Retries via Databricks job retries (configurable, default 3 attempts), with exponential backoff.

7) Testing requirements and release gates
- Unit tests:
  - Python unit tests for transformation utility functions (sign normalization, date parsing, account mapping).
- Data tests:
  - Row count verification after each stage (bronze ingest count matches input).
  - Schema contract tests: required fields present and types as expected.
  - Referential integrity: account_code -> coa_master mapping coverage (threshold e.g., >= 99.5%).
  - Nullability checks: posting_date must not be null in final silver.
- Integration tests:
  - End-to-end small-sample run in ephemeral sandbox workspace (DAB + test dataset) asserting final counts and aggregates.
- Acceptance gates in CI:
  - All unit tests pass.
  - Data tests pass thresholds.
  - No high-severity exceptions in gold.
- Tests implementation:
  - Pytest for python logic; simple SQL assertions or Great Expectations-like checks for data tests (can be executed as notebooks).
- GitHub Actions: run unit tests and data tests, then deploy DAB to Databricks workspace and schedule jobs if tests succeed.

8) Observability & lineage
- Ingest audit table delta.`meta.ingest_audit`:
  - job_run_id, job_name, source_path, source_count, inserted_count, updated_count, error_count, start_ts, end_ts, status, input_schema_hash.
- Lineage:
  - If Unity Catalog available, enable UC lineage. Else capture lineage metadata in ingest_audit and a separate delta.`meta.column_mapping`.
- Metrics & Alerts:
  - Freshness alert: if gold.latest_posting_date < expected_cutoff (e.g., yesterday for daily), alert.
  - Failure alert: job status != SUCCESS -> send to Slack/Email/PagerDuty.
  - Data-volume anomaly: sudden +/- > 50% change triggers alert.
- Dashboard: basic Databricks SQL dashboard for row counts, error counts, and recency.

9) Deployment & CI/CD
- Artifacts:
  - Notebooks: 1) bronze_ingest.py/ipynb, 2) silver_transform.py/ipynb, 3) gold_aggregates.sql, 4) backfill_notebook.ipynb, 5) remediation_notebook.ipynb
  - SQL scripts for merges and table definitions.
  - Tests: tests/unit/, tests/data/
  - DAB manifest (databricks asset bundle) describing notebooks, jobs, and table ACLs.
- GitHub Actions workflow (high-level):
  - on: push/PR
  - jobs:
    - lint (flake8, sqlfluff)
    - unit-tests (pytest)
    - data-tests (runs small sample job on staging workspace)
    - build-dab
    - deploy: upload DAB and register jobs via databricks-cli or Databricks REST API
- Databricks Job definitions:
  - Job 1: bronze_ingest (triggered on upload or schedule)
  - Job 2: silver_transform (depends on Job 1)
  - Job 3: gold_aggregates (depends on Job 2)
  - Alerts configured on job failures.

10) Implementation snippets (Python + SQL)
- Bronze ingest (Python databricks notebook cell):
  ```python
  # python
  from pyspark.sql.functions import current_timestamp, input_file_name, lit
  df = spark.read.option("header","true").csv("/mnt/landing/gl_uploads/*.csv")
  df = df.withColumn("ingest_ts", current_timestamp()).withColumn("raw_source_file", input_file_name())
  df.write.format("delta").mode("append").option("mergeSchema","true").save("/delta/bronze/gl_transactions_raw")
  ```
- Apply rename mapping & type coercion (Python):
  ```python
  # python
  mapping_df = spark.table("meta.column_mapping").filter("active = true")
  rename_map = {r.old_name: r.new_name for r in mapping_df.collect()}
  raw_df = spark.read.format("delta").load("/delta/bronze/gl_transactions_raw")
  for old, new in rename_map.items():
      if old in raw_df.columns and new not in raw_df.columns:
          raw_df = raw_df.withColumnRenamed(old, new)
  # Coerce numeric widening
  from pyspark.sql.types import DecimalType
  raw_df = raw_df.withColumn("amount", raw_df["amount"].cast(DecimalType(38,10)))
  ```
- Silver MERGE into clean table (SQL):
  ```sql
  -- sql
  MERGE INTO silver.gl_transaction_clean tgt
  USING staging.gl_transaction_staging src
  ON tgt.transaction_id = src.transaction_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```

11) Deliverables
- Notebooks with above code and parameterization
- SQL scripts for MERGE and aggregate creation
- DAB manifest and packaging scripts
- GitHub Actions .github/workflows/ci-cd.yml
- Test suites (unit + data)
- Operational runbook (how to remediate quarantined rows, how to backfill)

---

- **Assumptions**
1. Initial upload format: CSV files landed on DBFS (if different, ingestion code will be parameterized).
2. Future DB source does not provide CDC by default; we will use timestamp-based incremental ingestion. If CDC exists, we will switch to CDC ingestion.
3. Canonical sign convention: debits are positive, credits negative (changeable).
4. SCD behavior: transactions are SCD Type 1 (overwrite). If the business needs history, we will implement SCD Type 2.
5. CoA master lookup exists or will be supplied; if not, mapping will be implemented via a maintainable reference table in delta.`reference.coa_master`.
6. No explicit SLA given → default gold readiness daily within 2 hours after close.
7. Unity Catalog not assumed; we capture lineage metadata in delta.`meta.ingest_audit`.
8. Alerts will use Databricks job webhooks; integration to Slack/PagerDuty will be configured later.

---

- **Acceptance Criteria (Given / When / Then)**

1) Bronze ingest
- Given: a valid upload file is placed at /mnt/landing/gl_uploads/2026-07-01.csv
- When: the bronze_ingest job runs
- Then:
  - delta.`bronze.gl_transactions_raw` contains all rows from the file with ingest_ts set
  - schema evolution applies if new columns present (new columns show up in Delta schema)
  - meta.ingest_audit records source_path and row_count

2) Silver cleaning & normalization
- Given: bronze contains a set of raw transactions
- When: silver_transform job runs
- Then:
  - delta.`silver.gl_transaction_clean` has one canonical row per transaction_id (or synthetic key)
  - posting_date is a DATE and within allowed range; invalid-dates moved to meta.quarantine_gl_transactions with error_code 'INVALID_DATE'
  - account_code is mapped to account_id for >= 99.5% of rows; unmapped rows appear in delta.`gold.gl_exceptions`
  - amount is DECIMAL(38,10) and sign normalized per the defined convention

3) Schema drift handling
- Given: a new column "source_system" appears in bronze or a renaming post_date→posting_date is required
- When: silver_transform runs
- Then:
  - New columns persist in bronze automatically
  - The rename mapping in meta.column_mapping is applied; silver uses normalized name posting_date
  - Numeric widening is handled (amount coerced to DECIMAL(38,10)) without data loss

4) Gold aggregations & exceptions
- Given: gl_transaction_clean populated up to date
- When: gold_aggregates job runs for date 2026-07-01
- Then:
  - delta.`gold.gl_balance_daily` contains a partition for posting_date=2026-07-01 with correct sum(amount) per account_id x cost_center
  - delta.`gold.gl_variance_monthly` contains month totals and variance compared to previous month
  - delta.`gold.gl_exceptions` lists invalid account codes or missing cost centers, with raw payload and error metadata

5) Observability & lineage
- Given: any ETL job run completes
- When: run ends (success or failure)
- Then:
  - meta.ingest_audit has an entry with start_ts, end_ts, source_count, inserted_count, error_count
  - If job failure occurs, an alert is emitted to the configured channel

6) CI/CD & Tests
- Given: a PR is opened to the pipeline repo
- When: GitHub Actions runs
- Then:
  - Linting + unit tests pass
  - Data tests on a small sample sandbox run pass
  - If tests pass, DAB is built and deployed to staging workspace

---

If you confirm defaults or answer the clarifying questions, I will produce:
- a prioritized implementation checklist,
- the 6 deliverable notebooks with directory layout and parameter examples,
- a skeleton GitHub Actions workflow and DAB manifest ready to commit.