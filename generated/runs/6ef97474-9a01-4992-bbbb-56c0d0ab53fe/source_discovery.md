Source discovery result for: Databricks ELT pipeline for General Ledger (GL) transactions (bronze → silver → gold). Goal: implement append-only bronze raw Delta with schema evolution, silver canonicalization & validation, gold reporting marts, support schema drift, provide notebooks/SQL/DAB/GitHub Actions artifacts.

1) Source Inventory (Ranked)
- 1. Uploaded dataset (high relevance, immediate)
  - Evidence: user said “uploaded dataset” as initial input. Highest priority for discovery and first-run ingestion.
  - Expected locations to check (ranked): DBFS:/FileStore/uploads/, /dbfs/mnt/<ingest>/uploads/, ADLS/S3 mount points used by Databricks.
  - File formats to expect: CSV, JSON, Parquet, Excel (xls/xlsx). Assume CSV/JSON common; detection step will confirm.
  - Relevance score: 10/10
- 2. Future OLTP/ERP GL Database (medium-high)
  - Evidence: user wants future database source ingestion (ERP/GL system).
  - Candidate access methods: JDBC/ODBC (Databricks JDBC), VPN/PrivateLink, Databricks SQL Warehouse.
  - Relevance score: 8/10
- 3. Reference CoA lookup (delta.reference.coa_master) (critical)
  - Evidence: required for silver mapping/account_id resolution and exceptions.
  - Relevance score: 10/10
- 4. Schema drift control/metadata stores (delta.meta.* tables)
  - Evidence: required per context summary (ingest_audit, column_mapping, quarantine).
  - Relevance score: 9/10

2) Schema & Profiling Summary (inferred / actionable discovery steps)
- Canonical Silver schema (provided / target)
  - transaction_id: STRING (nullable → generate synthetic if missing)
  - posting_date: DATE (required)
  - account_code: STRING (required)
  - account_id: STRING (nullable until mapped)
  - amount: DECIMAL(38,10) (signed canonical amount)
  - debit_credit: STRING(1) (optional)
  - cost_center: STRING (nullable)
  - currency: STRING(3) (nullable)
  - source_system: STRING (nullable, new driftable)
  - posting_batch_id: STRING (nullable, new driftable)
  - raw_source_file: STRING
  - raw_row_id: STRING
  - ingest_ts: TIMESTAMP
  - provenance fields (e.g., job_run_id)
  - Unique key: transaction_id OR (source_system, posting_batch_id, raw_row_id)
- Current profiling: Not available until sources are inspected. Do not fabricate counts. Use the commands below to compute:
  - Row counts
  - Null rates by column
  - Distinct counts for transaction_id/account_code/cost_center
  - Min/max posting_date
  - Value distributions for debit_credit and currency
  - Examples (masked) of account_code and amount formatting
- Profiling commands (runnable — run on Databricks workspace against discovered file/table; examples below in Discovery Queries)

3) Recommended Bronze / Raw Ingestion Specs (implementation-ready)
- Target table: delta.bronze.gl_transactions_raw (in specified catalog/schema; e.g., <catalog>.bronze.gl_transactions_raw)
- Write semantics
  - Mode: Append-only Delta
  - Delta options: mergeSchema = true, overwriteSchema = false
  - Isolation: default Delta ACID writes
- Schema evolution policy
  - Allow nullable new columns (mergeSchema=true)
  - Do NOT drop columns in bronze. All columns are additive and nullable by default.
- Full vs incremental
  - Bronze ingestion: append-only per file / per batch (use file-level ingestion). Treat each uploaded file as source partition; capture file metadata and ingest_ts.
  - Incremental for DB source: use a high-watermark column (source timestamp or CDC). If CDC available, use CDC flow; otherwise, use last_modified or ingestion watermark.
  - Offer both: (A) File ingestion incremental by processed_files audit (skip previously ingested files), (B) DB incremental via WHERE last_modified > last_watermark.
- Watermark & PK candidates
  - Watermark: ingest_ts (file ingestion time) and source_event_ts (if present, e.g., posting_date) for downstream deduping.
  - PK candidates: transaction_id OR composite (source_system, posting_batch_id, raw_row_id)
- Deduplication strategy (two options)
  - Option 1 (recommended default): late-dedup in Silver — bronze remains append-only, handle dedupe during MERGE INTO delta.silver.gl_transaction_clean using last ingest_ts per business key.
  - Option 2 (if required early): dedupe at bronze write using a job that performs DISTINCT ON business key within batch and writes only first/last row; caution: more expensive and may drop useful raw rows.
- Partitioning
  - Bronze: partition by ingest_date = date(ingest_ts) (small number of partitions) OR by year_month derived from posting_date if posting_date is reliably present.
  - Silver/Gold: silver not heavily partitioned; gold aggregates partitioned by posting_date (daily) & account_id.
- File format
  - Delta format; small files avoided via autoOptimize/optimize as a scheduled post-write step.
- Retention & retention-of-raw
  - Keep raw bronze for retention period (default 3 years), then cold-tier or snapshot; TTL governed by Finance/Governance.
  - Maintain delta.meta.ingest_audit for life of dataset.
- Auditability & lineage capture
  - At each job run, write row counts, schema_json, schema_hash, job_run_id, start_ts, end_ts to delta.meta.ingest_audit.
  - Record column mapping suggestions/renames in delta.meta.column_mapping.
- Quarantine
  - Fatal parse errors / malformed rows to delta.meta.quarantine_gl_transactions with error_code, message, raw_payload (masked if PII), original file reference.
- Drift handling specifics
  - New columns: accepted into bronze automatically (mergeSchema); automatically appended as nullable.
  - Datatype widening (e.g., amount precision): allow delta to widen decimals using ALTER TABLE ... ALTER COLUMN when safe; track candidate change in ingest_audit and notify.
  - Renamed fields: detect via schema diff; propose mapping rules written to delta.meta.column_mapping and apply mappings in silver transformation to unify names.

4) Discovery Queries / Commands (runnable) — run in Databricks notebooks / Databricks SQL
- A. Locate uploaded files (adjust mount/path)
  - Python (Databricks notebook)
  ```python
  import os, fnmatch
  base_paths = ["/dbfs/FileStore/uploads", "/dbfs/mnt/ingest/uploads", "/dbfs/mnt/landing/gl"]
  for p in base_paths:
      if os.path.exists(p):
          for root, dirs, files in os.walk(p):
              for f in files:
                  if fnmatch.fnmatch(f, "*.csv") or fnmatch.fnmatch(f, "*.json") or fnmatch.fnmatch(f, "*.parquet"):
                      print(os.path.join(root, f))
  ```
- B. Read sample & infer schema (CSV/JSON)
  - Python (Spark)
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.getOrCreate()
  sample_file = "dbfs:/FileStore/uploads/gl_transactions_sample.csv"  # replace
  df = spark.read.option("header","true").option("inferSchema","true").option("multiLine","true").csv(sample_file)
  display(df.limit(10))
  df.printSchema()
  ```
  - SQL
  ```
  CREATE OR REPLACE TEMP VIEW v_gl_sample AS
  SELECT * FROM csv.`/FileStore/uploads/gl_transactions_sample.csv` OPTIONS (header="true", inferSchema="true");
  SELECT * FROM v_gl_sample LIMIT 10;
  DESCRIBE TABLE v_gl_sample;
  ```
- C. Profiling queries (run after reading into df or temp view)
  - Python (Spark) sample metrics
  ```python
  from pyspark.sql.functions import col, count, countDistinct, min, max, expr
  metrics = {}
  metrics['row_count'] = df.count()
  metrics['min_posting_date'] = df.selectExpr("min(posting_date)").collect()[0][0]
  metrics['max_posting_date'] = df.selectExpr("max(posting_date)").collect()[0][0]
  null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
  distinct_counts = {c: df.select(countDistinct(col(c))).collect()[0][0] for c in ['transaction_id','account_code','cost_center']}
  print(metrics, null_counts, distinct_counts)
  ```
  - SQL examples
  ```
  SELECT
    count(*) as row_count,
    min(posting_date) as min_posting_date,
    max(posting_date) as max_posting_date
  FROM v_gl_sample;

  SELECT
    sum(case when transaction_id is null then 1 else 0 end) as null_transaction_id,
    sum(case when account_code is null then 1 else 0 end) as null_account_code
  FROM v_gl_sample;
  ```
- D. Detect schema drift against canonical silver (schema diff)
  - Python to compute schema JSON/hash
  ```python
  import json, hashlib
  def schema_hash(df):
      s = json.dumps(sorted([(f.name, str(f.dataType), f.nullable) for f in df.schema.fields]))
      return hashlib.sha256(s.encode("utf-8")).hexdigest()

  current_hash = schema_hash(df)
  # compare with recorded latest hash from delta.meta.ingest_audit
  spark.sql("SELECT schema_hash FROM delta.meta.ingest_audit WHERE dataset='gl_transactions_raw' ORDER BY ts DESC LIMIT 1").show()
  ```
- E. Create/append to bronze table (append + mergeSchema)
  - Python
  ```python
  df = spark.read.option("header", "true").csv("dbfs:/FileStore/uploads/gl_transactions_sample.csv")
  from pyspark.sql.functions import current_timestamp, lit, input_file_name, monotonically_increasing_id
  df2 = df.withColumn("raw_source_file", input_file_name()) \
          .withColumn("raw_row_id", monotonically_increasing_id()) \
          .withColumn("ingest_ts", current_timestamp())
  df2.write.format("delta") \
     .option("mergeSchema","true") \
     .mode("append") \
     .saveAsTable("bronze.gl_transactions_raw")
  ```
  - SQL (Databricks SQL)
  ```
  CREATE TABLE IF NOT EXISTS bronze.gl_transactions_raw
  USING delta
  LOCATION 'dbfs:/mnt/delta/bronze/gl_transactions_raw'
  AS SELECT * FROM v_gl_sample WHERE 1=0;

  INSERT INTO bronze.gl_transactions_raw
  SELECT *, current_timestamp() as ingest_ts, input_file_name() as raw_source_file
  FROM v_gl_sample;
  ```
- F. Create meta tables (DDL)
  - SQL samples
  ```
  CREATE TABLE IF NOT EXISTS meta.ingest_audit (
    dataset STRING,
    job_run_id STRING,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    row_count LONG,
    schema_json STRING,
    schema_hash STRING,
    status STRING,
    notes STRING
  ) USING delta;

  CREATE TABLE IF NOT EXISTS meta.column_mapping (
    source_column STRING,
    canonical_column STRING,
    mapping_reason STRING,
    detected_ts TIMESTAMP,
    resolved BOOLEAN
  ) USING delta;

  CREATE TABLE IF NOT EXISTS meta.quarantine_gl_transactions (
    source_file STRING,
    raw_row_id STRING,
    error_code STRING,
    error_message STRING,
    raw_payload STRING,
    ingest_ts TIMESTAMP
  ) USING delta;
  ```
- G. Detect column renames candidate via similarity (runnable)
  - Python (simple heuristic)
  ```python
  # compare bronze schema columns to canonical list
  canonical = ['transaction_id','posting_date','account_code','amount','debit_credit','cost_center','currency','source_system','posting_batch_id']
  bronze_cols = [f.name for f in spark.table("bronze.gl_transactions_raw").schema.fields]
  candidates = []
  from difflib import SequenceMatcher
  for b in bronze_cols:
      best = max(canonical, key=lambda c: SequenceMatcher(None, b, c).ratio())
      score = SequenceMatcher(None, b, best).ratio()
      if score > 0.7 and b != best:
          candidates.append((b, best, score))
  candidates
  ```
  - Write suggestions to meta.column_mapping.

5) Metadata / Lineage Stubs
- Dataset descriptions and ownership (stubs to fill)
  - bronze.gl_transactions_raw
    - description: Append-only raw GL transactions as ingested from files/DB. Schema-evolution enabled.
    - owner: Data Engineering (placeholder)
    - tags: pii=false?, finance=true, raw=true
    - freshness: updated on file arrival / DB schedule
  - silver.gl_transaction_clean
    - description: Cleaned, normalized GL transactions; account_code mapped to account_id via delta.reference.coa_master, canonical amount signed, validated posting_date.
    - owner: Data Engineering / Finance owner for CoA
    - tags: finance=true, pii=false, trusted=true
  - gold.gl_balance_daily, gl_variance_monthly, gl_exceptions
    - descriptions per names. Owners: Finance BI.
- Meta tables
  - meta.ingest_audit: ingestion runs, schema_hash, counts
  - meta.column_mapping: detected rename/alias mapping suggestions and status
  - meta.quarantine_gl_transactions: fatal rows with error codes & remediation notes
- Lineage approach
  - If Unity Catalog available, register tables there and attach column-level descriptions and tags.
  - Else store lineage as JSON in meta.ingest_audit and track upstream source_file, job_run_id → silver/gl/gold datasets.

6) Recommended Silver Transformations (short runnable patterns)
- Notebook should include both Python (PySpark) and SQL cells.

- Python: canonicalization + validation snippet (run in notebook as job)
  ```python
  # Read raw bronze partition (incremental by ingest_ts watermark)
  from pyspark.sql.functions import col, to_date, lit, when, coalesce, current_timestamp, expr, concat_ws
  last_watermark = spark.sql("SELECT max(end_ts) as last_ts FROM meta.ingest_audit WHERE dataset='silver.gl_transaction_clean'").collect()[0]['last_ts']
  bronze_df = spark.table("bronze.gl_transactions_raw").filter(col("ingest_ts") > lit(last_watermark))

  # Apply rename mapping from meta.column_mapping
  mappings = spark.table("meta.column_mapping").filter("resolved = false").collect()
  for r in mappings:
      if r.source_column in bronze_df.columns:
          bronze_df = bronze_df.withColumnRenamed(r.source_column, r.canonical_column)

  # Parse posting_date safely
  from pyspark.sql.functions import to_date, unix_timestamp
  safe = bronze_df.withColumn("posting_date_parsed", to_date(col("posting_date")))
  invalid_dates = safe.filter(col("posting_date_parsed").isNull() & col("posting_date").isNotNull())
  invalid_dates.select("raw_source_file","raw_row_id","posting_date").limit(10).show()

  # Coerce numeric to DECIMAL(38,10)
  from pyspark.sql.types import DecimalType
  safe = safe.withColumn("amount_canonical", when(col("amount").rlike("^-?\\d+(\\.\\d+)?$"), col("amount").cast(DecimalType(38,10))).otherwise(None))

  # Normalize sign from debit_credit if present
  canonical = safe.withColumn("amount_signed", 
                  when(col("debit_credit").isin("D","d"), col("amount_canonical"))
                  .when(col("debit_credit").isin("C","c"), -col("amount_canonical"))
                  .otherwise(col("amount_canonical"))
               )

  # Map account_code -> account_id via reference.coa_master
  coa = spark.table("reference.coa_master").select("account_code","account_id")
  joined = canonical.join(coa, on="account_code", how="left")
  # Flag unmapped accounts to exceptions/quarantine
  unmapped = joined.filter(col("account_id").isNull())
  unmapped.select("raw_source_file","raw_row_id","account_code").limit(10).show()

  # Generate transaction_id if missing (synthetic)
  from pyspark.sql.functions import monotonically_increasing_id
  joined = joined.withColumn("transaction_id", coalesce(col("transaction_id"), concat_ws("-", col("source_system"), col("posting_batch_id"), col("raw_row_id"))))
  # Select canonical columns
  canonical_out = joined.selectExpr(
      "transaction_id",
      "posting_date_parsed as posting_date",
      "account_code",
      "account_id",
      "amount_signed as amount",
      "debit_credit",
      "cost_center",
      "currency",
      "source_system",
      "posting_batch_id",
      "raw_source_file",
      "raw_row_id",
      "ingest_ts"
  )
  # Write to silver via MERGE (idempotent)
  silver_table = "silver.gl_transaction_clean"
  # staging & merge logic below (SQL cell)
  canonical_out.createOrReplaceTempView("staging_gl")
  ```
- SQL: MERGE into silver (idempotent)
  ```
  MERGE INTO silver.gl_transaction_clean tgt
  USING (SELECT * FROM staging_gl) src
  ON tgt.transaction_id = src.transaction_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```

- Quarantine invalid posting_date rows:
  - Insert rows with posting_date_parsed is null and posting_date not null into meta.quarantine_gl_transactions with error_code='INVALID_DATE' and details.

7) Gold Aggregates (pattern queries)
- gl_balance_daily (SQL)
  ```
  CREATE OR REPLACE TABLE gold.gl_balance_daily
  USING delta
  PARTITIONED BY (posting_date)
  AS
  SELECT posting_date, account_id, cost_center, currency, sum(amount) as balance
  FROM silver.gl_transaction_clean
  GROUP BY posting_date, account_id, cost_center, currency;
  ```
- gl_variance_monthly (SQL)
  ```
  CREATE OR REPLACE TABLE gold.gl_variance_monthly
  USING delta
  PARTITIONED BY (year_month)
  AS
  WITH monthly AS (
    SELECT date_trunc('month', posting_date) as month_dt, account_id, cost_center, sum(amount) as monthly_total
    FROM silver.gl_transaction_clean
    GROUP BY date_trunc('month', posting_date), account_id, cost_center
  )
  SELECT m1.month_dt as month, m1.account_id, m1.cost_center,
         m1.monthly_total,
         m1.monthly_total - coalesce(m2.monthly_total, 0) AS month_over_month_variance
  FROM monthly m1
  LEFT JOIN monthly m2 ON m2.month_dt = add_months(m1.month_dt, -1) AND m1.account_id = m2.account_id AND m1.cost_center = m2.cost_center;
  ```
- gl_exceptions (SQL)
  ```
  CREATE OR REPLACE TABLE gold.gl_exceptions
  USING delta
  AS
  SELECT transaction_id, posting_date, account_code, account_id, cost_center, amount, 'UNMAPPED_ACCOUNT' as exception_type, current_timestamp() as flagged_ts
  FROM silver.gl_transaction_clean
  WHERE account_id IS NULL
  UNION ALL
  SELECT transaction_id, posting_date, account_code, account_id, cost_center, amount, 'MISSING_COST_CENTER', current_timestamp()
  FROM silver.gl_transaction_clean
  WHERE cost_center IS NULL;
  ```

8) DAB (Databricks Asset Bundle) & GitHub Actions CI/CD stubs
- DAB structure (artifact list)
  - notebooks/
    - 01_ingest_bronze.py (parametrized notebook)
    - 02_silver_transform.py
    - 03_gold_aggregates.py
    - 04_quarantine_remediation.py
    - 99_backfill.py
  - sql/
    - create_meta_tables.sql
    - create_bronze_table.sql
    - merge_silver.sql
    - gold_aggregates.sql
  - tests/
    - unit_tests/ (pytest for Python transformation functions)
    - data_tests/ (SQL asserts to run post-job)
  - deployment/
    - job_definitions/ (Databricks Jobs JSON)
    - service_principals.md
- GitHub Actions (CI) pipeline (high-level steps)
  - on: push, PR to main
  - jobs:
    - lint: run Python linters (black/isort/flake8)
    - unit-tests: run pytest on tests/unit_tests
    - data-tests: run integration job in sandbox Databricks workspace (Databricks CLI or GitHub Action), run SQL tests (row counts, not null gating)
    - build-dab: package notebooks + SQL into DAB artifact
    - deploy: on success deploy DAB to target workspace via Databricks REST API / databricks-cli
- Example GitHub Actions snippet (conceptual)
  - Steps: checkout, setup python, install deps, run tests, call databricks action to deploy DAB.

9) Tests to include
- Unit tests (pytest): sign normalization samples; date parsing edge cases; mapping lookup fallback; synthetic transaction_id generation.
- Data tests (SQL):
  - row_count > 0 after ingestion
  - percent_null(posting_date) < threshold (e.g., 2%) or flag
  - no duplicate transaction_id for same business day unless expected
  - total balance reconciliation test vs source aggregates (where available)
- Integration tests:
  - End-to-end ingest sample file → bronze → silver → gold in sandbox with expected counts.

10) Discovery Actions (exact steps to run when access missing)
- Step 0: Provide mount/paths and DB credentials (do not share here). Required values:
  - path of uploaded files (dbfs path or mount)
  - JDBC connection string or Databricks SQL endpoint for DB source
  - reference CoA table location or grant to access reference.coa_master
- Step 1: Run file listing script (see 4.A) to enumerate candidate files.
- Step 2: For representative files, run schema inference (4.B) and profiling (4.C).
- Step 3: Create bronze table skeleton (4.F) and ingest a sample file (4.E).
- Step 4: Run schema diff script (4.D) and write schema_json & hash into meta.ingest_audit.
- Step 5: Run silver notebook (transformation pattern in 6) on sample data and validate tests.
- Step 6: Create DAB and run GitHub Actions CI in sandbox.

11) Metadata / Lineage Stubs (example rows)
- meta.ingest_audit example record (write after ingest)
  - dataset: "bronze.gl_transactions_raw"
  - job_run_id: "<job-run-guid>"
  - start_ts / end_ts
  - row_count: <computed>
  - schema_json: "<json string>"
  - schema_hash: "<sha256>"
  - status: "SUCCESS"
- meta.column_mapping
  - source_column: "post_date"
  - canonical_column: "posting_date"
  - mapping_reason: "rename detected via similarity 0.82"
  - detected_ts: <ts>
  - resolved: false

12) Next Actions (minimum, ordered)
- 1) Provide exact upload path(s) for current dataset(s) OR grant Data Engineering access to DBFS/mount where uploads land.
- 2) Confirm presence/access to reference CoA (delta.reference.coa_master) and contact for Finance owner.
- 3) Run the discovery notebook (01_discovery.ipynb) which performs file listing, sample reads, schema inference, and writes initial meta.ingest_audit. Use the scripts/commands above.
- 4) After discovery run, review meta.column_mapping suggestions and approve mappings (or provide mapping file).
- 5) Execute the bronze ingest job with a sample file; verify meta.ingest_audit and run silver pipeline; run tests and review exceptions.

13) Open Questions (minimum)
- A) Where exactly are uploaded files located (dbfs path or mount)? Provide example file(s) or grant access.
- B) What sample file formats should we expect (CSV, JSON, Parquet, Excel)? Are headers present? Delimiter specifics?
- C) Is the default canonical sign debits = positive? (context said default, confirm if different)
- D) Will the future DB source expose CDC or last_modified timestamps? (affects incremental design)
- E) Who is the owner/contact for delta.reference.coa_master to confirm account mapping rules and default sign override?
- F) Any GDPR/PII concerns in GL payloads (names, employee IDs) needing masking before landing in bronze?

14) Assumptions
- No access to files or DB currently — discovery commands provided to be executed in user workspace.
- Bronze will be Delta tables in Databricks (Unity Catalog optional).
- CoA reference exists and is accessible as delta.reference.coa_master (if not, discovery will find its absence).
- Default canonical sign: debits positive unless Finance indicates otherwise.
- Ingest frequency: file-based uploads drive bronze; DB source will be scheduled or CDC-enabled later.
- Retention defaults (3 years) and SLA targets follow context summary unless adjusted.

Appendices: Quick runnable notebook skeleton (names & key cells)
- Notebook: 01_ingest_bronze.py
  - Cells:
    - Params: input_path, max_sample_rows, target_bronze_table, job_run_id
    - List files (4.A)
    - Read sample file & infer schema (4.B)
    - Compute profiling (4.C)
    - Write to bronze with mergeSchema=true (4.E)
    - Write ingest_audit entry (4.F)
- Notebook: 02_silver_transform.py
  - Cells:
    - Params: watermark_ts, dry_run (True/False)
    - Load staging (from bronze)
    - Apply column mapping (meta.column_mapping)
    - Validate posting_date; move invalid → quarantine
    - Normalize amount (decimal cast + sign)
    - Map account_code → account_id via reference
    - MERGE into silver.gl_transaction_clean
    - Emit exceptions rows to gold.gl_exceptions and meta.quarantine where fatal

If you want, I will:
- (A) produce the parameterized Databricks notebooks (full code) for 01/02/03 plus tests and DAB folder tree, or
- (B) run the discovery scripts in your Databricks environment if you provide the upload path(s) and grant access.

Which do you prefer?