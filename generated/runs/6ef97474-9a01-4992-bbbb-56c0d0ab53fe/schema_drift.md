**Drift Report**

Summary of observed/expected drift (based on the supplied scenarios):
- Added columns (non-breaking, bronze-acceptable)
  - source_system (new, STRING)
  - posting_batch_id (new, STRING)
  - Confidence: detected as brand-new columns in incoming payloads. Policy: auto-accept in bronze as nullable.
- Datatype widening (non-breaking if widened)
  - amount: existing DECIMAL(38,10) → incoming DECIMAL(38,14) (example)
  - Classification: widening (precision/scale increased) — non-breaking if target tables are widened; detect and track.
- Suspected rename (potentially breaking)
  - post_date → posting_date
  - Heuristic: name similarity, same semantics (date), same value range.
  - Confidence: 0.92 (requires confirmation if policy < 0.9; still can auto-map in silver with audit entry).

Schema diff (canonical silver target vs observed)
- Missing in incoming vs canonical:
  - posting_date (required in silver) — may be present under post_date
- Extra in incoming:
  - source_system, posting_batch_id, other vendor-specific ephemeral columns
- Nullability/type changes:
  - transaction_id: expected STRING (nullable → synthesize if missing)
  - amount: scale/precision increased
- Actionable detection artifacts to produce per ingest:
  - schema_json, schema_hash, column list diff, suggested mappings (meta.column_mapping)

---

**Impact Analysis**

Breaking vs non-breaking classification
- Non-breaking:
  - Additive columns (source_system, posting_batch_id) if nullable — bronze accepts automatically.
  - Datatype widening for numeric types (if target widened): non-breaking if we update downstream column definitions and/or cast on read.
- Breaking:
  - Renames or missing required columns in silver (posting_date missing) → will cause silver validation failures and quarantine entries.
  - Type narrowing or nullability tightening (e.g., making posting_date nullable→required without mapping) — breaking.
  - Dropping columns (never auto-apply) — breaking.

Downstream blast radius (models/tests/consumers affected)
- delta.silver.gl_transaction_clean: HIGH (relies on posting_date, amount semantics, account mapping)
- Gold marts:
  - gold.gl_balance_daily: HIGH (depends on posting_date and account mapping)
  - gold.gl_variance_monthly: HIGH (depends on posting_date/account totals)
  - gold.gl_exceptions: MEDIUM (may pick up new exception types)
- Reference data:
  - delta.reference.coa_master: used for account_code → account_id mapping; unaffected by new columns but impacted if account_code distribution changes.
- Consumers:
  - Finance reports and BI dashboards (PowerBI/Tableau), downstream pipelines that expect exact column names or types.

Tests likely to fail:
- Non-null constraint checks on posting_date
- Referential checks to coa_master for account_code
- Data quality checks for sign normalization and amount ranges

Estimated immediate severity: Medium-to-High until silver mapping for renamed field is applied and amount precision adjustments made.

---

**Recommended Remediation**

High-level policy decisions (contract & version guidance)
- Bronze layer (delta.bronze.*): continue additive policy. Accept new nullable columns automatically (mergeSchema=true).
- Silver layer (delta.silver.*): enforce canonical schema. Do not accept silent renames — explicit mapping required. Apply non-breaking changes via patch release (minor version bump). Breaking changes require contract review + major version bump.
- Contract versioning:
  - Semantic versioning for contracts: MAJOR.MINOR.PATCH
    - Additive, backward-compatible additions → bump MINOR
    - Non-functional changes/bugfixes (metadata only) → bump PATCH
    - Breaking (rename/remove/nullable→required/type narrowing) → bump MAJOR and require sign-off.
- Mapping resolution policy:
  - If suspected rename confidence >= 0.9: auto-suggest mapping and apply as a "soft auto-map" in silver with audit and retention of original column in bronze metadata (no destructive change). Log mapping in meta.column_mapping; mark mapping as "auto-applied" with reviewer notification.
  - If confidence < 0.9: do not auto-apply; create mapping candidate for human review.

Concrete remediation steps (safe-first, minimal downtime)
1. Bronze
   - Continue append-only with mergeSchema=true and partition by ingest_date.
   - Record schema hash and diff in meta.ingest_audit; capture column-level suggestions in meta.column_mapping.
   - Do not drop/alter bronze columns.

2. Silver (gl_transaction_clean)
   - Implement alias mapping layer in silver ingestion:
     - If posting_date is NULL and post_date exists -> set posting_date = CAST(post_date AS DATE); write mapping entry.
   - Normalize amount changes:
     - If incoming amount precision > canonical, widen silver column to the larger precision (DECIMAL(max_precision, max_scale)) OR store as DECIMAL(38,14) and round/cast for gold when appropriate.
   - Enforce posting_date required: rows failing mapping/validation → write to meta.quarantine_gl_transactions with clear error codes.
   - Generate synthetic transaction_id if missing: e.g., sha2(concat(source_system, posting_batch_id, raw_row_id), 256).

3. Gold
   - After silver is validated/cleaned, compute marts. Keep existing gold DDL but ensure amount types match gold expectations; adapt conversions at aggregation stage.
   - Retain historical data; avoid in-place destructive changes.

4. Backfill / Reprocessing
   - For rename mapping: run one-time reprocess of bronze rows whose posting_date is NULL but post_date is populated. Reprocess into silver via the new mapping job. This is expected and non-destructive (reads bronze, writes new silver rows with dedupe).
   - For amount precision widening: if silver/gold stored with lower precision, backfill by reprocessing silver with widened DDL and re-materializing gold aggregates for affected time window (see Backfill plan).

5. Tests & Automation
   - Add regression tests:
     - posting_date not null
     - transaction_id presence via business key rule
     - amount precision check (no truncation)
     - account_code present in coa_master
     - sign convention normalization
   - Add a mapping resolution workflow for meta.column_mapping items (GitHub issues or ticket integration).

---

**Patch Suggestions**

Below are implementation-ready snippets to add to notebooks / SQL scripts / job definitions.

A. Bronze ingestion notebook (Python) — append mode with mergeSchema
```python
# Python: bronze_ingest.py (Databricks notebook cell)
from pyspark.sql import functions as F

df = (spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("/mnt/landing/gl_transactions/latest/*.csv"))
# add ingest metadata
df = df.withColumn("raw_source_file", F.input_file_name()) \
       .withColumn("raw_row_id", F.monotonically_increasing_id()) \
       .withColumn("ingest_ts", F.current_timestamp()) \
       .withColumn("ingest_date", F.to_date(F.current_timestamp()))

(df.write
   .format("delta")
   .option("mergeSchema","true")
   .mode("append")
   .partitionBy("ingest_date")
   .saveAsTable("delta.bronze.gl_transactions_raw"))
# write ingest audit (example)
schema_json = df.schema.json()
schema_hash = hashlib.sha256(schema_json.encode('utf-8')).hexdigest()
spark.createDataFrame([("delta.bronze.gl_transactions_raw", "job_run_id_XXX", F.current_timestamp(), F.lit(df.count()), F.lit(schema_json), F.lit(schema_hash), "SUCCESS")]) \
     .toDF("dataset","job_run_id","ingest_ts","row_count","schema_json","schema_hash","status") \
     .write.format("delta").mode("append").saveAsTable("meta.ingest_audit")
```

B. Silver cleaning notebook — SQL + Python mixed cells

SQL: Create or refresh temp view to detect rename and normalization
```sql
-- SQL cell: detect candidate rename and create mapping entry
CREATE OR REPLACE TEMP VIEW v_bronze_latest AS
SELECT *,
       CASE WHEN posting_date IS NULL AND post_date IS NOT NULL THEN post_date ELSE posting_date END AS posting_date_mapped
FROM delta.bronze.gl_transactions_raw
WHERE ingest_date >= date_sub(current_date(), 7);
```

Python: canonicalization, mapping, quarantine writes
```python
from pyspark.sql import functions as F
bronze = spark.table("v_bronze_latest")

# synthesize transaction_id if missing or null
bronze = bronze.withColumn("transaction_id",
              F.when(F.col("transaction_id").isNull(),
                     F.sha2(F.concat_ws("||", F.coalesce("source_system", F.lit("unknown")),
                                            F.coalesce("posting_batch_id", F.lit("unknown")),
                                            F.col("raw_row_id").cast("string")), 256))
               .otherwise(F.col("transaction_id")))

# normalize posting_date (cast)
bronze = bronze.withColumn("posting_date", F.to_date("posting_date_mapped"))

# normalize debit_credit to sign convention and produce signed amount
bronze = bronze.withColumn("amount_signed",
                           F.when(F.col("debit_credit").isin("D","d","Debit"), F.abs(F.col("amount")))
                            .when(F.col("debit_credit").isin("C","c","Credit"), -F.abs(F.col("amount")))
                            .otherwise(F.col("amount")))

# Join to coa_master
coa = spark.table("delta.reference.coa_master")
silver = (bronze.join(coa, on="account_code", how="left")
          .withColumnRenamed("account_id", "mapped_account_id"))

# Validation: required fields
valid = silver.filter((F.col("posting_date").isNotNull()) & (F.col("account_code").isNotNull()) & (F.col("mapped_account_id").isNotNull()))
invalid = silver.subtract(valid)

# write valid to silver canonical table with cast for amount to DECIMAL(38,14) if necessary
valid = valid.withColumn("amount", F.col("amount_signed").cast("decimal(38,14)")) \
             .selectExpr("transaction_id","posting_date", "account_code", "mapped_account_id AS account_id",
                         "amount","debit_credit","cost_center","currency","source_system",
                         "posting_batch_id","raw_source_file","raw_row_id","ingest_ts") \
             .withColumn("provenance_job_run_id", F.lit(dbutils.widgets.get("job_run_id")))

(valid.write
    .format("delta")
    .mode("merge")   # implement merge/dedupe strategy via MERGE INTO if required
    .option("mergeSchema","true")
    .saveAsTable("delta.silver.gl_transaction_clean"))

# write invalid rows to quarantine with error codes
invalid_q = invalid.withColumn("error_code", F.lit("VALIDATION_FAILED")) \
                   .withColumn("error_message", F.lit("missing required field or account mapping")) \
                   .withColumn("quarantine_ts", F.current_timestamp())

(invalid_q.select("raw_source_file","raw_row_id","error_code","error_message", F.to_json(F.struct(*invalid_q.columns)).alias("raw_payload"), "ingest_ts","quarantine_ts")
 .write.format("delta").mode("append").saveAsTable("meta.quarantine_gl_transactions"))
```

C. SQL DDL changes (amount precision widen example)
```sql
-- SQL: widen silver/gold amount column (non-destructive)
ALTER TABLE delta.silver.gl_transaction_clean
ALTER COLUMN amount TYPE DECIMAL(38,14);
-- Note: Delta supports column type changes when widening precision; verify with current runtime.
```

D. Suspected rename mapping audit entry (meta.column_mapping write)
```python
spark.createDataFrame([("post_date","posting_date","suspected_rename","auto_mapped","confidence:0.92", F.current_timestamp())],
                     schema=["source_column","canonical_column","reason","status","confidence","detected_ts"]) \
     .write.format("delta").mode("append").saveAsTable("meta.column_mapping")
```

E. Tests (notebook / SQL checks)
- Test 1: posting_date not null
```sql
SELECT COUNT(*) AS failures FROM delta.silver.gl_transaction_clean WHERE posting_date IS NULL;
```
- Test 2: account_code exists in reference table
```sql
SELECT COUNT(*) AS orphan_count FROM delta.silver.gl_transaction_clean s LEFT JOIN delta.reference.coa_master r ON s.account_code = r.account_code WHERE r.account_code IS NULL;
```
- Test 3: amount precision loss detection (compare original vs stored)
```sql
-- store raw amounts and compare after cast; if mismatch, flag
WITH diffs AS (
  SELECT raw_row_id, amount AS raw_amount, CAST(amount AS decimal(38,14)) AS stored_amount
  FROM delta.silver.gl_transaction_clean
)
SELECT COUNT(*) FROM diffs WHERE raw_amount <> stored_amount;
```

F. DLT/DAB Job Spec outline (Databricks Asset Bundle JSON fragment)
- Create a DAB job (or Databricks job) that runs:
  - discovery notebook (profiling + schema_hash)
  - bronze ingestion notebook
  - silver cleaning notebook (depends on bronze)
  - gold marts notebook
- Include parameters: job_run_id, backfill_window, mapping_override.

G. GitHub Actions (CI/CD) outline
- Workflow steps:
  - Checkout repo
  - Run linters/tests for notebooks
  - Run unit tests (PySpark local via pytest)
  - Deploy using databricks-cli or official GitHub action to workspace (import notebooks, register DAB)
  - Trigger integration run on test workspace and run DLT job; assert ingest_audit and tests pass
- Example step names: lint, unit-test, upload-notebooks, register-dab, run-integration.

H. Migration/Backfill Plan (one-time, careful, no-data-loss)
1. Create mapping flag: add mapping for post_date → posting_date in meta.column_mapping, status="auto_applied" (with reviewer notification).
2. Start a controlled backfill job:
   - Read bronze rows where posting_date IS NULL AND post_date IS NOT NULL OR schema_hash indicates older schema.
   - Re-run silver cleaning for those partitions into a staging silver table delta.silver.gl_transaction_clean_staging.
   - Validate staging against tests. If pass, MERGE staging into production silver using business key dedupe (transaction_id or composite key).
3. Materialize gold re-aggregations for affected date ranges (e.g., last N months) using staging or time-travel.
4. Monitor and validate; finalize mapping as "resolved".

---

**Risk Level & Rollback Plan**

Risk level: Medium
- Rationale: Renames and missing required fields cause validation failures (quarantine) and may break downstream reports until mapping/backfill is completed. Datatype widening is lower risk but requires coordinated DDL updates and possible backfills to avoid truncation in gold.

Rollback / Backout strategy (non-destructive, Delta-native)
1. Bronze: no destructive operations — bronze append-only. No rollback needed; reprocess reading historical bronze files.
2. Silver:
   - Use Delta time travel to restore silver to pre-change snapshot if a patch causes unexpected failures:
     - e.g., CREATE TABLE delta.silver.gl_transaction_clean_restore AS SELECT * FROM delta.silver.gl_transaction_clean TIMESTAMP AS OF '<ts_before_change>'
   - Alternatively, MERGE from a tested staging table back into silver to revert specific rows.
3. Gold:
   - Recompute affected aggregates from silver snapshot or re-run ETL for affected window.
4. Contract rollback:
   - Set meta.contract_registry entry to previous semantic version and disable auto-mapping; notify consumers.
5. Emergency steps if something goes wrong:
   - Pause jobs (Databricks Job API cancel)
   - Re-execute silver reprocessing from safe bronze snapshot
   - Recreate backups from delta logs / S3 / ADLS backups
6. Monitoring & Alerts:
   - Ensure ingestion alerts for quarantine spikes, test failures, and schema_hash changes.

---

**Open Questions**

Please provide / confirm the following to finalize exact scripts and automation parameters:
1. Concrete sample source schemas or representative sample files (CSV/JSON/Parquet) to derive exact rename heuristics and precision examples.
2. Is there an authoritative list of source_system values and their owners? (needed for business key composition and mapping)
3. Expected maximum decimal precision/scale for amount across sources (current canonical is DECIMAL(38,10) — do we accept DECIMAL(38,14) as new canonical?)
4. Backfill window tolerance and SLA: How many historical days/months must be reprocessed? What is downtime tolerance for downstream consumers?
5. Ingestion mode for future database source: JDBC high-watermark or CDC? (affects incremental logic and dedupe)
6. Who is the approver for contract MAJOR bumps? (name/team + communication channel)
7. Do we want to implement automatic mapping confirmation UI (e.g., via GitHub PRs or an internal ticketing workflow) for meta.column_mapping items?
8. Where should DAB job specs and workspace IDs be deployed (prod/test workspace GUIDs) and where are secrets stored (Databricks secret scope name)?

---

If you confirm items 1–7 (or provide the missing samples/constraints), I will:
- generate the full set of Databricks notebooks (discovery, bronze, silver, gold) with embedded SQL+Python cells,
- produce the DAB job JSON, Delta DDL migration scripts, and a ready-to-run GitHub Actions workflow file,
- create a small reviewable mapping PR template and the meta.column_mapping auto-resolver notebook for confidence >= 0.9.