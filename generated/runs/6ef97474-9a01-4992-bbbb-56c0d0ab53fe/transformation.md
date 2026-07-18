**Generated Artifacts**
- Notebooks (Python + SQL) for:
  - 01_ingest_bronze.py (bronze ingestion + drift detection + ingest_audit writes)
  - 02_transform_silver.py / SQL (apply mappings, validations, quarantine, synth transaction_id, join COA, write silver via MERGE)
  - 03_build_gold.sql (materialize gl_balance_daily, gl_variance_monthly, gl_exceptions)
  - 99_tests.sql & 99_tests_py.py (SQL checks + PySpark unit checks)
- Delta tables (namespaces):
  - raw.gl_transactions_bronze (append-only delta, partitioned by ingest_date)
  - meta.ingest_audit
  - meta.column_mapping
  - meta.quarantine_gl_transactions
  - refined.gl_transaction_clean (silver)
  - refined.coa_master (reference)
  - curated.gl_balance_daily
  - curated.gl_variance_monthly
  - curated.gl_exceptions
- Databricks Asset Bundle (DAB) manifest (assets.json snippet)
- GitHub Actions workflow: .github/workflows/ci-deploy.yml
- Documentation: model/column descriptions and lineage notes included below
- Performance guidance & run instructions

---

**Transformation Logic (by layer)**

Notes:
- All Delta writes use format="delta".
- Parameterize environment via notebook parameters: bronze_path, ingest_file_path (or jdbc configs), ingest_user, job_run_id, env, max_records_for_preview.
- Rename/auto-mapping policy implemented by reading meta.column_mapping and applying mappings where confidence >= 0.9 and status='candidate' → auto_applied (soft apply).
- All auto-applied events are logged into meta.column_mapping with status and audit fields.

1) Bronze ingestion notebook (01_ingest_bronze.py)

Python notebook (Databricks / PySpark). Purpose: ingest uploaded file or JDBC source to raw.gl_transactions_bronze, detect schema drift, persist ingest audit artifacts.

```python
# 01_ingest_bronze.py (PySpark notebook)
# Parameters (Databricks widgets or job params):
#   source_type: 'file'|'jdbc'
#   source_path: path to uploaded file (dbfs:/mnt/... or s3)
#   jdbc_url, jdbc_table, jdbc_options...
#   bronze_table = "raw.gl_transactions_bronze"
#   ingest_audit_table = "meta.ingest_audit"
#   column_mapping_table = "meta.column_mapping"
#   ingest_date = yyyy-MM-dd (if not provided, use current_date())

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import hashlib
import datetime

# Params from widgets or environment
source_type = dbutils.widgets.get("source_type")  # 'file' or 'jdbc'
source_path = dbutils.widgets.get("source_path")
bronze_table = dbutils.widgets.get("bronze_table", "raw.gl_transactions_bronze")
ingest_audit_table = dbutils.widgets.get("ingest_audit_table", "meta.ingest_audit")
column_mapping_table = dbutils.widgets.get("column_mapping_table", "meta.column_mapping")
ingest_date = dbutils.widgets.get("ingest_date", datetime.date.today().isoformat())
job_run_id = dbutils.widgets.get("job_run_id", dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get())

# Read source
if source_type == "file":
    # support csv/parquet/json based on extension; fallback to auto
    if source_path.endswith(".parquet"):
        df = spark.read.parquet(source_path)
    elif source_path.endswith(".json"):
        df = spark.read.json(source_path)
    else:
        df = spark.read.option("header","true").option("inferSchema","true").csv(source_path)
elif source_type == "jdbc":
    jdbc_url = dbutils.widgets.get("jdbc_url")
    jdbc_table_name = dbutils.widgets.get("jdbc_table")
    jdbc_props = {}  # add username/password from secrets if needed
    df = spark.read.format("jdbc").option("url",jdbc_url).option("dbtable",jdbc_table_name).load()
else:
    raise ValueError("unsupported source_type")

# Add ingest metadata columns (do not drop original columns)
df_ingest = df.withColumn("_ingest_job_run_id", F.lit(job_run_id)) \
              .withColumn("_ingest_ts", F.current_timestamp()) \
              .withColumn("ingest_date", F.lit(ingest_date))

# Write to bronze (append) with schema evolution and partitioning by ingest_date
(spark.table(bronze_table).limit(1)  # ensure table exists else create below...
   if spark._jsparkSession.catalog().tableExists(bronze_table) else None)

# Create path by using Delta table write
(df_ingest.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("append")
    .partitionBy("ingest_date")
    .saveAsTable(bronze_table)
)

# Compute schema_json and hash
schema_json = json.loads(df.schema.json())
schema_json_str = json.dumps(schema_json, sort_keys=True)
schema_hash = hashlib.sha256(schema_json_str.encode("utf-8")).hexdigest()

# Compute column diffs against last seen schema in meta.ingest_audit if exists
from delta.tables import DeltaTable
new_cols = [c["name"] for c in schema_json["fields"]]
last_schema_row = None
if spark._jsparkSession.catalog().tableExists(ingest_audit_table):
    last_schema_row = spark.sql(f"select schema_json, schema_hash, ts from {ingest_audit_table} where table_name = '{bronze_table}' order by ts desc limit 1").toPandas()
    last_cols = []
    if not last_schema_row.empty:
        last_cols = [f['name'] for f in json.loads(last_schema_row.iloc[0]['schema_json'])['fields']]
else:
    last_cols = []

added_cols = list(set(new_cols) - set(last_cols))
removed_cols = list(set(last_cols) - set(new_cols))
col_diff = {"added": added_cols, "removed": removed_cols}

# Persist ingest audit
audit_row = spark.createDataFrame([(
    bronze_table,
    schema_json_str,
    schema_hash,
    json.dumps(col_diff),
    job_run_id,
    F.current_timestamp()
)], schema=StructType([
    StructField("table_name", StringType(), False),
    StructField("schema_json", StringType(), False),
    StructField("schema_hash", StringType(), False),
    StructField("column_diff", StringType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("ts", TimestampType(), True)
])).withColumn("insert_ts", F.current_timestamp())

# Upsert audit row by simple append (audit history)
audit_row.write.format("delta").mode("append").saveAsTable(ingest_audit_table)

# Generate candidate rename suggestions using simple heuristic (levenshtein)
# Compare new column names against a curated list (e.g., silver expected names)
expected_silver_cols = ["transaction_id","posting_date","post_date","account_code","debit_amount","credit_amount","amount","cost_center","raw_row_id","source_system","posting_batch_id"]
mapping_candidates = []
for c in new_cols:
    for expected in expected_silver_cols:
        # use Spark SQL to compute levenshtein distance
        dist = spark.sql(f"select levenshtein('{c}','{expected}') as dist").collect()[0]['dist']
        # heuristics: small distance and length similar → candidate
        if dist <= 2 and abs(len(c)-len(expected)) <= 2 and c != expected:
            mapping_candidates.append((c, expected, 1.0 - dist/max(len(c),len(expected))))

# Persist mapping candidates into meta.column_mapping with low-cost upsert (append, status='candidate')
now = datetime.datetime.utcnow().isoformat()
mapping_rows = []
for src, tgt, score in mapping_candidates:
    mapping_rows.append((bronze_table, src, tgt, float(score), "candidate", now))

if mapping_rows:
    mapping_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("source_column", StringType(), False),
        StructField("target_column", StringType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("status", StringType(), False),  # candidate, auto_applied, reviewed
        StructField("detected_ts", StringType(), False)
    ])
    spark.createDataFrame(mapping_rows, schema=mapping_schema) \
         .write.format("delta").mode("append").saveAsTable(column_mapping_table)

# End of ingest notebook
```

Key points:
- Bronze is append-only, mergeSchema=true.
- ingest_audit persists schema_json, schema_hash, column_diff.
- column mapping candidates written to meta.column_mapping.

2) Silver transformation (02_transform_silver.py)

Goal: create refined.gl_transaction_clean with canonical columns and validations, auto-apply high-confidence renames, quarantine invalid rows, synth transaction_id, normalize signed_amount, join coa_master for account_id mapping. Use MERGE for incremental idempotency.

High-level algorithm:
- Read latest meta.column_mapping rows where confidence>=0.9 & status='candidate' → mark auto_applied and apply renames (log).
- Build a select that aliases columns from bronze to canonical names (e.g., post_date → posting_date).
- Validate posting_date not null; write invalid rows to meta.quarantine_gl_transactions with error_code/error_message and original payload.
- create transaction_id if missing using deterministic hash: sha2(concat_ws('|', coalesce(source_system,''), coalesce(posting_batch_id,''), coalesce(raw_row_id, cast(row_hash as string))),256)
- Normalize debit/credit into signed_amount (policy below).
- Join to refined.coa_master to resolve account_id; rows with missing mapping will be flagged for gl_exceptions (written to curated.gl_exceptions).

Important SQL/PySpark code:

```python
# 02_transform_silver.py (PySpark notebook)
from pyspark.sql import functions as F
from pyspark.sql.types import *
bronze_table = dbutils.widgets.get("bronze_table", "raw.gl_transactions_bronze")
silver_table = dbutils.widgets.get("silver_table", "refined.gl_transaction_clean")
coop_coa_table = dbutils.widgets.get("coa_table", "refined.coa_master")
column_mapping_table = dbutils.widgets.get("column_mapping_table", "meta.column_mapping")
quarantine_table = dbutils.widgets.get("quarantine_table", "meta.quarantine_gl_transactions")
job_run_id = dbutils.widgets.get("job_run_id")

# Load mapping candidates with high confidence to auto-apply
mapping_df = spark.table(column_mapping_table).filter("confidence >= 0.9 and status = 'candidate' and table_name = '{}'".format(bronze_table))
applied_mappings = {row.source_column: row.target_column for row in mapping_df.collect()}

# Mark them as auto_applied in mapping table (audit)
if not mapping_df.rdd.isEmpty():
    now = F.current_timestamp()
    # update mapping rows status to auto_applied
    spark.sql(f"""
        MERGE INTO {column_mapping_table} t
        USING (SELECT table_name, source_column, target_column FROM ( VALUES {','.join(["('%s','%s','%s')"%(r.table_name,r.source_column,r.target_column) for r in mapping_df.collect()])}) as s(table_name, source_column, target_column))
        ON t.table_name = s.table_name AND t.source_column = s.source_column
        WHEN MATCHED THEN UPDATE SET status = 'auto_applied', detected_ts = current_timestamp()
    """)  # if MERGE not supported in this snippet environment, fallback: write appended audit row with auto_applied

# Read bronze
df = spark.table(bronze_table)

# Apply column aliases (create DataFrame with canonical names)
def alias_col(df, src, tgt):
    if src in df.columns:
        return df.withColumnRenamed(src, tgt)
    else:
        return df

# start with df and rename columns per applied_mappings
for s, t in applied_mappings.items():
    if s in df.columns and t not in df.columns:
        df = df.withColumnRenamed(s, t)

# Canonical column names expected:
# transaction_id, posting_date, account_code, debit_amount, credit_amount, amount, cost_center, raw_row_id, source_system, posting_batch_id

# Derive posting_date: if posting_date not present but post_date exists, alias handled above; enforce not null
# Synthesize transaction_id if missing
df2 = df.withColumn("_row_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns]), 256))

df2 = df2.withColumn("transaction_id",
                     F.when(F.col("transaction_id").isNull(),
                            F.expr("sha2(concat_ws('|', coalesce(source_system,''), coalesce(posting_batch_id,''), coalesce(raw_row_id,''), _row_hash),256)"))
                      .otherwise(F.col("transaction_id"))
                    )

# Normalize debit/credit into signed_amount
# Rules:
#  - If 'amount' exists assume already signed if field sign present; else if debit_amount and credit_amount exist: signed_amount = coalesce(debit_amount,0) - coalesce(credit_amount,0)
#  - If debit_credit_flag exists ('D'/'C') and amount exists -> sign accordingly
if 'debit_amount' in df2.columns and 'credit_amount' in df2.columns:
    df2 = df2.withColumn("signed_amount", F.coalesce(F.col("debit_amount"), F.lit(0)) - F.coalesce(F.col("credit_amount"), F.lit(0)))
elif 'amount' in df2.columns and 'debit_credit_flag' in df2.columns:
    df2 = df2.withColumn("signed_amount", F.when(F.col("debit_credit_flag").isin('D','Debit','DR'), F.abs(F.col("amount")))
                                                .when(F.col("debit_credit_flag").isin('C','Credit','CR'), -F.abs(F.col("amount")))
                                                .otherwise(F.col("amount")))
elif 'amount' in df2.columns:
    # assume amount already signed; ensure numeric
    df2 = df2.withColumn("signed_amount", F.col("amount").cast("decimal(38,14)"))
else:
    df2 = df2.withColumn("signed_amount", F.lit(None).cast("decimal(38,14)"))

# Cast signed_amount to wide decimal (policy: DECIMAL(38,14))
df2 = df2.withColumn("signed_amount", F.col("signed_amount").cast("decimal(38,14)"))

# Validate posting_date presence and type
invalid_posting_date = df2.filter(F.col("posting_date").isNull())
valid_df = df2.filter(F.col("posting_date").isNotNull())

# Quarantine invalid posting_date rows (append original payload + error meta)
if invalid_posting_date.count() > 0:
    q = invalid_posting_date.withColumn("error_code", F.lit("MISSING_POSTING_DATE")).withColumn("error_message", F.lit("posting_date is null and post_date mapping failed")) \
        .withColumn("job_run_id", F.lit(job_run_id)).withColumn("quarantine_ts", F.current_timestamp())
    q.write.format("delta").mode("append").saveAsTable(quarantine_table)

# Join to coa_master to resolve account_id
coa = spark.table(coop_coa_table)
joined = valid_df.join(coa.select("account_code", "account_id"), on="account_code", how="left")

# Write exceptions for missing account mapping (to curated.gl_exceptions or meta)
missing_account = joined.filter(F.col("account_id").isNull())
if missing_account.count() > 0:
    missing_account.selectExpr("*", f"'{job_run_id}' as job_run_id", "current_timestamp() as detected_ts") \
        .write.format("delta").mode("append").saveAsTable("curated.gl_exceptions")

# Final silver columns (canonical set)
silver_cols = ["transaction_id", "posting_date", "account_id", "account_code", "cost_center", "signed_amount", "raw_row_id", "source_system", "posting_batch_id", "_ingest_job_run_id", "_ingest_ts", "ingest_date", "_row_hash"]
silver_df = joined.select(*[c for c in silver_cols if c in joined.columns])

# Merge into silver table to support idempotent reprocessing
# Assuming silver_table exists; if not create it first via write.mode("overwrite")
from delta.tables import DeltaTable
if not spark._jsparkSession.catalog().tableExists(silver_table):
    silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    delta_tbl = DeltaTable.forName(spark, silver_table)
    # Merge on transaction_id (business key). Updates latest values based on ingest_ts or overwrite strategy.
    delta_tbl.alias("tgt").merge(
        silver_df.alias("src"),
        "tgt.transaction_id = src.transaction_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll().execute()

# End of silver transformation
```

Notes:
- Rows failing validation are non-destructively quarantined to meta.quarantine_gl_transactions with original payload and error metadata.
- All DECIMAL columns use DECIMAL(38,14) at silver.
- Auto-applied rename actions are recorded in meta.column_mapping with status='auto_applied' and timestamp.

3) Gold marts (03_build_gold.sql)

SQL notebook that builds aggregates and exception tables.

SQL blocks:

```sql
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
```

Notes:
- gl_balance_daily partitioned by year_month for efficient pruning.
- Use small-window incremental re-materialization strategy: when silver gets reprocessed for a posting_date range, re-run gold for only affected year_month partitions.

---

**Tests**

Provide two flavors: SQL assertions run as notebooks and PySpark unit tests.

Tests (SQL) - 99_tests.sql:

```sql
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
```

Pytest style (99_tests_py.py) for CI:

```python
# 99_tests_py.py - run in CI with databricks-connect or locally via spark-submit against test cluster
import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gl_tests").getOrCreate()

def test_posting_date_not_null():
    df = spark.table("refined.gl_transaction_clean").filter("posting_date IS NULL")
    assert df.count() == 0, "Found rows with missing posting_date"

def test_transaction_id_uniqueness():
    df = spark.table("refined.gl_transaction_clean")
    total = df.count()
    distinct = df.select("transaction_id").distinct().count()
    assert total == distinct, f"transaction_id not unique: total {total}, distinct {distinct}"

def test_account_code_exists():
    df = spark.sql("""
        SELECT count(*) AS missing
        FROM refined.gl_transaction_clean t
        LEFT JOIN refined.coa_master c ON t.account_code = c.account_code
        WHERE c.account_code IS NULL
    """)
    assert df.collect()[0]['missing'] == 0, "Missing account_code mapping found"

if __name__ == "__main__":
    import sys
    ret = pytest.main(sys.argv)
    sys.exit(ret)
```

Schema / monitoring tests:
- Freshness/latency monitor: check max(ingest_date) in raw.gl_transactions_bronze and ensure last ingest within threshold (e.g., 24h).
- Schema-diff alert: meta.ingest_audit rows with column_diff.added non-empty → create alert.

---

**Performance Considerations**

- Bronze:
  - Partition by ingest_date (ingest_date) to limit file counts and speed small reads.
  - Keep file sizes 100MB-1GB on cluster (optimize via repartition before write if extremely small).
  - mergeSchema=true used only on bronze writes. Avoid mergeSchema in heavy production writes—use controlled schema evolution process if possible.

- Silver:
  - Use DECIMAL(38,14) for amounts to avoid precision issues.
  - Use Delta MERGE for idempotent upserts on transaction_id. Cluster key: transaction_id is unique (use ZORDER on transaction_id or account_id if heavy lookups).
  - Partition recommendations: do not over-partition by posting_date at very granular level; consider partition by year_month for silver if query patterns mostly by month/day.
  - Keep small-memory operations minimized; pushdown filters early (posting_date range).
  - Cache hot reference tables (refined.coa_master) using spark.catalog.cacheTable in jobs or use broadcast join if small (< 100 MB).
  - Optimize delta tables: run OPTIMIZE ... ZORDER BY (account_id, posting_date) on silver and gold when large.

- Gold:
  - Partition gl_balance_daily by year_month to support fast date-range queries and incremental reprocessing.
  - Materialize monthly aggregates incrementally: when silver reprocesses a date range, only recompute affected month partitions.
  - Use approximate aggregates for exploratory queries (if needed) but ensure accuracy for formal reports.

- Concurrency & Transactionality:
  - Use Delta transactions (MERGE) for atomic writes.
  - Use optimistic concurrency for multiple jobs updating different partitions.

---

**Documentation**

Model: refined.gl_transaction_clean
- purpose: canonical GL transaction rows with validated posting_date, normalized signed_amount, resolved account_id.
- key columns:
  - transaction_id (string): business key; deterministic hash synthesized when missing.
  - posting_date (date/timestamp): required. Source mapped from post_date if present.
  - account_id (string): surrogate from coa_master, null => reported in exceptions.
  - account_code (string): original accounting code.
  - cost_center (string): cost center; rows missing go to exceptions.
  - signed_amount (decimal(38,14)): positive = debit (policy), negative = credit; normalization rules applied.
  - raw_row_id (string): original source row id (if present).
  - source_system, posting_batch_id: pass-through optional metadata.
  - ingest_date, _ingest_ts, _ingest_job_run_id: ingestion metadata.
- lineage:
  - primary source: raw.gl_transactions_bronze
  - mapping audited in meta.column_mapping
  - invalid rows written to meta.quarantine_gl_transactions
  - missing accounts written to curated.gl_exceptions

Contract versioning:
- schema changes are tracked via meta.ingest_audit; additive nullable columns are MINOR version updates; breaking renames flagged as MAJOR and require signoff.

Auditability:
- meta.ingest_audit (schema_json, schema_hash, column_diff)
- meta.column_mapping (mapping suggestions, status, confidence, audit timestamps)
- meta.quarantine_gl_transactions (original payload + error codes)
- All merges/writes preserve original bronze payload.

---

**Run Instructions**

Local / Databricks Job:
1. Commit notebooks to GitHub repo under notebooks/ (01_ingest_bronze.py, 02_transform_silver.py, 03_build_gold.sql).
2. Create Databricks Job with steps:
   - Step A: Run 01_ingest_bronze.py (pass source_type, source_path, ingest_date, job_run_id)
   - Step B: Run 02_transform_silver.py (pass job_run_id)
   - Step C: Run 03_build_gold.sql (SQL task)
3. Provide cluster configuration: runtime (latest ML or Spark LTS), enable autoscaling, ~8-16 workers for medium scale.
4. Schedule Job daily/hourly as required.

CI / GitHub Actions:
- On PR: run unit tests (99_tests_py.py) using a lightweight Spark environment or a Databricks job triggered via API for tests.
- On push to main: deploy notebooks via Databricks CLI / GitHub Actions to workspace and register Job definitions (see example workflow below).

Example GitHub Actions workflow (ci-deploy.yml):

```yaml
name: CI/CD Databricks

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ "*" ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install deps
        run: |
          pip install -r requirements.txt
      - name: Run unit tests
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          # Option A: Trigger test job on Databricks and poll results
          python .github/actions/run_databricks_job.py --job-name "gl-tests"
  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - name: Deploy notebooks to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          # Using databricks-cli or databricks workspace import to sync notebooks
          databricks workspace import_dir notebooks /Repos/<org>/<repo>/notebooks --overwrite
      - name: Register Job definitions
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          python infra/register_databricks_jobs.py
```

Notes:
- Use Databricks Service Principal / token as secrets.
- Provide helper scripts infra/register_databricks_jobs.py and .github/actions/run_databricks_job.py to create/run jobs via Databricks REST API.

DAB (Assets JSON) sample snippet:

```json
{
  "assets": [
    {"path": "notebooks/01_ingest_bronze.py", "type": "notebook", "language": "python"},
    {"path": "notebooks/02_transform_silver.py", "type": "notebook", "language": "python"},
    {"path": "notebooks/03_build_gold.sql", "type": "notebook", "language": "sql"},
    {"path": "tables/refined.gl_transaction_clean", "type": "table"},
    {"path": "tables/curated.gl_balance_daily", "type": "table"}
  ],
  "version": "1.0.0"
}
```

---

**Open Questions / TODOs**
- Source schema details missing:
  - Confirm canonical source column names and types (e.g., raw_row_id type, debit/credit columns, amount column name).
  - Confirm posting_date timezone semantics and desired timezone normalization.
- COA reference table:
  - Provide refined.coa_master schema (account_code -> account_id mapping); is this provided by another system or to be ingested?
- Rename confidence policy:
  - Confirm threshold 0.9 acceptable for auto-apply; adjust human-review workflow and notification channels.
- Deduplication keys:
  - Confirm that transaction_id is the correct primary business key. If not, provide business key composition.
- Retention:
  - Retention policy for bronze (how long to keep raw files?) and for meta tables required.
- Backfill policy:
  - Confirm triggers & windows for reprocessing on schema widening or post_date -> posting_date mapping backfill (one-time backfill requested).
- Error semantics:
  - Confirm how to prioritize rows which both have missing posting_date and missing account mapping (which table should receive them).

---

If you want, I can:
- Generate the exact notebook files (full content) ready to commit.
- Create infra scripts (register_databricks_jobs.py, run_databricks_job.py) and the DAB manifest.
- Scaffold GitHub Actions helper scripts to trigger Databricks jobs and retrieve results.
Which of the above should I generate next?