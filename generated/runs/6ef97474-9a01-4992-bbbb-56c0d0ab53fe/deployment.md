Deployment Plan
- Goal: Deploy repeatable Databricks Delta ELT for GL transactions with Bronze (append-only raw Delta, schema evolution), Silver (normalization/validation/sign conventions, gl_transaction_clean), and Gold (reporting marts gl_balance_daily, gl_variance_monthly, gl_exceptions). Include audit/lineage via meta.* tables, drift detection, and masking.
- Prerequisites
  - Databricks workspace(s) with Delta Lake; recommended Unity Catalog enabled (if not available, use dedicated schemas + view-based RBAC).
  - Git repo with:
    - /notebooks/bronze_ingest.py (or .ipynb)
    - /notebooks/silver_transform.py (or .ipynb)
    - /notebooks/gold_reporting.py (or .ipynb)
    - /sql/asserts/*.sql (blocking/non-blocking assertions)
    - /tests/* (unit/pytests; schema & logic)
    - databricks/asset-bundle.yaml (DAB manifest)
    - ci/github-actions.yml
    - /configs/{dev,test,prod}.yaml
    - /mappings/column_mapping.yaml (candidate/applied mapping rules)
  - Service principal / job user and secret scope (no secrets in repo). CI runner must have access to a secret providing DATABRICKS_HOST + DATABRICKS_TOKEN or use OIDC.
  - Access & grants policy: Bronze restricted, Silver limited analysts, Gold read-only business roles. Implement via Unity Catalog or DB-level grants and view-based masking.
- Environments & promotion flow
  - dev: automatic deploy on merge to develop branch; runs full pipeline on sample partition(s).
  - test: automatic deploy on merge to test branch (or tag) after dev success; runs full pipeline on full test dataset + integration checks.
  - prod: deploy only via GitHub Release / tag or manual promotion with an approval gate. Production deploy runs final full pipeline and publishes trust_report artifact.
- Promotion rules
  - All unit tests & static checks pass -> dev -> integration tests -> test
  - All critical checks must be PASS in test environment to enable prod approval workflow
  - Prod deployment requires manual approval (GitHub Environment approval) and successful upstream checks
- Artifacts produced & stored
  - Delta tables: <catalog>.<schema>.<table> for bronze/silver/gold
  - meta.* audit tables
  - trust_report JSON per ingest (persisted as a file or table link)
  - DAB bundle + Databricks Job definitions + notebooks published to workspace
  - Git tag/Release pointing to deployed artifact version and run_id references

CI/CD Configuration
- Pattern: GitHub Actions pipeline implementing Build → Lint → Unit Tests → Package (DAB) → Deploy(dev) → Run(dev jobs & checks) → Promote(test) → Run(test jobs & checks) → Manual approval → Deploy(prod).
- Key features:
  - Idempotent deploy using Databricks Asset Bundle (DAB) + job definitions that are deterministic (not recreated unless changed).
  - Separation of build/test and deploy steps; deployments use job run arguments (ingest_run_id, snapshot_ts).
  - Prod gate using GitHub Environments with required approvers.
  - No secrets in repo; use GitHub Environments secrets OR OIDC federated token to obtain Databricks token from secret store.
- Example GitHub Actions (core jobs only, trimmed)
name: CI-CD Databricks ELT
on:
  push:
    branches: [ develop, test, main ]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Python setup
        uses: actions/setup-python@v4
        with: {python-version: '3.9'}
      - run: pip install -r requirements-dev.txt && flake8
  unit_tests:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements-test.txt
      - run: pytest tests/unit -q
  build_dab:
    runs-on: ubuntu-latest
    needs: unit_tests
    steps:
      - uses: actions/checkout@v4
      - name: Install databricks CLI/SDK
        run: pip install databricks-cli databricks-sdk
      - name: Build Asset Bundle
        run: databricks-assets build --manifest databricks/asset-bundle.yaml --output artifacts/asset-bundle.zip
      - uses: actions/upload-artifact@v4
        with: name: dab-bundle; path: artifacts/asset-bundle.zip
  deploy_dev:
    runs-on: ubuntu-latest
    needs: build_dab
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with: name: dab-bundle
      - name: Deploy to Databricks (dev)
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_DEV }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        run: databricks-assets deploy artifacts/asset-bundle.zip --workspace-path /Repos/org/gl_elt/dev --overwrite
  run_dev_jobs:
    needs: deploy_dev
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Dev Job Runs & Wait
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_DEV }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_DEV }}
        run: |
          python ci/trigger_jobs.py --env dev --jobs bronze,silver,gold --wait
  promote_to_test:
    if: needs.run_dev_jobs.result == 'success'
    needs: run_dev_jobs
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to TEST (same steps as deploy_dev but pointing to test secrets)
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_TEST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_TEST }}
        run: databricks-assets deploy artifacts/asset-bundle.zip --workspace-path /Repos/org/gl_elt/test --overwrite
  prod_deploy:
    needs: promote_to_test
    runs-on: ubuntu-latest
    environment: production
    steps:
      - name: Wait for manual approval
        uses: chrnorm/deployment-approval@v1
      - uses: actions/download-artifact@v4
        with: name: dab-bundle
      - name: Deploy to PROD
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
        run: databricks-assets deploy artifacts/asset-bundle.zip --workspace-path /Repos/org/gl_elt/prod --overwrite
Notes:
- trigger_jobs.py should start databricks jobs (job_id from DAB) with params {ingest_run_id, run_mode=full|sample, git_commit} and poll for completion, persist run_id in artifacts.
- Use GitHub Environments secrets for env-specific tokens and require approvers on production environment.

IaC Requirements
- Use Terraform with databricks provider (or native cloud + databricks provider). Example resources to manage:
  - databricks_permissions/databricks_group & databricks_service_principal (or cloud SP)
  - databricks_secret_scope (backed by cloud secret store if supported)
  - workspace folders and ACLs (notebooks/repos)
  - Unity Catalog objects: metastore, catalog, schemas/catalog_permissions
  - Delta table locations (S3/GCS/ADLS) and lifecycle policies
  - databricks_job definitions (optional: keep jobs in GitOps via DAB)
- Sample Terraform snippet (Databricks provider pseudo):
resource "databricks_group" "platform_engineers" {
  display_name = "platform-engineers"
}
resource "databricks_secret_scope" "ci" {
  name = "gl-elT-scope"
  initial_manage_principal = "users"
  # Backend: link to cloud-managed secrets
}
resource "databricks_metastore" "uc" {
  # configure only if managed; often created by platform
}
resource "databricks_catalog" "gl_catalog" {
  name = "gl_catalog"
  comment = "GL datasets (bronze/silver/gold)"
  provider = "unity_catalog"
}
- Storage: create dedicated Delta locations per layer:
  - s3://<bucket>/gl/bronze/, .../silver/, .../gold/ or abfss://... if ADLS.
- Jobs & compute:
  - Create job clusters or instance pools for Bronze (short-lived small), Silver (medium), Gold (compute heavy / long running).
  - Define minimum permissions to allow job run.

IaC snippet for Delta database aliases (SQL):
CREATE SCHEMA IF NOT EXISTS gl_catalog.bronze COMMENT 'raw ingest';
CREATE SCHEMA IF NOT EXISTS gl_catalog.silver COMMENT 'normalized';
CREATE SCHEMA IF NOT EXISTS gl_catalog.gold COMMENT 'reporting';

Environment Config Templates
- configs/{dev,test,prod}.yaml (parameterized)
dataset:
  name: gl_transactions
  catalog: gl_catalog
  schemas:
    bronze: bronze
    silver: silver
    gold: gold
storage:
  bronze_location: ${storage_base}/gl/bronze
  silver_location: ${storage_base}/gl/silver
  gold_location: ${storage_base}/gl/gold
thresholds:
  MAPPING_CONFIDENCE_AUTO_APPLY: 0.90
  QUARANTINE_CRITICAL_RATE: 0.005
  DUPLICATE_TRANSACTION_ID_CRITICAL_RATE: 0.0001
  VOLUME_DAY_OVER_DAY_ALERT_PCT: 0.30
retention_days:
  bronze: 365
  silver: 3650
  gold: 2555
jobs:
  bronze_job_id: JOB_BRONZE_ID_PLACEHOLDER
  silver_job_id: JOB_SILVER_ID_PLACEHOLDER
  gold_job_id: JOB_GOLD_ID_PLACEHOLDER
secrets:
  secret_scope: gl-elt-scope
  key_salt_name: SALT_KEY_NAME
mapping:
  auto_apply_threshold: ${thresholds.MAPPING_CONFIDENCE_AUTO_APPLY}
  mapping_confidence_source: /mappings/column_mapping.yaml
- column_mapping.yaml example (candidate rules):
- column: post_date
  mapped_column: posting_date
  transform: to_date
  confidence: 0.98
  status: candidate
- column: amount
  mapped_column: amount
  transform: decimal(38,14)
  confidence: 1.0
  status: applied
- Secrets: never stored in repo. Reference via secret scope names; CI must inject runtime tokens from secrets.

Notebooks / SQL & Tests (examples)
- Bronze notebook (Python fenced snippet):
```python
# bronze_ingest.py (Notebook cell)
from pyspark.sql import functions as F
from delta.tables import DeltaTable
# read uploaded files (mount or ADLS/S3 path passed as param)
src = spark.read.option("multiline", "false").json(dbutils.widgets.get("input_path"))
# ensure _raw_row_id exists
src = src.withColumn("_raw_row_id", F.coalesce(F.expr("_raw_row_id"), F.sha2(F.to_json(F.struct(*src.columns)), 256)))
# append-only write with schema evolution
bronze_path = dbutils.widgets.get("bronze_path")
src.write.format("delta").mode("append").option("mergeSchema","true").save(bronze_path)
# register ingest audit
schema_json = spark.read.format("delta").load(bronze_path).schema.json()
schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()
spark.createDataFrame([(ingest_run_id, schema_json, schema_hash, source, columns)], schema=...).write...
```
- Silver notebook (mix Python + SQL): normalize account mapping, posting_date validation, sign normalization, MERGE by transaction_id into silver table.
```sql
-- SQL assert example (blocking)
-- Ensure posting_date completeness
SELECT
  SUM(CASE WHEN posting_date IS NULL THEN 1 ELSE 0 END) AS null_count,
  COUNT(1) AS total_count
FROM {bronze_db}.{bronze_table}
```
```python
# Silver transform (extract)
bronze = spark.table(f"{cfg['catalog']}.{cfg['schemas']['bronze']}.{dataset}_raw")
# Apply column mappings (use meta.column_mapping to transform renamed fields)
# Example sign normalization
from pyspark.sql.functions import when, col
silver_df = bronze.withColumn("amount_signed",
    when(col("debit_credit_flag") == "D", col("amount"))
    .when(col("debit_credit_flag") == "C", -col("amount"))
    .otherwise(col("amount")))
# Ensure DECIMAL(38,14) type
silver_df = silver_df.withColumn("amount_signed", col("amount_signed").cast("decimal(38,14)"))
# MERGE idempotent upsert
target = f"{cfg['catalog']}.{cfg['schemas']['silver']}.gl_transaction_clean"
DeltaTable.forPath(spark, silver_path).alias("t").merge(
    silver_df.alias("s"),
    "t.transaction_id = s.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```
- Gold reporting (SQL):
```sql
-- gl_balance_daily
CREATE OR REPLACE VIEW {catalog}.{schema}.gl_balance_daily AS
SELECT account_id, cost_center, date(posting_date) AS day,
       SUM(amount_signed) AS balance
FROM {catalog}.{schema_silver}.gl_transaction_clean
GROUP BY account_id, cost_center, date(posting_date);
```
- Tests
  - Unit tests for mapping functions (pytest).
  - End-to-end integration assertions executed as SQL/Notebook tasks immediately after each layer:
    - Critical checks write to meta.quality_checks and block promotion if failed.
    - Non-blocking checks produce alerts and record details.
  - Example pytest pseudo:
    - validate sign normalization idempotency by running transform twice and asserting identical row_counts and no duplicates.

Databricks Asset Bundle (DAB)
- Build includes: notebooks, SQL assets, job definitions referencing notebooks, libraries.
- Example databricks/asset-bundle.yaml (conceptual):
assets:
  notebooks:
    - path: notebooks/bronze_ingest.py
  jobs:
    - name: gl_bronze_ingest
      tasks:
        - notebook_path: /Repos/org/gl_elt/dev/notebooks/bronze_ingest.py
          timeout_seconds: 7200
- Use databricks-assets CLI (or dbx) to build and deploy bundle.

Rollback Strategy
- Versioning:
  - All pipeline code and config are versioned in Git. Use Git tags/releases for production promotions.
  - Every Databricks job run records run_id + git_commit in meta.transform_audit.
- Data Rollback:
  - Prefer Delta time travel: to roll back table to previous state:
    - Identify stable version: SELECT max(version) FROM (DESCRIBE HISTORY delta.`<path>`) WHERE commit_ts < <problem_ts>;
    - RESTORE: spark.sql("RESTORE TABLE <table> TO VERSION AS OF <version>") or create CLONE from previous version.
  - For Gold reporting marts, consider atomic swap pattern:
    - Write to a staging table gl_balance_daily_vYYYYMMDD, validate, then ALTER TABLE RENAME to swap; or maintain view layered on an alias that points to the active table.
- Code Rollback:
  - Revert Git commit or checkout tag and rebuild DAB, then redeploy via CI.
- Safe redeploy steps:
  1. Pause downstream scheduled jobs (if needed) via job pause API.
  2. Revert code to known-good tag.
  3. Deploy DAB to target env.
  4. If data rollback required use Delta time-travel or table CLONE -> run reconciliations.
  5. Run smoke checks; if good, resume schedules.
- Rollback for schema migrations:
  - Avoid destructive schema changes in Silver/Gold. For breaking renames/width changes, create compatibility views that expose previous schema while rolling forward transformation; revert by redeploying previous transform and restoring data via time travel.
- Retain metadata of rollback in meta.trust_report & meta.transform_audit.

Observability & Runbook
- Metrics to collect (persist to meta.metrics):
  - job_run_status, job_duration, row_counts_by_layer, schema_hash events, null rates, duplicate ratios, quarantine counts, drift_scores.
- Alerts (example channels):
  - PagerDuty/OnCall (critical): job failures, blocking check failures, quarantine_rate > QUARANTINE_CRITICAL_RATE or >10k rows, duplicate_transaction rate breach, schema-change without mapping applied.
  - Slack/email (warning/info): drift alerts, low-confidence mapping, volume change warnings.
- Drift & anomaly pipeline:
  - Persist test results to meta.drift_metrics; alert on KS p < 0.001 or mean change >50%, categorical share change >20%, z-score beyond thresholds.
- Runbook (short)
  - On critical check failure after Silver:
    1. Inspect meta.quality_checks for detailed failure (check_id, details).
    2. Query meta.quarantine_gl_transactions for rows quarantined and error reasons.
    3. If mapping issue: open mapping candidate in /mappings and review; if confidence low, set remediation to manual mapping and re-process quarantined rows.
    4. If duplicate_transaction_id breach: run dedupe queries on Bronze to identify source; investigate upstream system.
    5. After fix: re-run only impacted job with parameter ingest_run_id pointing to quarantined run.
  - On job failure:
    1. Check job run logs in Databricks UI and meta.transform_audit for run_id and git_commit.
    2. If transient cluster issue, retry via job rerun. If code bug, revert to previous tag/commit and redeploy.
  - On schema drift alert:
    1. Inspect meta.column_mapping for candidate mappings and mapping.confidence.
    2. If confidence >= MAPPING_CONFIDENCE_AUTO_APPLY, confirm mapping auto-applied and re-run Silver transforms.
    3. If not, review candidate mapping, approve in UI or via YAML, then re-run mapping-only job to apply transformations and reprocess.
  - On data quality regressions in Gold:
    1. Compare gl_balance_daily to gl_transaction_clean aggregates; check meta.metrics for reconciliation numbers.
    2. If discrepancy > tolerance, run differential query using time travel to pinpoint first bad run; roll back or reprocess as required.
- Observability hooks
  - Add a task at end of each job to:
    - write meta.transform_audit
    - push metrics to monitoring (Datadog / Prometheus / CloudWatch) via HTTP or integrated connector
    - send trust_report artifact to artifact store (S3/DBFS) and notify Slack with permalink
- Sample key commands for ops:
  - Inspect recent quality checks:
    SELECT * FROM meta.quality_checks WHERE dataset='gl_transactions' ORDER BY recorded_ts DESC LIMIT 50;
  - Find quarantined rows:
    SELECT * FROM meta.quarantine_gl_transactions WHERE ingest_run_id = '<id>' LIMIT 100;

How to Run (minimal manual steps for engineers)
1. Prepare secrets in secret store / GitHub Environment (DATABRICKS_HOST/DATABRICKS_TOKEN per environment; secret scope salt).
2. Push code to develop branch (or open PR). CI runs lint/tests and builds DAB.
3. Merge to develop -> automatic deploy to dev, run jobs & checks. Inspect trust_report and fix issues.
4. Merge to test branch/tag -> automatic deploy to test; run integration tests.
5. Create GitHub Release or click Approve on Production environment to deploy to prod; CI will deploy and run prod jobs.
6. For ad-hoc reprocessing: run the databricks job with param ingest_run_id or reprocess_quarantine job to handle quarantined rows only.

Open Questions (need up to 3 to finalize IaC & secrets)
1. Cloud provider and workspace details: which cloud (AWS / Azure / GCP) and will Unity Catalog be available? (affects Terraform & secret scope recommendations)
2. Source connection for future DB: is it a JDBC-capable database (Snowflake, Oracle, Postgres) or another landing zone? Provide connection details or pattern (JDBC/CDC) to include connector configs in IaC.
3. Retention & required SLA: confirm exact retention durations per layer (if different from defaults) and max acceptable job latency / SLA for daily GL jobs to size compute pools.

Appendices (concise implementation-ready notes)
- Quality checks implementation pattern:
  - Implement reusable Python module checks/qc.py with functions that return structured dicts written to meta.quality_checks. Block promotion if any critical check returns status!=PASS.
- Mapping auto-apply logic:
  - Implement candidate generation module: on schema change create candidate in meta.column_mapping with a computed confidence score. Auto-apply only when confidence >= env.MAPPING_CONFIDENCE_AUTO_APPLY; otherwise leave candidate and emit Slack alert & create issue.
- Masking & PII:
  - Bronze: persist raw values but tag meta.column_classification.
  - Silver/Gold: replace PII columns with deterministic salted hash using secret in secret scope; record in meta.masking_audit.
- Tests & assertions execution:
  - Each Job has final "assert" task to run /sql/asserts/*.sql and a lightweight notebook that writes meta.quality_checks. CI runner must fail pipeline on any blocking check.
- Deliverables to produce in repo:
  - notebooks (bronze/silver/gold), reusable libs (checks, mappings, drift), SQL asserts, DAB manifest, GitHub Actions workflow, Terraform snippets, environment config templates, runbook markdown.

If you confirm cloud provider (Q1), whether Unity Catalog is available (Q1a), and the type of future DB source (Q2), I will:
- produce concrete Terraform code for workspace, catalog & storage,
- generate a ready-to-run databricks/asset-bundle.yaml with job definitions and parameterized notebook headers,
- emit the full GitHub Actions YAML with secrets mapping and the trigger_jobs.py utility script.