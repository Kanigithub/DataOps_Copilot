Quality Check Plan
- Goal: automated, measurable gates that block production promotion on critical failures; non-critical checks generate alerts and quarantine/issue tickets.
- Critical (blocking) checks — fail pipeline and write failure audit to meta.ingest_audit and CI:
  1. Schema presence: all canonical columns exist after auto-rename (transaction_id OR fields to synthesize it, posting_date, account_code, debit_amount/credit_amount/amount, cost_center). SQL check example:
     ```sql
     -- Critical: required columns present in refined candidate dataframe
     WITH cols AS (
       SELECT explode(schema_of_json(schema_json)) AS c FROM meta.ingest_audit WHERE table_name='raw.gl_transactions_bronze' ORDER BY ts DESC LIMIT 1
     )
     SELECT c FROM cols WHERE c NOT IN ('transaction_id','posting_date','account_code','debit_amount','credit_amount','amount','cost_center','raw_row_id');
     ```
     If any rows returned → fail.
  2. Posting date validity: posting_date NOT NULL and between (today - 7 years) and (today + 31 days). Block when > 0% of required rows have invalid posting_date. Implemented as PySpark assertion (blocking if invalid_count > 0):
     ```python
     invalid_count = df.filter(col('posting_date').isNull() | (col('posting_date') < date_sub(current_date(), 365*7)) | (col('posting_date') > date_add(current_date(), 31))).count()
     assert invalid_count == 0, f"{invalid_count} rows with invalid posting_date"
     ```
  3. Signed_amount normalization: values must be non-null and numeric after normalization; block if > 0 rows fail numeric parse.
  4. Deduplication uniqueness: transaction_id uniqueness within the ingest window: block if duplicate rate > 0.1% (configurable). Example check:
     ```sql
     SELECT COUNT(*) AS total, COUNT(DISTINCT transaction_id) AS unique_ids FROM refined.gl_transaction_clean WHERE ingest_job_run_id = '$JOB_RUN'
     ```
     block if (total - unique_ids)/total > 0.001.
  5. Schema drift unhandled: new column detected without mapping and auto-apply confidence < 0.9 → block ingestion to Silver until mapping reviewed. Record to meta.column_mapping.
  6. COA mapping coverage: after join to coa_master, missing_coa_rate > configured threshold (default 0.5%) → block promotion to production reporting marts.

- Non-critical (alert/quarantine) checks — notify and quarantine rows, but continue processing:
  1. Minor posting_date skew: small number of out-of-range dates (<= threshold).
  2. Percent quarantined rows < 5%: send warning.
  3. Amount precision widening detected (scale change) — do not block but record a schema drift event and run numeric sanity tests.
  4. Volume anomalies (ingest volume deviates > 3 StdDev or by configured percentage) → alert for operator review.
  5. Column rename candidates with confidence between 0.5 and 0.9 → create tickets and require manual review.
  6. Text fields length growth beyond historical median + 5 StdDev → log for downstream schema/size planning.

- Drift detection & anomaly hooks:
  - Compute and persist schema_json and schema_hash per ingest to meta.ingest_audit (already in spec). Diff against last schema; if diff contains additions/removals/renames -> create meta.drift_events row with type (ADD/DROP/RENAME/DATATYPE_WIDEN).
  - For numeric distribution drift: maintain baseline histograms (approx_count_distinct, quantiles) in meta.data_metrics. Trigger anomaly detection when KS-test p-value < 0.01 or median shift > configured threshold.
  - Use rolling windows (7d baseline vs. last run) for distribution shifts.

Governance Plan
- Classification and tagging:
  - Tag datasets in Unity Catalog / Hive metastore with classification: raw.gl_transactions_bronze (classification=unclassified/raw), refined.gl_transaction_clean (classification=financial), curated.* (classification=financial_reporting).
  - Column-level: inspect and tag potential PII (employee_id, vendor_name, payer_name, payee_name). If not present by default, include automated PII scan job on bronze ingest to detect likely PII (regex + ML).
- PII handling & masking/tokenization:
  - If PII detected, apply per-column policy in Silver:
    - Identifiers (employee_id, vendor_id): deterministic HMAC-SHA256 with per-environment secret (Databricks Secret Scope). Example PySpark:
      ```python
      from pyspark.sql.functions import sha2, concat_ws, lit
      df = df.withColumn('vendor_id_hash', sha2(concat_ws('|', col('vendor_id'), lit(secret_salt)), 256))
      ```
    - Names / free-text: redact or hashed with salt; if needed for joins, use tokenization service or FPE.
    - Store mapping table meta.pii_token_map encrypted and access-controlled.
  - Never store secrets in code; reference secret scope.
- Access controls & least privilege:
  - Apply Unity Catalog grants:
    - raw namespace: DATA_READ for ingestion service account, DATA_OWNER for pipeline service; no broad user access.
    - meta namespace: read/write for pipeline engineers; read for auditors.
    - refined/curated: provide role-based SELECT to analytics roles; enable column-level masking for PII columns via Unity Catalog policies.
  - Use Unity Catalog lineage and audit logging; restrict alter/drop to infra admins.
- Retention & deletion:
  - Bronze raw.gl_transactions_bronze: retain full raw data for audit for 365 days compressed; then move to cold storage (archival delta table or object storage tier) for up to 7 years per finance retention policy. Provide an archival job.
  - Silver refined.gl_transaction_clean: retained 7 years; yearly purge or archive to immutable storage after 7 years.
  - Curated gold tables: keep 7+ years, snapshots required for audit trail.
  - Quarantine/meta tables: retain 3 years.
  - Implement retention policy as scheduled jobs that produce audit records and secure deletion logs in meta.retention_audit.
- Audit requirements:
  - Persist lineage: when writing any refined/curated table, write a lineage row to meta.lineage with table_name, input_table_versions (Delta version), transformation_job_run_id, timestamp, notebook_git_commit.
  - Keep column_mapping, ingest_audit, drift_events, quarantine, pii_token_map, retention_audit under meta namespace with restricted access.
  - All promotion operations require an immutable audit row with operator/service account and git_commit.

Observability & Alerting
- Pipeline & job metrics to capture (store in meta.metrics or forward to your monitoring system):
  - Job-level: run_id, job_name, start_ts, end_ts, duration_ms, status, executor_count.
  - Ingest metrics: rows_in, rows_out, files_processed, input_size_bytes, partition_count, schema_hash.
  - Data metrics: percent_quarantined, percent_missing_coa, duplicate_rate, avg_signed_amount, total_amount, min/max amounts, quantiles.
  - Drift metrics: number_of_new_columns, datatypes_changed, rename_candidates_count.
  - SLA metrics: freshness_lag (seconds since last successful run), last_success_ts.
- Recommended alert thresholds and routing:
  - Critical alerts (Pager/OnCall + Email + Slack #data-pager):
    - Posting_date validity failure (critical).
    - Schema drift unhandled (new required column dropped) or auto-apply confidence > threshold change unexpectedly.
    - COA missing rate > 0.5% (configurable).
    - Pipeline failure or run duration > 3x baseline median.
    - Duplicate rate > 0.1%.
  - Warning alerts (Slack #data-alerts + Email):
    - Percent_quarantined > 1% and <= 5%.
    - Volume deviation > 50% or < 50%.
    - Amount distribution drift p-value < 0.01 but coverage < critical.
  - Info (dashboard only):
    - Schema additions with auto-apply; minor datatype widenings.
- Example alert definitions (pseudocode):
  - Alert: COA Missing Rate
    - Condition: percent_missing_coa > 0.005
    - Severity: critical
    - Pager: oncall-finance-data
    - Runbook summary:
      1. Check meta.ingest_audit for job_run_id and schema.
      2. Inspect sample rows: SELECT * FROM meta.quarantine_gl_transactions WHERE job_run_id='$JOB'
      3. If COA change expected (new account codes), update coa_master; otherwise investigate source data.
      4. Re-run 02_transform_silver with --reprocess_job_run_id, then re-run gold.
- Runbook snippets (concise):
  - Failure due to missing required column:
    1. Fetch meta.column_mapping generated candidates for job_run_id.
    2. If auto-applied status is present, inspect mapping; else open ticket and mark mapping.review.
    3. If mapping needs manual apply, update meta.column_mapping status='approved' then re-run silver notebook.
  - High quarantine rate:
    1. Query meta.quarantine_gl_transactions for top error_code counts.
    2. If single widespread error, rollback ingestion and notify producer.
    3. Fix transform logic or producer data and reprocess.

Trust Report Template
- Purpose: single-page dataset scorecard for refined.gl_transaction_clean and curated outputs.
- Fields (one row per dataset):
  - dataset_name
  - as_of_ts
  - job_run_id / git_commit
  - freshness_lag (mins)
  - rows_in / rows_out
  - percent_quarantined
  - percent_missing_coa
  - duplicate_rate
  - schema_version (schema_hash)
  - drift_events_count (last 7 days)
  - PII_detected (Y/N)
  - access_policy_status (OK/RequiresReview)
  - retention_policy_status (OK/Overdue)
  - overall_trust_score (0-100) computed:
    - formula (example): 100 - (w1*percent_quarantined*100 + w2*percent_missing_coa*100 + w3*duplicate_rate*100 + w4*freshness_penalty)
    - default weights: w1=0.4,w2=0.3,w3=0.2,w4=0.1; freshness_penalty = min(1, freshness_lag / freshness_SLA_in_mins)
- Release readiness: dataset is "Ready" if:
  - overall_trust_score >= 90
  - no critical checks failed in last run
  - percent_missing_coa <= configured threshold
  - schema changes either auto_applied or approved in meta.column_mapping.

Integrations & Implementation Hooks (vendor-neutral)
- Where to plug checks:
  1. Bronze ingestion notebook (01_ingest_bronze.py):
     - After write to raw table, compute schema_json/schema_hash and write to meta.ingest_audit.
     - Run schema presence check and basic volume sanity; produce metrics to meta.metrics.
  2. Silver transform notebook (02_transform_silver.py):
     - Apply auto-applied renames from meta.column_mapping.
     - Run validation checks (posting_date, signed_amount parsing, dedupe). Use PyTest + SQL unit tests (99_tests.py, 99_tests.sql).
     - Write quarantined rows to meta.quarantine_gl_transactions with error_code.
     - On pass, MERGE into refined.gl_transaction_clean and write lineage record.
  3. Gold SQL notebook (03_build_gold.sql):
     - Define materializations for gl_balance_daily, gl_variance_monthly, gl_exceptions with MERGE/REPLACE as appropriate.
     - Assert input trust score >= threshold before materialize.
- CI/CD:
  - GitHub Actions workflow:
    - Lint notebooks, run unit tests against a small dev Delta snapshot, run quality checks script (99_tests_py.py), build Databricks Asset Bundle (DAB) and run a dry-run deploy to dev workspace.
    - Require PR approval for column_mapping changes with confidence < 0.9.
  - Databricks Asset Bundle (manifest):
    - Include notebooks, SQL, tests, and meta tables definitions.
    - On deploy to prod, run gate job that executes the Quality Check Plan; if failure, abort deployment.
- Example code snippets:
  - Compute schema_hash in PySpark:
    ```python
    import hashlib, json
    schema_json = json.dumps(df.schema.jsonValue(), sort_keys=True)
    schema_hash = hashlib.sha256(schema_json.encode('utf-8')).hexdigest()
    spark.createDataFrame([(table_name, schema_json, schema_hash, job_run_id, current_timestamp())], ...) \
         .write.mode('append').saveAsTable('meta.ingest_audit')
    ```
  - MERGE into refined (SQL):
    ```sql
    MERGE INTO refined.gl_transaction_clean tgt
    USING (SELECT * FROM staging.gl_transaction_normalized WHERE job_run_id = '$JOB_RUN') src
    ON tgt.transaction_id = src.transaction_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    ```

Release Gates
- Preconditions for promotion to production reporting marts (automatic checks performed in CI/CD):
  1. All unit tests (99_tests.sql, 99_tests_py.py) pass.
  2. No critical checks failed in last production run.
  3. Overall_trust_score >= 90 for refined.gl_transaction_clean and source tables.
  4. COA missing rate <= configured SLA (default 0.5%).
  5. No unresolved schema drift events with status='unreviewed' for required columns.
  6. Column mapping changes with confidence < 0.9 require manual approval in PR.
  7. Lineage recorded for the candidate release with notebook git_commit and job_run_id in meta.lineage.
  8. DAB manifest successfully builds and deployment job completes in dev integration environment.
- Blockers:
  - Any failing critical check or missing provenance/lineage record blocks promotion.
  - Manual approval required for any PII unmasking or retention change.
- Post-promotion checklist:
  - Verify gold tables refreshed and run trust-report; publish to #data-ops.
  - Archive previous snapshot and log retention event.

Open Questions
1. SLAs: What are the target freshness and latency SLAs for gold reports (e.g., near-real-time, hourly, daily)? Default thresholds we used are daily materialization with freshness SLA = 24 hours.
2. Retention policy confirmation: Is there a legal requirement (e.g., 7 years) or internal policy that overrides the suggested retention windows?
3. PII presence: Do source files include explicit PII fields (employee/vendor names, IDs)? If yes, list columns requiring deterministic joins vs full redact.
4. COA master ops: Who owns coa_master updates? Should pipeline auto-create candidate COA entries or always require manual enrichment?
5. Alert routing: Provide on-call rotation/contact for critical Pager alerts and Slack channels to post to.
6. Threshold tuning: Confirm acceptable thresholds for missing_coa_rate, duplicate_rate, quarantine_rate for your business.
7. Environment secrets: where should tokenization salts and HMAC secrets be stored (Databricks Secret Scope name)?
8. Drift policy: Approve auto-apply confidence threshold (default 0.9) or change?

Summary (actionable next steps)
- Implement audit/diff write to meta.ingest_audit in 01_ingest_bronze.py and fire drift events.
- Implement meta.column_mapping write and auto-apply logic; block auto-apply <0.9.
- Add quality assertions in 02_transform_silver.py per critical checks; write quarantines and metrics.
- Add pre-materialization trust score check in 03_build_gold.sql and CI workflow.
- Configure Unity Catalog grants, retention jobs, and secret scope for PII hashing.
- Create dashboards for metrics and set up alert channels/runbooks above.

If you want, I can:
- Generate the concrete notebook cells (01/02/03) including the exact PySpark + SQL code for each check and MERGE statements, and
- Produce the GitHub Actions workflow + Databricks Asset Bundle manifest with the outlined gates.