# End-to-End Data Pipeline: Customer Alert Consolidation & Tiered Targeting

An orchestrated, multi-engine data pipeline demonstrating rule-based alert consolidation and tiered customer targeting — built with dummy data to practice patterns used in production pharma commercial analytics pipelines (business-rules-based alerting, tiered target lists, SCD Type 2 change tracking), and now fully automated end-to-end with Airflow.

This is the capstone of a four-week hands-on build spanning the core modern data engineering stack: **dbt (transformation/testing) → Spark (distributed processing) → Databricks/Delta Lake (managed lakehouse) → Airflow (orchestration)**.

## Architecture
┌─────────────────────────┐
                      │   Airflow (Docker)       │
                      │   DAG: full_customer_    │
                      │   alerts_pipeline        │
                      │   Schedule: @daily       │
                      └───────────┬──────────────┘
                                  │
             ┌────────────────────┼────────────────────┐
             ▼                    ▼                     ▼
  validate_raw_ingestion   run_spark_transform    run_dbt_run → run_dbt_test
  (checks seeds exist)     (PySpark, containerized)  (dbt-duckdb, containerized)
                                  │                     ▲
                                  ▼                     │
                          Parquet output ────────────────┘
                          (external dbt source)

  Separately, on Databricks (manual notebook trigger, documented as
  next-step for full DatabricksSubmitRunOperator integration):

  Raw CSVs (Unity Catalog Volume)
         │
         ▼
  Databricks notebooks (Serverless compute)
         │
         ▼
  Delta Lake tables (ACID, versioned, MERGE INTO upserts, SCD Type 2)
         │
         ▼
  dbt-databricks adapter → int_customer_alerts_databricks → fct_customer_targets_databricks
  ## What this demonstrates, by layer

**dbt / DuckDB**
- Layered modeling: seeds → staging (`stg_*`) → intermediate (`int_*`) → marts (`fct_*`)
- Incremental models (`is_incremental()`, `delete+insert`), generic + custom tests, full docs/lineage

**PySpark**
- Reading the same raw source data dbt uses; staging logic reimplemented as DataFrame transformations and Spark SQL
- Joins with broadcast vs. shuffle plan inspection; incremental/SCD2-style logic via window functions
- Output written to Parquet, consumed directly by dbt as an external source — no manual export step

**Databricks + Delta Lake**
- Delta table writes, `DESCRIBE HISTORY`, time travel (`VERSION AS OF`)
- `MERGE INTO` upsert logic and full SCD Type 2 implementation (`is_current`, `effective_date`, `end_date`)
- Schema evolution (`mergeSchema`) and write-time `CHECK`/`NOT NULL` constraints, contrasted against dbt's post-build `schema.yml` tests
- `dbt-databricks` adapter connecting the local dbt project directly to live Delta tables

**Airflow**
- Full stack run via Docker Compose (webserver, scheduler, dag-processor, Postgres metadata DB, Redis) — the same pattern used in real production Airflow deployments, not a bare pip install
- Custom Docker image (Java + PySpark + dbt-duckdb baked in via Dockerfile) so DAG tasks can actually execute Spark and dbt inside containers
- DAG chaining raw-ingestion validation → Spark transform → `dbt run` → `dbt test`, using `>>` dependency operators
- Retries, `retry_delay`, and `on_failure_callback` alerting stubs (task-level and DAG-level) for failure handling
- `@daily` schedule, with the full chain verified via manual trigger and Graph View inspection

## Why this stack, and what each piece is actually for

- **dbt** owns transformation logic and testing — the SQL-first layer, portable across warehouses.
- **Spark** demonstrates the distributed-processing pattern, even at small scale here — the same DataFrame/window-function/join logic that matters once data outgrows a single node.
- **Databricks + Delta** shows the managed-lakehouse pattern: ACID transactions, schema enforcement, versioning, and native upsert (`MERGE INTO`) — replacing manual SCD2 logic built by hand in previous roles.
- **Airflow** ties it together as the orchestration layer, adding scheduling, retries, and failure handling — the piece that turns a set of scripts into an actual production pipeline.

**On idempotency** (a running theme across the incremental/MERGE/retry work): every write in this pipeline is designed to be safely rerunnable — dbt's `delete+insert` incremental strategy and Delta's `MERGE INTO` both match on a unique key rather than blindly inserting, so a retry (automatic, via Airflow, or manual) never duplicates data. This matters specifically because Airflow assumes failures are normal and retries by default — a pipeline built on raw inserts would corrupt itself under that assumption; this one doesn't.

## Project structure
seeds/ raw dummy data (customers, orders)
models/staging/ cleaned/renamed source data + source defs
models/intermediate/ consolidated alert logic (seed-based, Spark-based, Databricks-based)
models/marts/ final tiered outputs (seed-based, Spark-based, Databricks-based)
tests/ custom singular test
spark_pipeline/ PySpark scripts (staging, joins, window functions, Spark SQL + Parquet write-out)
spark_pipeline/output/ Parquet output consumed by dbt as an external source
airflow_pipeline/ Docker Compose stack, custom Dockerfile, DAGs
airflow_pipeline/dags/ full_customer_alerts_pipeline (production DAG), first_dag (learning DAG)
profiles.yml dbt profile (DuckDB dev/prod targets + Databricks target) — gitignored, contains credentials

## How to run

**Full orchestrated pipeline (recommended):**
```bash
cd airflow_pipeline
docker compose up airflow-init
docker compose up -d
# open http://localhost:8080 (airflow/airflow), trigger full_customer_alerts_pipeline
```

**Individual layers, run manually:**
```bash
# dbt (seed-based chain)
pip install dbt-duckdb && dbt seed && dbt run && dbt test

# Spark (feeds dbt via Parquet)
pip install pyspark pandas numpy
cd spark_pipeline && python sparksql_writeout.py
cd .. && dbt run --select int_customer_alerts_from_spark fct_customer_targets_from_spark

# Databricks + Delta (run notebooks in workspace first, then:)
pip install dbt-databricks
dbt run --target databricks --select int_customer_alerts_databricks fct_customer_targets_databricks
```

## What's next (Weeks 5-6)

This pipeline's outputs — the tiered `fct_customer_targets` tables — become the data foundation for the next phase: standing up a vector store and a basic RAG pipeline on top of this same data, moving from "AI-adjacent data engineering" into applied AI infrastructure.

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project. `profiles.yml` is excluded from version control since it contains a Databricks access token.