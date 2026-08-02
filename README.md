# Multi-Engine Data Pipeline: Customer Alert Consolidation & Tiered Targeting

An end-to-end data pipeline demonstrating rule-based alert consolidation and tiered customer targeting — built with dummy data to practice patterns used in production pharma commercial analytics pipelines (business-rules-based alerting, tiered target lists, SCD Type 2 change tracking).

The project deliberately spans **three storage/compute layers** to demonstrate the same business logic implemented across the stack most modern data engineering job descriptions ask for: local SQL-based transformation (dbt + DuckDB), distributed transformation (PySpark), and a managed lakehouse (Databricks + Delta Lake).

## Architecture

Raw CSVs (seeds/)
│
├─────────────────────────┬──────────────────────────────┐
│ │ │
▼ ▼ ▼
dbt seed/staging/ PySpark (local) Databricks notebooks
intermediate/marts read + transform (Serverless compute)
(DuckDB, SQL-based) write to Parquet │
│ │ ▼
│ ▼ Delta Lake tables
│ dbt source (stg_customers, stg_orders,
│ (external_location) int_customer_alerts_delta,
│ │ customers_scd2)
│ ▼ │
│ int_customer_alerts_from_spark ▼
│ │ dbt-databricks adapter
│ ▼ (source + models)
│ fct_customer_targets_from_spark │
│ ▼
│ int_customer_alerts_databricks
│ │
│ ▼
│ fct_customer_targets_databricks
▼
fct_customer_targets
(original seed-based chain, kept for comparison)
Three parallel chains exist in this repo on purpose, each building on the last:
- **Seed-based (DuckDB)**: `seeds → stg_* → int_customer_alerts → fct_customer_targets` — pure dbt/SQL, no external engine.
- **Spark + Parquet (DuckDB)**: PySpark reads the same raw CSVs, replicates staging/intermediate logic, writes Parquet — dbt reads that Parquet directly as an external source and finishes the modeling layer.
- **Databricks + Delta**: Databricks notebooks (Serverless compute) read the same raw CSVs from a Unity Catalog Volume, write Delta tables with full ACID transactions, schema enforcement, versioning, and `MERGE INTO` upsert/SCD2 logic — dbt then connects directly to Databricks via the `dbt-databricks` adapter and builds on top of those live Delta tables.

All three produce the same underlying business logic result, used throughout to validate that each engine agrees before wiring them together.

## What this demonstrates

**dbt / DuckDB (local)**
- Models, sources, refs, materializations (view/table/incremental)
- Layered modeling: seeds → staging (`stg_*`) → intermediate (`int_*`) → marts (`fct_*`)
- Rule-based alert consolidation and tiered target list output
- Incremental models using `is_incremental()` and a `last_updated` watermark
- Generic + custom singular tests, full docs/lineage via `dbt docs generate`

**PySpark (local)**
- Reading the same raw source data dbt uses, reimplementing staging logic as DataFrame transformations
- Joins with broadcast vs. shuffle plan inspection
- Incremental/SCD2-style logic via `row_number()`, `lag()`, `lead()` window functions
- Spark SQL via registered temp views, writing final output to Parquet as a dbt-readable external source

**Databricks + Delta Lake**
- Unity Catalog Volumes for file storage (modern replacement for the legacy DBFS root)
- Delta table writes (`saveAsTable`), `DESCRIBE HISTORY`, time travel (`VERSION AS OF`)
- `MERGE INTO` upsert logic, replacing manual delete+insert incremental strategies
- Full SCD Type 2 implementation (`is_current`, `effective_date`, `end_date`) using `MERGE INTO` — a direct, modern parallel to SCD2 logic built manually in previous roles
- Schema evolution (`mergeSchema`) and `CHECK`/`NOT NULL` constraints — enforced at write time, contrasted against dbt's post-build `schema.yml` tests
- `dbt-databricks` adapter connecting the local dbt project directly to live Databricks Delta tables via a second profile target

## Project structure
seeds/ raw dummy data (customers, orders)
models/staging/ cleaned/renamed source data + source defs (src_spark.yml, src_databricks.yml)
models/intermediate/ consolidated alert logic (seed-based, Spark-based, Databricks-based versions)
models/marts/ final tiered outputs (seed-based, Spark-based, Databricks-based versions)
tests/ custom singular test
spark_pipeline/ PySpark scripts (read, staging transform, joins, window functions, Spark SQL + Parquet write-out)
spark_pipeline/output/ Parquet output consumed by dbt as an external source
(Databricks notebooks live in the Databricks workspace itself, not this repo, since Community Edition notebooks aren't exported here.)

## How to run

**dbt (seed-based chain, DuckDB):**
```bash
pip install dbt-duckdb
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve
```

**Spark (upstream transformation, feeds DuckDB):**
```bash
pip install pyspark pandas numpy
cd spark_pipeline
python sparksql_writeout.py
cd ..
dbt run --select int_customer_alerts_from_spark fct_customer_targets_from_spark
```

**Databricks + Delta chain:**
1. Run the staging/intermediate/SCD2 notebooks in a Databricks workspace (Serverless compute, Unity Catalog Volumes for CSV upload).
2. Install the adapter locally: `pip install dbt-databricks`
3. Add a `databricks` target to `~/.dbt/profiles.yml` with your workspace host, HTTP path, and access token.
4. Run:
```bash
dbt run --target databricks --select int_customer_alerts_databricks fct_customer_targets_databricks
dbt test --target databricks --select stg_customers stg_orders int_customer_alerts_databricks fct_customer_targets_databricks
```

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project.