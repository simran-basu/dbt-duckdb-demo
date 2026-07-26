# Spark + dbt Pipeline: Customer Alert Consolidation & Tiered Targeting

An end-to-end local data pipeline demonstrating a rule-based alert consolidation and tiered customer targeting system — built with dummy data to practice patterns used in production pharma commercial analytics pipelines (business-rules-based alerting, tiered target lists).

The pipeline intentionally uses **two engines** across two layers to demonstrate both SQL-based transformation (dbt + DuckDB) and distributed-style transformation (PySpark), and shows how their outputs interoperate via Parquet.

## Architecture

Raw CSVs (seeds/)
│
├──────────────────────────────┐
│ │
▼ ▼
dbt seed/staging/ PySpark read + transform
intermediate/marts (staging logic, joins,
(DuckDB, SQL-based) window functions, Spark SQL)
│ │
│ ▼
│ Write to Parquet
│ (spark_pipeline/output/)
│ │
│ ▼
│ dbt source (external_location)
│ │
│ ▼
│ int_customer_alerts_from_spark
│ │
│ ▼
│ fct_customer_targets_from_spark
│
▼
fct_customer_targets
(original seed-based chain, kept for comparison)


Two parallel chains exist in this repo on purpose:
- **Seed-based chain**: `seeds → stg_* → int_customer_alerts → fct_customer_targets` — pure dbt/SQL, no Spark involved.
- **Spark-based chain**: `seeds → PySpark transform/join/window functions → Parquet → dbt source → int_customer_alerts_from_spark → fct_customer_targets_from_spark` — Spark does the upstream transformation, dbt picks up from the Parquet output and finishes the modeling layer.

Both produce the same business logic result, which was used to validate that the two engines agree before wiring them together.

## What this demonstrates

**dbt / DuckDB layer**
- Models, sources, refs, materializations (view/table/incremental)
- Layered modeling: raw seeds → staging (`stg_*`) → intermediate (`int_*`) → marts (`fct_*`)
- Rule-based alert consolidation logic, replicated from real alerting patterns (spend thresholds, pending/cancelled order signals) into a single prioritized alert per customer
- Tiered output mirroring a monthly opportunity-index / tiered target list
- Incremental models using `is_incremental()` and a `last_updated` watermark — analogous to SCD Type 2 change tracking
- Generic + custom singular tests (`unique`, `not_null`, `accepted_values`, `relationships`, plus a duplicate-alert-combo check)
- Full lineage and docs via `dbt docs generate`

**PySpark layer**
- Reading the same raw source data dbt uses (not separate dummy data)
- Reimplementing the dbt staging cleaning/renaming logic as DataFrame transformations
- Joining staging tables, examining broadcast vs. shuffle join physical plans
- Reimplementing incremental/SCD2-style logic using `row_number()`, `lag()`, `lead()` window functions
- Running the same consolidation logic as Spark SQL via a registered temp view
- Writing final output to Parquet in a location dbt/DuckDB reads directly as an external source

**Integration point**
- dbt's `sources.yml` uses `meta.external_location` to point directly at Spark's Parquet output — no manual export/import step, no format conversion. This is the core proof that Spark can serve as an upstream transformation layer feeding a dbt-modeled warehouse.

## Project structure

seeds/ raw dummy data (customers, orders)
models/staging/ cleaned/renamed source data + Spark source definition (src_spark.yml)
models/intermediate/ consolidated alert logic (seed-based and Spark-based versions)
models/marts/ final tiered outputs (seed-based and Spark-based versions)
tests/ custom singular test
spark_pipeline/ PySpark scripts (read, staging transform, joins, window functions, Spark SQL + Parquet write-out)
spark_pipeline/output/ Parquet output consumed by dbt as an external source


## How to run

**dbt (seed-based chain):**
```bash
pip install dbt-duckdb
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve
```

**Spark (upstream transformation):**
```bash
pip install pyspark pandas numpy
cd spark_pipeline
python sparksql_writeout.py
```

**dbt (Spark-based chain, run after Spark has written Parquet output):**
```bash
cd ..
dbt run --select int_customer_alerts_from_spark fct_customer_targets_from_spark
dbt test
```

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project.