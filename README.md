# dbt + DuckDB Demo Project

A local, self-contained dbt project demonstrating a rule-based alert consolidation and tiered customer targeting pipeline — built with dummy data to practice patterns used in production data pipelines (staging → intermediate → mart, incremental models, testing, docs).

## What this demonstrates

- **dbt fundamentals**: models, sources, refs, materializations (view/table/incremental)
- **Layered modeling**: raw seeds → staging (`stg_*`) → intermediate (`int_*`) → marts (`fct_*`)
- **Business logic replication**: `int_customer_alerts` consolidates multiple signals (spend, pending orders, cancellations) into a single prioritized alert per customer — a simplified version of rule-based alert consolidation logic
- **Tiered output**: `fct_customer_targets` produces a final tiered target list, similar in spirit to a monthly opportunity-index / tiered target list
- **Incremental models**: `fct_orders_incremental` uses `is_incremental()` and a `last_updated` watermark to process only new/changed rows on subsequent runs — analogous to SCD Type 2 change-tracking patterns
- **Testing**: generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) plus a custom singular test checking for duplicate customer-alert combinations
- **Documentation**: full dbt docs site with lineage graph, generated via `dbt docs generate`

## Project structure
seeds/              raw dummy data (customers, orders)
models/staging/      cleaned/renamed source data (stg_customers, stg_orders)
models/intermediate/ consolidated alert logic (int_customer_alerts)
models/marts/        final outputs (fct_customer_targets, fct_orders_incremental)
tests/                custom singular test
## How to run

```bash
pip install dbt-duckdb
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project.