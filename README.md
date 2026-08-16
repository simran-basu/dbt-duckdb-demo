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

## Architecture (RAG layer, sitting on top of the pipeline)
fct_customer_targets (DuckDB)
                dbt/Spark pipeline output
                          │
                          ▼
             pull_pipeline_data.py
             (queries dev.duckdb directly,
              derives alert_note text field
              from real alert_reason/spend/
              pending/cancelled columns)
                          │
                          ▼
             embed_alert_notes.py
             (sentence-transformers →
              384-dim embeddings)
                          │
                          ▼
             Chroma (persistent, local)
             collection: customer_alert_notes
             (embedding + document + metadata:
              customer_id, region, alert_reason,
              target_tier)
                          │
                          ▼
             retrieve_alert_notes.py
             (semantic search + optional
              metadata filter, e.g. region)
                          │
                          ▼
             generate.py
             (retrieved chunks → grounded
              prompt → flan-t5-base → answer
              + source attribution)
This is the same integration pattern as the Spark→dbt and Databricks→dbt work earlier: a new layer consuming `fct_customer_targets` the same way a BI tool or another pipeline stage would, rather than operating on a disconnected dataset.

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

**RAG / Vector Retrieval Layer**
- Local, zero-server vector store (ChromaDB, persistent client) — same local-first philosophy as DuckDB
- Local embedding model (`sentence-transformers`, `all-MiniLM-L6-v2`) — no API key or hosted service required
- Proper text chunking (`RecursiveCharacterTextSplitter`, boundary-aware, with overlap) rather than naive fixed-size splitting — verified via before/after retrieval comparison
- Two corpora embedded and made retrievable:
  - A dummy pharma knowledge base (drug info sheets, therapy area summaries) for general RAG mechanics
  - **Alert notes derived directly from `fct_customer_targets`** — the actual output of the dbt/Spark pipeline — proving the RAG layer is a downstream consumer of the same data foundation, not an isolated exercise
- Distance-threshold filtering to reject low-relevance retrieval results, tested against clearly out-of-domain and topically-adjacent-but-unanswered queries
- Full generation loop: retrieved chunks → grounded prompt template → local LLM (`flan-t5-base`) → answer with source attribution
- Metadata filtering (e.g. by region, therapy area) alongside semantic search, using metadata fields sourced from the pipeline's structured columns

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
rag_pipeline/                       Chroma vector store, embedding/retrieval/generation scripts
rag_pipeline/corpus/                  dummy pharma knowledge base (drug info, therapy summaries)
rag_pipeline/chroma_db/               persistent Chroma vector store (gitignored — regenerable)
rag_pipeline/alert_notes.csv          text field derived from fct_customer_targets

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
**RAG layer (after the dbt pipeline has run at least once):**
```bash
cd rag_pipeline
pip install chromadb sentence-transformers langchain-text-splitters transformers accelerate sentencepiece

# Pull real pipeline output and derive alert notes
python pull_pipeline_data.py

# Embed and store
python embed_alert_notes.py

# Retrieve + generate grounded answers
python retrieve_alert_notes.py
python generate.py

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project. `profiles.yml` is excluded from version control since it contains a Databricks access token.