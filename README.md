# End-to-End Data Pipeline: Customer Alert Consolidation & Tiered Targeting

An orchestrated, multi-engine data pipeline demonstrating rule-based alert consolidation and tiered customer targeting — built with dummy data to practice patterns used in production pharma commercial analytics pipelines (business-rules-based alerting, tiered target lists, SCD Type 2 change tracking), fully automated end-to-end with Airflow, and extended with a RAG layer and a feature store/MLOps layer as downstream consumers of the same data foundation.

This project spans the core modern data engineering and AI-adjacent stack: **dbt (transformation/testing) → Spark (distributed processing) → Databricks/Delta Lake (managed lakehouse) → Airflow (orchestration) → RAG (vector retrieval + generation) → Feast/MLflow (feature store + experiment tracking + drift detection)**.

## Architecture
                      ┌─────────────────────────┐
                      │   Airflow (Docker)       │
                      │   DAG: full_customer_    │
                      │   alerts_pipeline        │
                      │   Schedule: @daily       │
                      └───────────┬──────────────┘
                                  │
  validate_raw_ingestion → run_dbt_seed_chain → run_spark_transform → run_dbt_run → run_dbt_test
  (checks seeds exist)    (builds Chain A:      (PySpark,             (dbt-duckdb,
                            fct_customer_targets)  containerized)       containerized)
                                  │                    │
                                  │                    ▼
                                  │             Parquet output
                                  │             (external dbt source)
                                  │                    │
                                  │                    ▼
                                  │             fct_customer_targets_from_spark
                                  ▼
                          fct_customer_targets
                                  │
             ┌────────────────────┴────────────────────┐
             ▼                                          ▼
      RAG layer                                  Feature store / MLOps
(Chroma, sentence-transformers,            (Feast offline/online split,
 chunking, retrieval, generation)           MLflow experiment tracking,
                                             drift detection)

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
- Full stack run via Docker Compose (webserver, scheduler, dag-processor, triggerer, Postgres metadata DB, Redis) — the same pattern used in real production Airflow deployments, not a bare pip install
- Custom Docker image (Java + PySpark + dbt-duckdb baked in via Dockerfile) so DAG tasks can actually execute Spark and dbt inside containers
- DAG chaining raw-ingestion validation → seed-based dbt build → Spark transform → `dbt run` → `dbt test`, using `>>` dependency operators
- Retries, `retry_delay`, and `on_failure_callback` alerting stubs (task-level and DAG-level) for failure handling
- `@daily` schedule, with the full 5-task chain verified via manual trigger and Graph View inspection

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

**Feature Store + MLOps Layer**
- Local, zero-server feature store (Feast, SQLite-backed online store — same local-first philosophy as DuckDB and Chroma)
- `customer_stats` feature view defined against real pipeline output (`fct_customer_targets`, exported to Parquet as Feast's offline source) — `total_orders`, `total_spend`, `pending_orders`, `cancelled_orders`, `alert_priority`, mirroring physician-level opportunity-index-style inputs
- Both retrieval paths implemented and verified against the same underlying data:
  - `get_historical_features` — point-in-time-correct, offline, training-oriented (parallels the historical-snapshot correctness needed for reproducible target-list scoring)
  - `get_online_features` — fast single-entity lookup, online (SQLite), serving-oriented
- Materialization pipeline (offline → online sync) run and verified end-to-end
- MLflow experiment tracking applied to the existing rule-based alert-scoring logic — parameters (threshold), metrics (customers flagged), and artifacts (scored output) logged across two runs, demonstrating experiment tracking generalizes beyond ML models to any parameterized, tunable business logic
- Data drift detection: KS-test-based distribution comparison across simulated time windows, paired with a percent-change-in-mean fallback check after the KS test proved insufficiently sensitive at small sample sizes — a first-hand demonstration of a real limitation in naive drift detection, not just a textbook implementation

## Why this stack, and what each piece is actually for

- **dbt** owns transformation logic and testing — the SQL-first layer, portable across warehouses.
- **Spark** demonstrates the distributed-processing pattern, even at small scale here — the same DataFrame/window-function/join logic that matters once data outgrows a single node.
- **Databricks + Delta** shows the managed-lakehouse pattern: ACID transactions, schema enforcement, versioning, and native upsert (`MERGE INTO`) — replacing manual SCD2 logic built by hand in previous roles.
- **Airflow** ties it together as the orchestration layer, adding scheduling, retries, and failure handling — the piece that turns a set of scripts into an actual production pipeline.
- **RAG (Chroma + sentence-transformers)** shows how unstructured/semi-structured text derived from pipeline output can be made searchable by meaning, not just keyword — a different consumption pattern than SQL/BI on the same underlying data.
- **Feast + MLflow** show the data-layer foundations ML systems are built on: consistent offline/online feature serving, and reproducible experiment tracking — applied here to real rule-based scoring logic, without requiring actual ML modeling expertise, since that's a separate, deliberately out-of-scope skill.

**On idempotency** (a running theme across the incremental/MERGE/retry work): every write in this pipeline is designed to be safely rerunnable — dbt's `delete+insert` incremental strategy and Delta's `MERGE INTO` both match on a unique key rather than blindly inserting, so a retry (automatic, via Airflow, or manual) never duplicates data. This matters specifically because Airflow assumes failures are normal and retries by default — a pipeline built on raw inserts would corrupt itself under that assumption; this one doesn't.

## Architecture review notes (Week 7)

A few things surfaced when reviewing the full pipeline end-to-end, since it was built incrementally across many weeks. Documenting them here rather than hiding them:

- **Three parallel chains exist by design, not by accident**: `fct_customer_targets` (seed-based), `fct_customer_targets_from_spark` (Spark-fed), and `fct_customer_targets_databricks` (Delta-fed) all compute the same business logic. This was intentional — each chain exists to compare engines (dbt-only vs. Spark-augmented vs. Databricks/Delta-augmented), not to be three separate "real" pipelines.
- **The Airflow DAG orchestrates two of the three chains**: `full_customer_alerts_pipeline` builds both the seed-based chain (`run_dbt_seed_chain`) and the Spark-fed chain (`run_spark_transform` → `run_dbt_run` → `run_dbt_test`) in one run, in that order. The Databricks/Delta chain remains a manually-triggered notebook workflow — documented as a next step for full `DatabricksSubmitRunOperator` integration, not yet automated.
- **RAG and Feature Store layers read `fct_customer_targets`** (the seed-based chain), which is why `run_dbt_seed_chain` was added to the DAG — this makes those downstream consumers genuinely built on data the orchestrated pipeline produces, rather than a disconnected manual table.
- **`profiles.yml` uses an environment variable for the DuckDB path** (`DBT_DUCKDB_PATH`, defaulting to `dev.duckdb`), so the same profile works both for local `dbt run`/`dbt debug` and inside the Airflow containers (which set the variable to an absolute container path). This was fixed after discovering the container-specific path had silently broken local dbt runs.
- **The `seeds/` folder is mounted twice** into the Airflow containers (once directly, once as part of the whole project root) — kept intentionally redundant so `sparksql_writeout.py`'s relative path (`../seeds/...`) keeps working without hardcoding container-specific absolute paths.

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
rag_pipeline/ Chroma vector store, embedding/retrieval/generation scripts
rag_pipeline/corpus/ dummy pharma knowledge base (drug info, therapy summaries)
rag_pipeline/chroma_db/ persistent Chroma vector store (gitignored — regenerable)
rag_pipeline/alert_notes.csv text field derived from fct_customer_targets
feature_store/customer_features/ Feast feature repo (entity, feature view, offline/online retrieval)
mlflow_experiments/ MLflow-tracked runs of parameterized rule-based scoring
drift_check/ Feature distribution drift detection (KS test + mean-shift check)
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

**RAG layer (after the dbt pipeline has run at least once):**
```bash
cd rag_pipeline
pip install chromadb sentence-transformers langchain-text-splitters transformers accelerate sentencepiece

python pull_pipeline_data.py     # pull real pipeline output, derive alert notes
python embed_alert_notes.py      # embed and store
python retrieve_alert_notes.py   # retrieve
python generate.py               # grounded generation
```

**Feature store + MLOps layer:**
```bash
cd feature_store/customer_features
pip install feast mlflow scipy
python export_features.py
cd feature_repo
feast apply
feast materialize 2026-01-01T00:00:00 2026-08-20T00:00:00
python get_historical_features.py
python get_online_features.py

cd ../../../mlflow_experiments
python log_scoring_run.py
mlflow ui  # http://localhost:5000

cd ../drift_check
python simulate_drift.py
python drift_check.py
```

## Status

All planned phases are complete: dbt/DuckDB foundation, Spark (local distributed processing), Databricks/Delta Lake (managed lakehouse), Airflow (orchestration), a RAG layer (vector retrieval + generation), and a feature store/MLOps layer (offline/online feature serving, experiment tracking, drift detection) — all consuming the same `fct_customer_targets` pipeline output, which the orchestrated Airflow DAG itself builds. This demonstrates the full span of a modern AI-adjacent data engineering stack: batch transformation, distributed processing, lakehouse storage, orchestration, and the data-layer foundations that ML/AI systems are built on top of — without requiring ML modeling expertise, since every layer here is about making data correctly available, versioned, and monitored, which is the data engineer's actual scope of ownership.

## Notes

All data is synthetic/dummy — no real or proprietary data is used in this project. `profiles.yml` is excluded from version control since it contains a Databricks access token.