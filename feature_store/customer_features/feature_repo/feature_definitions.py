from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, Project
from feast.types import Float32, Int64, String

project = Project(
    name="customer_features",
    description="Feature store for customer alert/targeting data, sourced from the dbt/Spark pipeline",
)

# Entity: the "primary key" features are looked up by
customer = Entity(name="customer", join_keys=["customer_id"])

# Data source: points at the Parquet export of fct_customer_targets
customer_features_source = FileSource(
    name="customer_features_source",
    path="data/customer_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature view: the pipeline-derived, physician/customer-level aggregates
# analogous to opportunity-index inputs — total spend, order activity,
# pending/cancelled signals, and the alert priority the pipeline already computes.
customer_stats_fv = FeatureView(
    name="customer_stats",
    entities=[customer],
    ttl=timedelta(days=90),
    schema=[
        Field(name="region", dtype=String),
        Field(name="total_orders", dtype=Int64),
        Field(name="total_spend", dtype=Float32),
        Field(name="pending_orders", dtype=Int64),
        Field(name="cancelled_orders", dtype=Int64),
        Field(name="alert_priority", dtype=Int64),
    ],
    online=True,
    source=customer_features_source,
    tags={"team": "commercial_analytics"},
)