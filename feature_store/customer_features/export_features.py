import duckdb
import pandas as pd
from datetime import datetime, timezone

con = duckdb.connect("../../dev.duckdb")

df = con.sql("""
    SELECT customer_id, customer_name, region, total_orders, total_spend,
           pending_orders, cancelled_orders, alert_priority
    FROM fct_customer_targets
""").df()

# Feast requires a timestamp column for every feature row — this is what
# enables point-in-time correctness. Since your pipeline output doesn't
# carry a natural "as of" timestamp, we stamp it with the export time.
df["event_timestamp"] = datetime.now(timezone.utc)
df["created"] = datetime.now(timezone.utc)

df.to_parquet("feature_repo/data/customer_features.parquet")
print(f"Exported {len(df)} rows to feature_repo/data/customer_features.parquet")
print(df)