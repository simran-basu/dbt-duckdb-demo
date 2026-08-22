import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect("../dev.duckdb")

baseline_df = con.sql("SELECT * FROM fct_customer_targets").df()
baseline_df["window"] = "baseline"

# Simulate a later time window where spend has drifted upward
# (e.g. inflation, a pricing change, a new high-value segment entering)
shifted_df = baseline_df.copy()
shifted_df["total_spend"] = shifted_df["total_spend"] * np.random.uniform(1.8, 2.5, size=len(shifted_df))
shifted_df["pending_orders"] = shifted_df["pending_orders"] + np.random.choice([0, 1, 2], size=len(shifted_df))
shifted_df["window"] = "current"

combined = pd.concat([baseline_df, shifted_df], ignore_index=True)
combined.to_csv("windowed_data.csv", index=False)

print("Baseline total_spend stats:")
print(baseline_df["total_spend"].describe())
print("\nShifted (current) total_spend stats:")
print(shifted_df["total_spend"].describe())