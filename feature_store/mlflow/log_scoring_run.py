import duckdb
import mlflow
import pandas as pd

con = duckdb.connect("../dev.duckdb")

df = con.sql("""
    SELECT customer_id, customer_name, total_spend, pending_orders, cancelled_orders
    FROM stg_customers c
    LEFT JOIN (
        SELECT customer_id,
               SUM(order_amount) AS total_spend,
               SUM(CASE WHEN order_status = 'pending' THEN 1 ELSE 0 END) AS pending_orders,
               SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
        FROM stg_orders
        GROUP BY customer_id
    ) o USING (customer_id)
""").df()


def score_customers(df: pd.DataFrame, high_value_threshold: float):
    """The same rule-based logic from int_customer_alerts.sql, parameterized."""
    def classify(row):
        spend = row["total_spend"] or 0
        pending = row["pending_orders"] or 0
        cancelled = row["cancelled_orders"] or 0
        if spend > high_value_threshold:
            return "high_value_customer"
        elif pending > 0:
            return "pending_order_followup"
        elif cancelled > 0:
            return "cancellation_review"
        else:
            return "no_alert"

    df = df.copy()
    df["alert_reason"] = df.apply(classify, axis=1)
    return df


mlflow.set_experiment("customer_alert_scoring")

# --- Run 1: original threshold (400), matching your dbt model ---
with mlflow.start_run(run_name="threshold_400"):
    threshold = 400
    scored = score_customers(df, high_value_threshold=threshold)

    high_value_count = (scored["alert_reason"] == "high_value_customer").sum()

    mlflow.log_param("high_value_threshold", threshold)
    mlflow.log_metric("high_value_customer_count", high_value_count)
    mlflow.log_metric("total_customers_scored", len(scored))

    scored.to_csv("scored_customers_400.csv", index=False)
    mlflow.log_artifact("scored_customers_400.csv")

    print(f"Run 1 (threshold=400): {high_value_count} high-value customers")

# --- Run 2: lower threshold (350) — a "what if we tuned this" comparison ---
with mlflow.start_run(run_name="threshold_350"):
    threshold = 350
    scored = score_customers(df, high_value_threshold=threshold)

    high_value_count = (scored["alert_reason"] == "high_value_customer").sum()

    mlflow.log_param("high_value_threshold", threshold)
    mlflow.log_metric("high_value_customer_count", high_value_count)
    mlflow.log_metric("total_customers_scored", len(scored))

    scored.to_csv("scored_customers_350.csv", index=False)
    mlflow.log_artifact("scored_customers_350.csv")

    print(f"Run 2 (threshold=350): {high_value_count} high-value customers")

print("\nRun 'mlflow ui' from this folder and open http://localhost:5000 to view both runs.")