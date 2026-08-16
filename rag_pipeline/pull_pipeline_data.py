import duckdb

# Connect to the same DuckDB file your dbt project writes to
con = duckdb.connect("../dev.duckdb")

# Pull the actual mart output from your pipeline
df = con.sql("""
    SELECT customer_id, customer_name, region, total_orders, total_spend,
           pending_orders, cancelled_orders, alert_reason, alert_priority, target_tier
    FROM fct_customer_targets
""").df()

print(df)


def generate_alert_note(row):
    """Simulates a free-text alert note a rep/system might write, grounded in real pipeline fields."""
    if row["alert_reason"] == "high_value_customer":
        return (
            f"{row['customer_name']} in {row['region']} region is a high-value account with "
            f"${row['total_spend']:.2f} in total spend across {row['total_orders']} orders. "
            f"Recommend priority outreach to maintain engagement and explore upsell opportunities."
        )
    elif row["alert_reason"] == "pending_order_followup":
        return (
            f"{row['customer_name']} has {row['pending_orders']} pending order(s) requiring follow-up. "
            f"Total historical spend is ${row['total_spend']:.2f}. Recommend contacting to confirm order status "
            f"and resolve any blockers before the order lapses."
        )
    elif row["alert_reason"] == "cancellation_review":
        return (
            f"{row['customer_name']} has {row['cancelled_orders']} cancelled order(s), which may indicate "
            f"dissatisfaction or a competitive loss. Recommend a retention call to understand the cancellation "
            f"reason and rebuild the relationship."
        )
    else:
        return (
            f"{row['customer_name']} shows no active alerts. Standard cadence outreach is sufficient; "
            f"no immediate action required."
        )


df["alert_note"] = df.apply(generate_alert_note, axis=1)

for _, row in df.iterrows():
    print(f"\n[{row['customer_id']}] {row['customer_name']} ({row['target_tier']})")
    print(row["alert_note"])

df.to_csv("alert_notes.csv", index=False)
print(f"\nSaved {len(df)} alert notes to alert_notes.csv")