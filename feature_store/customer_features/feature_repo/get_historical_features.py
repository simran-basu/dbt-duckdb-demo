import pandas as pd
from datetime import datetime, timezone
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# --- This is the "entity dataframe" — the list of (entity, timestamp) pairs
# you want historical feature values for. In your real work, this is
# conceptually your target-list run: "for these physicians, as of this
# scoring date, give me their feature values."
entity_df = pd.DataFrame.from_dict({
    "customer_id": [1, 2, 3, 4, 5],
    "event_timestamp": [datetime.now(timezone.utc)] * 5,
})

# --- Pull historical (offline) feature values for each entity/timestamp pair
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "customer_stats:region",
        "customer_stats:total_orders",
        "customer_stats:total_spend",
        "customer_stats:pending_orders",
        "customer_stats:cancelled_orders",
        "customer_stats:alert_priority",
    ],
).to_df()

print("=== Historical features (offline, for training) ===")
print(training_df)