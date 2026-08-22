from feast import FeatureStore

store = FeatureStore(repo_path='.')

features = store.get_online_features(
    features=[
        'customer_stats:total_spend',
        'customer_stats:pending_orders',
        'customer_stats:alert_priority',
    ],
    entity_rows=[{'customer_id': 1}, {'customer_id': 3}],
).to_df()

print(features)