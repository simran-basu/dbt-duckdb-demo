from feast import FeatureStore

store = FeatureStore(repo_path=".")


def get_latest_customer_features(customer_ids: list[int]) -> "pd.DataFrame":
    entity_rows = [{"customer_id": cid} for cid in customer_ids]

    features = store.get_online_features(
        features=[
            "customer_stats:region",
            "customer_stats:total_orders",
            "customer_stats:total_spend",
            "customer_stats:pending_orders",
            "customer_stats:cancelled_orders",
            "customer_stats:alert_priority",
        ],
        entity_rows=entity_rows,
    ).to_df()

    return features


if __name__ == "__main__":
    result = get_latest_customer_features([1, 2, 3, 4, 5])
    print("=== Latest features (online, for serving) ===")
    print(result)