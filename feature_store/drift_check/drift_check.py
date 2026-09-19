import pandas as pd
import numpy as np
from scipy import stats

df = pd.read_csv("windowed_data.csv")

baseline = df[df["window"] == "baseline"]
current = df[df["window"] == "current"]


def check_numeric_drift(baseline_series: pd.Series, current_series: pd.Series, feature_name: str):
    """
    Compares two distributions of the same feature across time windows.
    Uses a Kolmogorov-Smirnov test — checks whether two samples likely come
    from the same underlying distribution. A low p-value (< 0.05) suggests
    the distributions are meaningfully different — i.e. drift.
    """
    ks_stat, p_value = stats.ks_2samp(baseline_series.dropna(), current_series.dropna())

    baseline_mean = baseline_series.mean()
    current_mean = current_series.mean()
    pct_change = ((current_mean - baseline_mean) / baseline_mean) * 100 if baseline_mean != 0 else float("nan")

    drift_detected = p_value < 0.05

    print(f"\n--- {feature_name} ---")
    print(f"Baseline mean: {baseline_mean:.2f} | Current mean: {current_mean:.2f} | Change: {pct_change:+.1f}%")
    print(f"KS statistic: {ks_stat:.4f} | p-value: {p_value:.4f}")
    print(f"Drift detected: {'YES' if drift_detected else 'no'}")

    return {
        "feature": feature_name,
        "baseline_mean": baseline_mean,
        "current_mean": current_mean,
        "pct_change": pct_change,
        "ks_stat": ks_stat,
        "p_value": p_value,
        "drift_detected": drift_detected,
    }


def check_categorical_drift(baseline_series: pd.Series, current_series: pd.Series, feature_name: str):
    """
    For categorical features (e.g. region, alert_reason), compares the
    proportion of each category between windows — a simple, interpretable
    check without needing a statistical test.
    """
    baseline_props = baseline_series.value_counts(normalize=True)
    current_props = current_series.value_counts(normalize=True)

    print(f"\n--- {feature_name} (category proportions) ---")
    all_categories = set(baseline_props.index) | set(current_props.index)
    for cat in sorted(all_categories):
        b_pct = baseline_props.get(cat, 0) * 100
        c_pct = current_props.get(cat, 0) * 100
        print(f"  {cat}: baseline {b_pct:.1f}% -> current {c_pct:.1f}%")


def check_mean_shift(baseline_series: pd.Series, current_series: pd.Series, feature_name: str, threshold_pct: float = 30.0):
    baseline_mean = baseline_series.mean()
    current_mean = current_series.mean()

    if baseline_mean == 0:
        pct_change = float("inf") if current_mean != 0 else 0
    else:
        pct_change = ((current_mean - baseline_mean) / baseline_mean) * 100

    drift_detected = abs(pct_change) > threshold_pct

    print(f"Mean shift check: {pct_change:+.1f}% (threshold: ±{threshold_pct}%) | Drift detected: {'YES' if drift_detected else 'no'}")

    return {"feature": feature_name, "mean_shift_drift": drift_detected, "pct_change": pct_change}


print("=" * 60)
print("DRIFT CHECK REPORT")
print("=" * 60)

results = []
for feature in ["total_spend", "pending_orders", "cancelled_orders", "total_orders"]:
    ks_result = check_numeric_drift(baseline[feature], current[feature], feature)
    mean_result = check_mean_shift(baseline[feature], current[feature], feature)
    # A feature is flagged if EITHER check catches it — this is the point:
    # combining a rigorous-but-sample-hungry test with a simple-but-sensitive one
    combined_drift = ks_result["drift_detected"] or mean_result["mean_shift_drift"]
    results.append({**ks_result, "mean_shift_drift": mean_result["mean_shift_drift"], "combined_drift": combined_drift})

check_categorical_drift(baseline["alert_reason"], current["alert_reason"], "alert_reason")
check_categorical_drift(baseline["region"], current["region"], "region")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
drifted_features = [r["feature"] for r in results if r["combined_drift"]]
if drifted_features:
    print(f"Drift detected in: {', '.join(drifted_features)}")
    print("Recommendation: review scoring thresholds (e.g. high_value_customer cutoff) — they may be stale relative to current data.")
else:
    print("No significant drift detected across checked features.")