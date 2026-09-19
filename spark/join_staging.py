from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, broadcast

spark = SparkSession.builder \
    .appName("Week2JoinDemo") \
    .getOrCreate()

# --- Read raw sources ---
customers_raw = spark.read.csv("../seeds/customers.csv", header=True, inferSchema=True)
orders_raw = spark.read.csv("../seeds/orders.csv", header=True, inferSchema=True)

# --- Staging transformations (from Tuesday) ---
stg_customers = customers_raw.select(
    col("customer_id"),
    col("name").alias("customer_name"),
    col("signup_date"),
    lower(col("region")).alias("region")
)

stg_orders = orders_raw.select(
    col("order_id"),
    col("customer_id"),
    col("order_date"),
    col("order_amount"),
    lower(col("STATUS")).alias("order_status"),
    col("last_updated")
)

# --- Regular join (Spark decides shuffle vs broadcast automatically) ---
joined = stg_orders.join(stg_customers, on="customer_id", how="left")

print("=== Joined (default plan) ===")
joined.show()

joined.explain(mode="formatted")

# --- Explicit broadcast join ---
joined_broadcast = stg_orders.join(broadcast(stg_customers), on="customer_id", how="left")

print("=== Joined (explicit broadcast) ===")
joined_broadcast.explain(mode="formatted")