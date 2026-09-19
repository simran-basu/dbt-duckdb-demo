from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

spark = SparkSession.builder \
    .appName("Week2StagingTransform") \
    .getOrCreate()

# --- Read raw sources (same as Monday) ---
customers_raw = spark.read.csv("../seeds/customers.csv", header=True, inferSchema=True)
orders_raw = spark.read.csv("../seeds/orders.csv", header=True, inferSchema=True)

# --- Replicate stg_customers.sql logic ---
# original SQL: select customer_id, name as customer_name, signup_date, lower(region) as region
stg_customers = customers_raw.select(
    col("customer_id"),
    col("name").alias("customer_name"),
    col("signup_date"),
    lower(col("region")).alias("region")
)

# --- Replicate stg_orders.sql logic ---
# original SQL: select order_id, customer_id, order_date, order_amount, lower(status) as order_status, last_updated
stg_orders = orders_raw.select(
    col("order_id"),
    col("customer_id"),
    col("order_date"),
    col("order_amount"),
    lower(col("STATUS")).alias("order_status"),
    col("last_updated")
)

print("=== stg_customers (PySpark) ===")
stg_customers.show()

print("=== stg_orders (PySpark) ===")
stg_orders.show()

stg_customers.printSchema()
stg_orders.printSchema()