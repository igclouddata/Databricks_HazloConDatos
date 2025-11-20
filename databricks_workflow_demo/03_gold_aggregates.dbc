# 03_gold_aggregates.py

%run ./00_config

from pyspark.sql.functions import date_format, count, col

silver_customers = f"{CATALOG}.{SILVER_SCHEMA}.customers"
silver_orders    = f"{CATALOG}.{SILVER_SCHEMA}.orders"
gold_fact        = f"{CATALOG}.{GOLD_SCHEMA}.fact_daily_orders"

orders_df = spark.read.table(silver_orders)
customers_df = spark.read.table(silver_customers)

fact_df = (
    orders_df.join(
        customers_df,
        orders_df.order_customer_id == customers_df.customer_id,
        "inner",
    )
    .withColumn("order_date_key", date_format(col("order_date"), "yyyyMMdd"))
    .groupBy("order_date_key", "order_customer_id")
    .agg(
        count("*").alias("order_count")
    )
)

fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_fact)
print(f"Tabla Gold creada: {gold_fact}")
