# 02_silver_transform.py

%run ./00_config

from pyspark.sql.functions import trim, lower, col

# ------------------------
# Customers → Silver
# ------------------------
bronze_customers = f"{CATALOG}.{BRONZE_SCHEMA}.customers"
silver_customers = f"{CATALOG}.{SILVER_SCHEMA}.customers"

customers_df = (
    spark.read.table(bronze_customers)
        .selectExpr(
            "cast(id as int) as customer_id",
            "trim(customer_fname) as customer_fname",
            "trim(customer_lname) as customer_lname",
            "lower(trim(customer_email)) as customer_email",
            "trim(customer_city) as customer_city",
            "trim(customer_state) as customer_state",
            "trim(customer_zipcode) as customer_zipcode"
        )
        .dropDuplicates(["customer_id"])
)

customers_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_customers)
print(f"Tabla Silver creada: {silver_customers}")

# ------------------------
# Orders → Silver
# ------------------------
bronze_orders = f"{CATALOG}.{BRONZE_SCHEMA}.orders"
silver_orders = f"{CATALOG}.{SILVER_SCHEMA}.orders"

orders_df = (
    spark.read.table(bronze_orders)
        .selectExpr(
            "cast(id as int) as order_id",
            "cast(order_customer_id as int) as order_customer_id",
            "cast(order_date as timestamp) as order_date",
            "trim(order_status) as order_status"
        )
        .dropDuplicates(["order_id"])
)

orders_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_orders)
print(f"Tabla Silver creada: {silver_orders}")
