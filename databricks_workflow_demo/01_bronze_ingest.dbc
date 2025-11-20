# 01_bronze_ingest.py

# Cargar configuración común
%run ./00_config

from pyspark.sql.functions import expr

# ------------------------
# Datos de clientes (Bronze)
# ------------------------
customers_bronze_df = (
    spark.range(1, 101)  # 100 clientes
        .withColumn("customer_fname", expr("concat('Nombre_', id)"))
        .withColumn("customer_lname", expr("concat('Apellido_', id)"))
        .withColumn("customer_email", expr("concat('cliente', id, '@demo.com')"))
        .withColumn("customer_city", expr("concat('Ciudad_', (id % 5))"))
        .withColumn("customer_state", expr("concat('Region_', (id % 3))"))
        .withColumn("customer_zipcode", expr("lpad(cast(id as string), 5, '0')"))
)

customers_bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.customers"
customers_bronze_df.write.format("delta").mode("overwrite").saveAsTable(customers_bronze_table)
print(f"Tabla Bronze creada: {customers_bronze_table}")

# ------------------------
# Datos de órdenes (Bronze)
# ------------------------
orders_bronze_df = (
    spark.range(1, 501)  # 500 órdenes
        .withColumn("order_customer_id", expr("1 + (id % 100)"))
        .withColumn("order_date", expr("date_sub(current_date(), cast(rand() * 30 as int))"))
        .withColumn("order_status", expr("case when id % 5 = 0 then 'CANCELLED' else 'COMPLETED' end"))
)

orders_bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.orders"
orders_bronze_df.write.format("delta").mode("overwrite").saveAsTable(orders_bronze_table)
print(f"Tabla Bronze creada: {orders_bronze_table}")
