import sys
import logging
from pyspark.sql.functions import trim, lower, col, expr

# --------------------------------------------------
# 1. Logging profesional
# --------------------------------------------------
logger = logging.getLogger("workflow_medallion_silver")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --------------------------------------------------
# 2. Parámetro de fecha (si lo usas)
# --------------------------------------------------
if len(sys.argv) > 1:
    process_date = sys.argv[1]
else:
    process_date = "N/A"

logger.info(f"Iniciando tarea SILVER para fecha: {process_date}")

# --------------------------------------------------
# 3. Configuración común
# --------------------------------------------------
CATALOG = "project_databricks_hcd"     # cámbialo si es otro

BRONZE_SCHEMA = "demo_bronze"
SILVER_SCHEMA = "demo_silver"
GOLD_SCHEMA   = "demo_gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

logger.info(
    "Usando catálogo %s con esquemas %s, %s, %s",
    CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA
)

# --------------------------------------------------
# 4. Lógica principal con manejo de errores
# --------------------------------------------------
def main():
    try:
        logger.info("Transformando datos para SILVER...")

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

        # 🔍 Métrica: verificar nulos en el correo
        null_emails = customers_df.filter(col("customer_email").isNull()).count()
        if null_emails > 0:
            logger.warning("Se encontraron %s emails nulos en SILVER.customers", null_emails)

        customers_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(silver_customers)

        logger.info("Tabla Silver creada: %s", silver_customers)

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

        # 🔍 Métrica: cuántas órdenes tienen status NULL
        null_status = orders_df.filter(col("order_status").isNull()).count()
        if null_status > 0:
            logger.warning("Se encontraron %s registros con status nulo", null_status)

        orders_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(silver_orders)

        logger.info("Tabla Silver creada: %s", silver_orders)
        logger.info("Tarea SILVER finalizada correctamente.")

    except Exception as e:
        logger.error("Error en la tarea SILVER", exc_info=True)
        raise e


# --------------------------------------------------
# 5. Entry point para Workflows
# --------------------------------------------------
if __name__ == "__main__":
    main()
