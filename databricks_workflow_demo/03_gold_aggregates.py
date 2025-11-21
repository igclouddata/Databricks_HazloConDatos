import sys
import logging
from pyspark.sql.functions import date_format, count, col

# --------------------------------------------------
# 1. Logging profesional
# --------------------------------------------------
logger = logging.getLogger("workflow_medallion_gold")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --------------------------------------------------
# 2. Parámetro de fecha (opcional)
# --------------------------------------------------
if len(sys.argv) > 1:
    process_date = sys.argv[1]
else:
    process_date = "N/A"

logger.info("Iniciando tarea GOLD para la fecha %s", process_date)

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
        logger.info("Leyendo tablas Silver...")

        silver_customers = f"{CATALOG}.{SILVER_SCHEMA}.customers"
        silver_orders    = f"{CATALOG}.{SILVER_SCHEMA}.orders"
        gold_fact        = f"{CATALOG}.{GOLD_SCHEMA}.fact_daily_orders"

        orders_df = spark.read.table(silver_orders)
        customers_df = spark.read.table(silver_customers)

        logger.info(
            "Registros en Silver Orders: %s | Silver Customers: %s",
            orders_df.count(),
            customers_df.count()
        )

        # --------------------------------------------------
        # Join y agregaciones → Fact Table
        # --------------------------------------------------
        logger.info("Iniciando agregaciones para tabla FACT...")

        fact_df = (
            orders_df.join(
                customers_df,
                orders_df.order_customer_id == customers_df.customer_id,
                "inner",
            )
            .withColumn("order_date_key", date_format(col("order_date"), "yyyyMMdd"))
            .groupBy("order_date_key", "order_customer_id")
            .agg(count("*").alias("order_count"))
        )

        # Validación de calidad: registros resultantes
        total_fact = fact_df.count()
        if total_fact == 0:
            logger.warning("La tabla FACT GOLD quedó vacía después de la agregación.")

        logger.info("Registros en Fact: %s", total_fact)

        # --------------------------------------------------
        # Escritura en GOLD
        # --------------------------------------------------
        fact_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_fact)

        logger.info("Tabla Gold creada: %s", gold_fact)
        logger.info("Tarea GOLD finalizada correctamente.")

    except Exception as e:
        logger.error("Error en la tarea GOLD", exc_info=True)
        raise e


# --------------------------------------------------
# 5. Entry point (requerido por Jobs)
# --------------------------------------------------
if __name__ == "__main__":
    main()
