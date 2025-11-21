
# imporamos las librerias
import sys
import logging
from pyspark.sql.functions import expr, col, rand

# -----------------------------------------
# Logging profesional
# -----------------------------------------

logger = logging.getLogger('Workflow Medallion')
logger.setLevel(logging.INFO)

# Para que el log salga en stdout del cluster
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --------------------------------------------------
# 2. Parámetro de fecha (opcional)
#    -> lo puedes pasar desde el Job como argumento
# --------------------------------------------------
if len(sys.argv) > 1:
    process_date = sys.argv[1]
else:
    process_date = "N/A"

logger.info(f"Iniciando tarea BRONZE para fecha: {process_date}")

# -----------------------------------------
# Cargar configuración común
# (IMPORTANTE: importar, no usar %run)
# -----------------------------------------

CATALOG = "project_databricks_hcd"  # cámbialo si es otro

BRONZE_SCHEMA = "demo_bronze"
SILVER_SCHEMA = "demo_silver"
GOLD_SCHEMA   = "demo_gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

print(f"Usando catálogo: {CATALOG}")
print(f"Esquemas: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")

logger.info(
    "Usando catálogo %s con esquemas %s, %s, %s",
    CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA
)

# --------------------------------------------------
# 4. Lógica principal en una función con try/except
# --------------------------------------------------
def main():
    try:
        logger.info("Creando tablas BRONZE...")

        # ------------------------
        # Clientes (Bronze)
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
        logger.info("Tabla Bronze creada: %s", customers_bronze_table)

        # Ejemplo de métrica para el warning (puedes cambiarla)
        count_nulls = customers_bronze_df.filter(col("customer_email").isNull()).count()
        if count_nulls > 0:
            logger.warning(
                "Se encontraron %s registros con nulos en customer_email en Bronze",
                count_nulls
            )


        # ------------------------
        # Datos de órdenes (Bronze)
        # ------------------------
        orders_bronze_df = (
            spark.range(1, 501)  # 500 órdenes
                .withColumn("order_customer_id", expr("1 + (id % 100)"))
                .withColumn(
                    "order_date",
                    expr("date_sub(current_date(), cast(rand() * 30 as int))")
                )
                .withColumn(
                    "order_status",
                    expr(
                        "case when id % 5 = 0 then 'CANCELLED' "
                        "else 'COMPLETED' end"
                    )
                )
        )

        orders_bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.orders"
        orders_bronze_df.write.format("delta").mode("overwrite").saveAsTable(orders_bronze_table)
        logger.info("Tabla Bronze creada: %s", orders_bronze_table)

        logger.info("Tarea BRONZE finalizada correctamente.")

    except Exception as e:
        # Aquí sí tiene sentido el logger.error
        logger.error("Error cargando tablas BRONZE", exc_info=True)
        # Muy importante: re-lanzar la excepción para que el Job falle
        raise e
# --------------------------------------------------
# 5. Punto de entrada del script (requerido para Jobs)
# --------------------------------------------------
if __name__ == "__main__":
    main()