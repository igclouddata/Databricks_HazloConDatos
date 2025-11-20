# 00_config.py

# Ajusta esto al catálogo que estés usando
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
