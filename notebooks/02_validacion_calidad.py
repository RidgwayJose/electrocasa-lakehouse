# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 3: Validación de Calidad, Conteos y Aserciones Finales
# MAGIC 
# MAGIC Este notebook ejecuta la validación automatizada requerida por la rúbrica del proyecto integrador:
# MAGIC 1. Muestra los conteos de registros por tabla en Silver y Gold.
# MAGIC 2. Muestra la cantidad de registros rechazados en la tabla de cuarentena por fuente y motivo.
# MAGIC 3. Falla explícitamente (`raise AssertionError`) si alguna tabla de Gold queda vacía (cero registros).

# COMMAND ----------
# Databricks runtime
from databricks.sdk.runtime import spark, dbutils
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "electrocasa_dev")
catalogo = dbutils.widgets.get("catalogo")

print(f"Iniciando validaciones de calidad sobre catálogo: {catalogo}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Conteos en Capa Silver

# COMMAND ----------
# Lista de tablas Silver del pipeline (esquema silver)
tablas_silver = [
    "ventas",
    "catalogo",
    "sucursales",
    "devoluciones",
    "resenas",
    "tracking_envios",
    "empleados"
]

print("=" * 60)
print(f"CONTEOS CAPA SILVER — CATÁLOGO: {catalogo}")
print("=" * 60)

conteos_silver = {}
for tbl in tablas_silver:
    # Intentar leer desde schema silver, fallback a legacy default.silver_{tbl}
    try:
        df = spark.table(f"{catalogo}.silver.{tbl}")
    except Exception:
        try:
            df = spark.table(f"{catalogo}.default.silver_{tbl}")
        except Exception:
            df = spark.table(tbl)
            
    count = df.count()
    conteos_silver[tbl] = count
    print(f"✓ Tabla: silver.{tbl:25} | Filas: {count:,}")

# Asegurar Dynamic Data Masking en Unity Catalog sobre silver.empleados (dni y salario)
try:
    spark.sql(f"ALTER TABLE {catalogo}.silver.empleados ALTER COLUMN dni SET MASK {catalogo}.silver.mask_dni")
    spark.sql(f"ALTER TABLE {catalogo}.silver.empleados ALTER COLUMN salario SET MASK {catalogo}.silver.mask_salario")
    print("✓ Dynamic Data Masking verificado exitosamente en silver.empleados (dni y salario).")
except Exception as e:
    print(f"Info DDM: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Métricas de Registros Rechazados (Cuarentena)

# COMMAND ----------
print("=" * 60)
print("AUDITORÍA DE CUARENTENA Y REGISTROS RECHAZADOS")
print("=" * 60)

try:
    df_cuarentena = spark.table(f"{catalogo}.silver.cuarentena")
except Exception:
    try:
        df_cuarentena = spark.table(f"{catalogo}.default.silver_cuarentena")
    except Exception:
        df_cuarentena = spark.table("silver.cuarentena")

total_rechazos = df_cuarentena.count()
print(f"Total registros en cuarentena: {total_rechazos:,}")

if total_rechazos > 0:
    print("\nDesglose de rechazos por fuente y motivo:")
    (
        df_cuarentena.groupBy("fuente", "motivo_rechazo")
        .agg(F.count("*").alias("cantidad_rechazos"))
        .orderBy(F.desc("cantidad_rechazos"))
        .show(truncate=False)
    )
else:
    print("Sin registros rechazados en esta corrida.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Conteos y Aserciones en Capa Gold (Cero Tablas Vacías)

# COMMAND ----------
tablas_gold = [
    "q1_ventas_netas_sucursal_mes",
    "q2_top_productos_ventas",
    "q2_top_productos_devoluciones",
    "q3_dotacion_activa_mensual",
    "q4_ventas_vendedor_historico",
    "q5_resenas_negativas_categoria",
    "q5_resenas_origen_variant",
    "q6_despachos_courier"
]

print("=" * 60)
print(f"CONTEOS CAPA GOLD Y VALIDACIÓN DE NO-VACÍO — CATÁLOGO: {catalogo}")
print("=" * 60)

tablas_vacias = []
for tbl in tablas_gold:
    try:
        df = spark.table(f"{catalogo}.gold.{tbl}")
    except Exception:
        try:
            df = spark.table(f"{catalogo}.default.gold_{tbl}")
        except Exception:
            df = spark.table(tbl)
            
    count = df.count()
    print(f"✓ Tabla Gold: gold.{tbl:32} | Filas: {count:,}")
    
    if count == 0:
        tablas_vacias.append(tbl)
    
    if count == 0:
        tablas_vacias.append(tbl)

print("=" * 60)
if tablas_vacias:
    error_msg = f"ERROR CRÍTICO: Las siguientes tablas Gold quedaron vacías: {tablas_vacias}"
    print(f"❌ {error_msg}")
    raise AssertionError(error_msg)
else:
    print("✅ TODAS LAS TABLAS GOLD CONTIENEN DATOS. VALIDACIÓN EXITOSA.")
