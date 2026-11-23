# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 1: Ingesta Batch con COPY INTO (Fuentes Estáticas y Snapshots)
# MAGIC 
# MAGIC Este notebook implementa la carga idempotente mediante `COPY INTO` hacia la capa **Bronze**
# MAGIC para las fuentes que llegan como snapshots completos o tablas de referencia estáticas:
# MAGIC 1. **Catálogo de Productos:** Snapshots periódicos en formato JSON multiline.
# MAGIC 2. **Maestro de Sucursales:** Archivo de referencia estático en formato CSV.
# MAGIC 
# MAGIC > **Pauta Arquitectural:**
# MAGIC > `COPY INTO` se ejecuta fuera del Lakeflow Spark Declarative Pipeline sobre tablas Delta comunes,
# MAGIC > permitiendo reejecuciones idempotentes sin duplicar registros.

# COMMAND ----------
# Databricks runtime
from databricks.sdk.runtime import spark, dbutils

dbutils.widgets.text("catalogo", "electrocasa_dev")
catalogo = dbutils.widgets.get("catalogo")

print(f"Iniciando ingesta COPY INTO sobre catálogo: {catalogo}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Ingesta de Catálogo de Productos (`bronze.catalogo`)
# MAGIC Se utiliza `FORMAT_OPTIONS ('multiLine' = 'true', 'primitivesAsString' = 'true')` para procesar el array
# MAGIC multilínea y evitar colisiones de tipos al inferir valores booleanos o nulos.

# COMMAND ----------
# Crear tabla bronze.catalogo si no existe
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.catalogo (
  producto_id STRING COMMENT 'Código de producto',
  nombre_producto STRING COMMENT 'Nombre comercial del producto',
  categoria STRING COMMENT 'Categoría del producto',
  marca STRING COMMENT 'Marca o fabricante',
  precio_lista STRING COMMENT 'Precio de lista en texto raw (puede incluir S/)',
  activo STRING COMMENT 'Estado activo/inactivo raw',
  _ingested_at TIMESTAMP COMMENT 'Timestamp de auditoría de ingesta'
)
USING DELTA
COMMENT 'Tabla Bronze para snapshots de catálogo cargados vía COPY INTO'
""")

# Ejecución idempotente de COPY INTO para catálogo
landing_catalogo = f"/Volumes/{catalogo}/default/vol_landing/catalogo/"

copy_catalogo_query = f"""
COPY INTO {catalogo}.bronze.catalogo
FROM '{landing_catalogo}'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true', 'primitivesAsString' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
"""
print(f"Ejecutando COPY INTO desde: {landing_catalogo}")
res_catalogo = spark.sql(copy_catalogo_query)
res_catalogo.show(truncate=False)

# Actualizar metadato de auditoría _ingested_at para nuevos registros
spark.sql(f"""
UPDATE {catalogo}.bronze.catalogo
SET _ingested_at = current_timestamp()
WHERE _ingested_at IS NULL
""")

conteo_catalogo = spark.table(f"{catalogo}.bronze.catalogo").count()
print(f"Total registros en {catalogo}.bronze.catalogo: {conteo_catalogo}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ingesta de Maestro de Sucursales (`bronze.sucursales`)
# MAGIC Carga batch única/incremental de la tabla de referencia de 40 sucursales.

# COMMAND ----------
# Crear tabla bronze.sucursales si no existe
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.sucursales (
  sucursal_id STRING COMMENT 'Identificador único de sucursal',
  nombre_sucursal STRING COMMENT 'Nombre de la tienda',
  distrito STRING COMMENT 'Distrito de ubicación',
  ciudad STRING COMMENT 'Ciudad (requiere estandarización de casing)',
  region STRING COMMENT 'Región geográfica',
  fecha_apertura STRING COMMENT 'Fecha de apertura raw',
  _ingested_at TIMESTAMP COMMENT 'Timestamp de auditoría de ingesta'
)
USING DELTA
COMMENT 'Tabla Bronze para maestro de sucursales cargado vía COPY INTO'
""")

landing_sucursales = f"/Volumes/{catalogo}/default/vol_landing/sucursales/"

copy_sucursales_query = f"""
COPY INTO {catalogo}.bronze.sucursales
FROM '{landing_sucursales}'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true')
"""
print(f"Ejecutando COPY INTO desde: {landing_sucursales}")
res_sucursales = spark.sql(copy_sucursales_query)
res_sucursales.show(truncate=False)

# Actualizar metadato de auditoría _ingested_at para nuevos registros
spark.sql(f"""
UPDATE {catalogo}.bronze.sucursales
SET _ingested_at = current_timestamp()
WHERE _ingested_at IS NULL
""")

conteo_sucursales = spark.table(f"{catalogo}.bronze.sucursales").count()
print(f"Total registros en {catalogo}.bronze.sucursales: {conteo_sucursales}")
print("Ingesta batch COPY INTO finalizada exitosamente.")
