"""
Transformaciones Capa Bronze - Lakeflow Spark Declarative Pipelines
ElectroCasa Lakehouse Platform

Ingesta raw de fuentes continuas/lotes hacia streaming tables con Auto Loader y metadatos tecnicos:
- Ventas (CSV)
- Empleados (CSV)
- Reseñas (JSON Lines)
- Devoluciones (CSV)

Pautas Arquitecturales:
- Schemas explicitos para fuentes CSV (prohibido inferSchema).
- Auditoria mediante _metadata.file_name (prohibido input_file_name()).
- Sin logica de negocio ni filtros en Bronze.
- Checkpoint y schema location gestionados automaticamente por el pipeline DLT.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# -------------------------------------------------------------------------
# Schemas Explicitos para Fuentes CSV
# -------------------------------------------------------------------------

SCHEMA_VENTAS_CSV = StructType([
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("vendedor_id", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("monto_total", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("fecha_venta", StringType(), True),
    StructField("canal", StringType(), True),
])

SCHEMA_EMPLEADOS_CSV = StructType([
    StructField("id_empleado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("dni", StringType(), True),
    StructField("email", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("fecha_evento", StringType(), True),
    StructField("fecha_registro", StringType(), True),
])

SCHEMA_DEVOLUCIONES_CSV = StructType([
    StructField("devolucion_id", StringType(), True),
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("monto_reembolso", StringType(), True),
    StructField("fecha_devolucion", StringType(), True),
])

SCHEMA_RESENAS_JSON = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", IntegerType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField(
        "respuestas",
        ArrayType(
            StructType([
                StructField("autor", StringType(), True),
                StructField("texto", StringType(), True),
                StructField("fecha", StringType(), True),
            ])
        ),
        True,
    ),
    StructField("fecha_resena", StringType(), True),
    StructField("metadata", StringType(), True),
])


# -------------------------------------------------------------------------
# Streaming Tables Bronze
# -------------------------------------------------------------------------

@dlt.table(
    name="bronze_ventas",
    comment="Ingesta raw de ventas por lotes mediante Auto Loader (CSV)",
    table_properties={"quality": "bronze"}
)
def bronze_ventas():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    landing_path = f"/Volumes/{catalog}/default/vol_landing/ventas/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(SCHEMA_VENTAS_CSV)
        .load(landing_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_metadata_file_name"),
            F.current_timestamp().alias("_ingested_at")
        )
    )


@dlt.table(
    name="bronze_empleados",
    comment="Ingesta raw de eventos de RR.HH. por lotes mediante Auto Loader (CSV)",
    table_properties={"quality": "bronze"}
)
def bronze_empleados():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    landing_path = f"/Volumes/{catalog}/default/vol_landing/empleados/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(SCHEMA_EMPLEADOS_CSV)
        .load(landing_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_metadata_file_name"),
            F.current_timestamp().alias("_ingested_at")
        )
    )


@dlt.table(
    name="bronze_resenas",
    comment="Ingesta raw de reseñas anidadas mediante Auto Loader (JSON Lines)",
    table_properties={"quality": "bronze"}
)
def bronze_resenas():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    landing_path = f"/Volumes/{catalog}/default/vol_landing/resenas/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA_RESENAS_JSON)
        .load(landing_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_metadata_file_name"),
            F.current_timestamp().alias("_ingested_at")
        )
    )


@dlt.table(
    name="bronze_devoluciones",
    comment="Ingesta raw de devoluciones por lotes mediante Auto Loader (CSV)",
    table_properties={"quality": "bronze"}
)
def bronze_devoluciones():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    landing_path = f"/Volumes/{catalog}/default/vol_landing/devoluciones/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(SCHEMA_DEVOLUCIONES_CSV)
        .load(landing_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_metadata_file_name"),
            F.current_timestamp().alias("_ingested_at")
        )
    )
