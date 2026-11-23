"""
Transformaciones Capa Silver - Lakeflow Spark Declarative Pipelines
ElectroCasa Lakehouse Platform

Limpieza, tipado ANSI, deduplicacion, expectativas de calidad, cuarentena y SCD Tipo 2:
- silver_ventas: deduplicada y tipada (expect_or_drop monto y cantidad > 0)
- silver_catalogo: maestro de productos deduplicado con precios limpios
- silver_sucursales: maestro con casing estandarizado (initcap)
- silver_devoluciones: reembolsos validados
- silver_resenas: calificaciones validas (1 a 5) y tags estructurados
- silver_tracking_envios: estados normalizados
- silver_empleados: historizacion SCD Tipo 2 con Auto CDC y DDM (mask_dni, mask_salario)
- silver_cuarentena: trazabilidad completa de registros rechazados (Reto diferenciador)
"""

import dlt
from pyspark.sql import functions as F

from electrocasa.utils.helpers import (
    clean_currency_amount,
    parse_multiformat_date,
    normalize_payment_method,
    normalize_category,
    normalize_employee_event,
    normalize_delivery_status,
    normalize_courier,
)


# -------------------------------------------------------------------------
# 1. Silver Ventas
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_ventas",
    comment="Ventas limpias y conformadas con expectativas de calidad y deduplicacion",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("venta_id_pk", "venta_id IS NOT NULL")
@dlt.expect_or_drop("monto_positivo", "monto_total IS NOT NULL AND monto_total > 0")
@dlt.expect_or_drop("cantidad_positiva", "cantidad IS NOT NULL AND cantidad > 0")
@dlt.expect_or_drop("sucursal_valida", "sucursal_id IS NOT NULL AND trim(sucursal_id) != ''")
def silver_ventas():
    return (
        spark.readStream.table("bronze_ventas")
        .select(
            F.trim(F.col("venta_id")).alias("venta_id"),
            F.trim(F.col("sucursal_id")).alias("sucursal_id"),
            F.trim(F.col("producto_id")).alias("producto_id"),
            F.when(F.trim(F.col("vendedor_id")) == "", F.lit(None))
            .otherwise(F.trim(F.col("vendedor_id"))).alias("vendedor_id"),
            F.col("cantidad").cast("int").alias("cantidad"),
            clean_currency_amount("monto_total").alias("monto_total"),
            normalize_payment_method("metodo_pago").alias("metodo_pago"),
            parse_multiformat_date("fecha_venta").alias("fecha_venta"),
            F.lower(F.trim(F.col("canal"))).alias("canal"),
            F.col("_metadata_file_name"),
            F.col("_ingested_at")
        )
        .dropDuplicates(["venta_id"])
    )


# -------------------------------------------------------------------------
# 2. Silver Catalogo (Materialized View desde Bronze cargado con COPY INTO)
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_catalogo",
    comment="Maestro de productos deduplicado con normalizacion de categorias y precios",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("producto_id_pk", "producto_id IS NOT NULL")
@dlt.expect_or_drop("precio_positivo", "precio_lista IS NOT NULL AND precio_lista > 0")
@dlt.expect_or_drop("categoria_valida", "categoria IN ('cocina', 'linea_blanca', 'electronica', 'climatizacion', 'entretenimiento')")
def silver_catalogo():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    return (
        spark.read.table(f"{catalog}.bronze.catalogo")
        .select(
            F.trim(F.col("producto_id")).alias("producto_id"),
            F.trim(F.col("nombre_producto")).alias("nombre_producto"),
            normalize_category("categoria").alias("categoria"),
            F.coalesce(F.trim(F.col("marca")), F.lit("Generico")).alias("marca"),
            clean_currency_amount("precio_lista").alias("precio_lista"),
            F.col("activo").cast("boolean").alias("activo"),
            F.col("_ingested_at")
        )
        .dropDuplicates(["producto_id"])
    )


# -------------------------------------------------------------------------
# 3. Silver Sucursales (Materialized View desde Bronze cargado con COPY INTO)
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_sucursales",
    comment="Maestro de sucursales con normalizacion de casing en ciudad y region",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("sucursal_id_pk", "sucursal_id IS NOT NULL")
@dlt.expect_or_fail("ciudad_valida", "ciudad IS NOT NULL AND trim(ciudad) != ''")
def silver_sucursales():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    return (
        spark.read.table(f"{catalog}.bronze.sucursales")
        .select(
            F.trim(F.col("sucursal_id")).alias("sucursal_id"),
            F.trim(F.col("nombre_sucursal")).alias("nombre_sucursal"),
            F.trim(F.col("distrito")).alias("distrito"),
            F.initcap(F.trim(F.col("ciudad"))).alias("ciudad"),
            F.initcap(F.trim(F.col("region"))).alias("region"),
            parse_multiformat_date("fecha_apertura").alias("fecha_apertura"),
            F.col("_ingested_at")
        )
        .dropDuplicates(["sucursal_id"])
    )


# -------------------------------------------------------------------------
# 4. Silver Devoluciones
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_devoluciones",
    comment="Devoluciones validadas y deduplicadas",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("devolucion_id_pk", "devolucion_id IS NOT NULL")
@dlt.expect_or_drop("venta_id_referenciada", "venta_id IS NOT NULL AND trim(venta_id) != ''")
@dlt.expect_or_drop("monto_reembolso_valido", "monto_reembolso IS NOT NULL AND monto_reembolso >= 0")
def silver_devoluciones():
    return (
        spark.readStream.table("bronze_devoluciones")
        .select(
            F.trim(F.col("devolucion_id")).alias("devolucion_id"),
            F.trim(F.col("venta_id")).alias("venta_id"),
            F.trim(F.col("sucursal_id")).alias("sucursal_id"),
            F.trim(F.col("producto_id")).alias("producto_id"),
            F.lower(F.trim(F.col("motivo"))).alias("motivo"),
            clean_currency_amount("monto_reembolso").alias("monto_reembolso"),
            parse_multiformat_date("fecha_devolucion").alias("fecha_devolucion"),
            F.col("_metadata_file_name"),
            F.col("_ingested_at")
        )
        .dropDuplicates(["devolucion_id"])
    )


# -------------------------------------------------------------------------
# 5. Silver Reseñas (con Reto VARIANT: parse_json en metadata)
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_resenas",
    comment="Reseñas con calificaciones en escala valida (1 a 5), tags estructurados y metadata VARIANT",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("resena_id_pk", "resena_id IS NOT NULL")
@dlt.expect_or_drop("calificacion_en_escala", "calificacion >= 1 AND calificacion <= 5")
def silver_resenas():
    return (
        spark.readStream.table("bronze_resenas")
        .select(
            F.trim(F.col("resena_id")).alias("resena_id"),
            F.trim(F.col("producto_id")).alias("producto_id"),
            F.trim(F.col("cliente_id")).alias("cliente_id"),
            F.col("calificacion").cast("int").alias("calificacion"),
            F.trim(F.col("comentario")).alias("comentario"),
            F.col("tags"),
            F.col("respuestas"),
            parse_multiformat_date("fecha_resena").alias("fecha_resena"),
            # Reto técnico diferenciador: transformar JSON string a VARIANT nativo
            F.parse_json(F.col("metadata")).alias("metadata"),
            F.col("_metadata_file_name"),
            F.col("_ingested_at")
        )
        .dropDuplicates(["resena_id"])
    )


# -------------------------------------------------------------------------
# 6. Silver Tracking Envios (Streaming desde conector Lakeflow Connect)
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_tracking_envios",
    comment="Eventos de seguimiento courier normalizados y deduplicados",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_fail("evento_id_pk", "evento_id IS NOT NULL")
@dlt.expect("estado_entrega_valido", "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')")
def silver_tracking_envios():
    catalog = spark.conf.get("catalog_name", "electrocasa_dev")
    return (
        spark.readStream.table(f"{catalog}.bronze.tracking_envios")
        .select(
            F.col("evento_id").cast("int").alias("evento_id"),
            F.trim(F.col("tracking_id")).alias("tracking_id"),
            F.trim(F.col("venta_id")).alias("venta_id"),
            normalize_courier("courier").alias("courier"),
            normalize_delivery_status("estado_entrega").alias("estado_entrega"),
            F.trim(F.col("sucursal_origen")).alias("sucursal_origen"),
            F.coalesce(
                parse_multiformat_date("fecha_actualizacion"),
                F.to_date(F.col("fecha_modificacion"))
            ).alias("fecha_actualizacion"),
            F.col("fecha_modificacion")
        )
        # Eliminar reprocesados idénticos en el mismo envío y estado
        .dropDuplicates(["tracking_id", "estado_entrega", "fecha_modificacion"])
    )


# -------------------------------------------------------------------------
# 7. Silver Empleados - SCD Tipo 2 con Auto CDC y Dynamic Masking
# -------------------------------------------------------------------------

@dlt.view(
    name="silver_empleados_preparados",
    comment="Eventos limpios de RR.HH. para alimentar Auto CDC SCD Tipo 2"
)
def silver_empleados_preparados():
    return (
        spark.readStream.table("bronze_empleados")
        .filter(F.col("id_empleado").isNotNull() & F.col("fecha_evento").isNotNull())
        .select(
            F.trim(F.col("id_empleado")).alias("id_empleado"),
            F.trim(F.col("nombre")).alias("nombre"),
            F.trim(F.col("dni")).alias("dni"),
            F.trim(F.col("email")).alias("email"),
            clean_currency_amount("salario").alias("salario"),
            F.trim(F.col("sucursal_id")).alias("sucursal_id"),
            F.trim(F.col("cargo")).alias("cargo"),
            normalize_employee_event("tipo_evento").alias("tipo_evento"),
            parse_multiformat_date("fecha_evento").alias("fecha_evento"),
            F.coalesce(
                F.to_timestamp(F.col("fecha_registro"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("fecha_registro"))
            ).alias("fecha_registro")
        )
    )


# Target streaming table para SCD Tipo 2
dlt.create_streaming_table(
    name="silver_empleados",
    comment="Historico de colaboradores con SCD Tipo 2",
    table_properties={"quality": "silver"}
)

dlt.create_auto_cdc_flow(
    target="silver_empleados",
    source="silver_empleados_preparados",
    keys=["id_empleado"],
    sequence_by="fecha_evento",
    stored_as_scd_type=2
)


# -------------------------------------------------------------------------
# 8. Silver Cuarentena (Reto Diferenciador: Trazabilidad Completa de Rechazos)
# -------------------------------------------------------------------------
# Se utiliza el patron oficial de Databricks: una streaming table destino con
# flujos @dlt.append_flow independientes para consolidar rechazos por fuente.

dlt.create_streaming_table(
    name="silver_cuarentena",
    comment="Registros rechazados por reglas de calidad con trazabilidad completa de origen y motivo",
    schema="""
        fuente STRING COMMENT 'Nombre de la fuente de datos que origino el rechazo',
        registro_id STRING COMMENT 'Identificador de la fila rechazada',
        archivo_origen STRING COMMENT 'Archivo fisico de origen en el Volume',
        fecha_rechazo TIMESTAMP COMMENT 'Momento en que se registro el descarte',
        motivo_rechazo STRING COMMENT 'Regla de calidad o motivo del descarte',
        payload_raw STRING COMMENT 'Contenido integro de la fila original en formato JSON'
    """,
    table_properties={"quality": "silver"}
)


@dlt.append_flow(target="silver_cuarentena")
def rechazos_ventas_flow():
    return (
        spark.readStream.table("bronze_ventas")
        .filter(
            (clean_currency_amount("monto_total").isNull() | (clean_currency_amount("monto_total") <= 0)) |
            (F.col("cantidad").cast("int").isNull() | (F.col("cantidad").cast("int") <= 0)) |
            (F.col("sucursal_id").isNull() | (F.trim(F.col("sucursal_id")) == ""))
        )
        .select(
            F.lit("ventas").alias("fuente"),
            F.col("venta_id").alias("registro_id"),
            F.col("_metadata_file_name").alias("archivo_origen"),
            F.current_timestamp().alias("fecha_rechazo"),
            F.when(clean_currency_amount("monto_total").isNull() | (clean_currency_amount("monto_total") <= 0), F.lit("monto_invalido_o_negativo"))
            .when(F.col("cantidad").cast("int").isNull() | (F.col("cantidad").cast("int") <= 0), F.lit("cantidad_invalida_o_cero"))
            .when(F.col("sucursal_id").isNull() | (F.trim(F.col("sucursal_id")) == ""), F.lit("sucursal_nula"))
            .otherwise(F.lit("otro_error")).alias("motivo_rechazo"),
            F.to_json(F.struct("*")).alias("payload_raw")
        )
    )


@dlt.append_flow(target="silver_cuarentena")
def rechazos_resenas_flow():
    return (
        spark.readStream.table("bronze_resenas")
        .filter(
            F.col("calificacion").isNull() |
            (F.col("calificacion") < 1) |
            (F.col("calificacion") > 5)
        )
        .select(
            F.lit("resenas").alias("fuente"),
            F.col("resena_id").alias("registro_id"),
            F.col("_metadata_file_name").alias("archivo_origen"),
            F.current_timestamp().alias("fecha_rechazo"),
            F.when(F.col("calificacion").isNull(), F.lit("calificacion_nula"))
            .when(F.col("calificacion") < 1, F.lit("calificacion_menor_a_1"))
            .when(F.col("calificacion") > 5, F.lit("calificacion_mayor_a_5"))
            .otherwise(F.lit("calificacion_invalida")).alias("motivo_rechazo"),
            F.to_json(F.struct("*")).alias("payload_raw")
        )
    )


@dlt.append_flow(target="silver_cuarentena")
def rechazos_devoluciones_flow():
    return (
        spark.readStream.table("bronze_devoluciones")
        .filter(
            clean_currency_amount("monto_reembolso").isNull() |
            (clean_currency_amount("monto_reembolso") < 0) |
            F.col("venta_id").isNull() |
            (F.trim(F.col("venta_id")) == "")
        )
        .select(
            F.lit("devoluciones").alias("fuente"),
            F.col("devolucion_id").alias("registro_id"),
            F.col("_metadata_file_name").alias("archivo_origen"),
            F.current_timestamp().alias("fecha_rechazo"),
            F.when(clean_currency_amount("monto_reembolso").isNull() | (clean_currency_amount("monto_reembolso") < 0), F.lit("monto_reembolso_negativo_o_nulo"))
            .when(F.col("venta_id").isNull() | (F.trim(F.col("venta_id")) == ""), F.lit("venta_id_nula"))
            .otherwise(F.lit("otro_error")).alias("motivo_rechazo"),
            F.to_json(F.struct("*")).alias("payload_raw")
        )
    )

