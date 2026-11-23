"""
Transformaciones Capa Gold - Lakeflow Spark Declarative Pipelines
ElectroCasa Lakehouse Platform

Modelado analitico y agregaciones de negocio para responder las 6 preguntas clave (periodo marzo-octubre 2026):
1. gold_q1_ventas_netas_sucursal_mes: Facturacion neta (ventas - devoluciones), ticket promedio por sucursal y mes.
2. gold_q2_top_productos: Top 10 mas vendidos y Top 10 mayor tasa de devolucion.
3. gold_q3_dotacion_activa: Cantidad de empleados activos por sucursal al ultimo dia de cada mes (SCD2).
4. gold_q4_ventas_vendedor_historico: Ventas de tienda fisica segun la sucursal historica del vendedor (as-of join).
5. gold_q5_resenas_negativas: Tasa de resenas negativas (1-2 estrellas) por categoria de catalogo y mes.
6. gold_q6_despachos_courier: Tiempo promedio de entrega en horas y tasa de devolucion por courier.
"""

import dlt
from pyspark.sql import functions as F


# -------------------------------------------------------------------------
# Pregunta 1: Ventas Netas por Sucursal y Mes
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q1_ventas_netas_sucursal_mes",
    comment="Q1: Ventas brutas, devoluciones, ventas netas y ticket promedio por sucursal y mes (marzo a octubre 2026)",
    table_properties={"quality": "gold"}
)
def gold_q1_ventas_netas_sucursal_mes():
    ventas = (
        spark.read.table("silver_ventas")
        .filter((F.col("fecha_venta") >= "2026-03-01") & (F.col("fecha_venta") <= "2026-10-31"))
        .withColumn("anio_mes", F.date_format(F.col("fecha_venta"), "yyyy-MM"))
        .groupBy("sucursal_id", "anio_mes")
        .agg(
            F.sum("monto_total").cast("decimal(14,2)").alias("ventas_brutas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )
    
    devoluciones = (
        spark.read.table("silver_devoluciones")
        .filter((F.col("fecha_devolucion") >= "2026-03-01") & (F.col("fecha_devolucion") <= "2026-10-31"))
        .withColumn("anio_mes", F.date_format(F.col("fecha_devolucion"), "yyyy-MM"))
        .groupBy("sucursal_id", "anio_mes")
        .agg(
            F.sum("monto_reembolso").cast("decimal(14,2)").alias("total_devoluciones")
        )
    )
    
    # Outer join para capturar sucursales que tengan ventas o devoluciones en el mes
    return (
        ventas.join(devoluciones, ["sucursal_id", "anio_mes"], "left")
        .select(
            F.col("sucursal_id"),
            F.col("anio_mes"),
            F.coalesce(F.col("ventas_brutas"), F.lit(0.00)).alias("ventas_brutas"),
            F.coalesce(F.col("total_devoluciones"), F.lit(0.00)).alias("total_devoluciones"),
            (F.coalesce(F.col("ventas_brutas"), F.lit(0.00)) - F.coalesce(F.col("total_devoluciones"), F.lit(0.00))).cast("decimal(14,2)").alias("ventas_netas"),
            F.col("cantidad_ventas"),
            F.round(F.col("ventas_brutas") / F.col("cantidad_ventas"), 2).cast("decimal(12,2)").alias("ticket_promedio")
        )
    )


# -------------------------------------------------------------------------
# Pregunta 2: Top Productos (Mas Vendidos y Mayor Tasa Devolucion)
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q2_top_productos_ventas",
    comment="Q2: Top productos mas vendidos por unidades totales en el periodo",
    table_properties={"quality": "gold"}
)
def gold_q2_top_productos_ventas():
    return (
        spark.read.table("silver_ventas")
        .filter((F.col("fecha_venta") >= "2026-03-01") & (F.col("fecha_venta") <= "2026-10-31"))
        .groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.countDistinct("venta_id").alias("cantidad_transacciones")
        )
        .join(spark.read.table("silver_catalogo").select("producto_id", "nombre_producto", "categoria"), "producto_id", "left")
        .orderBy(F.desc("unidades_vendidas"))
        .limit(10)
    )


@dlt.table(
    name="gold_q2_top_productos_devoluciones",
    comment="Q2: Top productos con mayor tasa de devolucion (devoluciones / ventas)",
    table_properties={"quality": "gold"}
)
def gold_q2_top_productos_devoluciones():
    ventas_prod = (
        spark.read.table("silver_ventas")
        .filter((F.col("fecha_venta") >= "2026-03-01") & (F.col("fecha_venta") <= "2026-10-31"))
        .groupBy("producto_id")
        .agg(F.countDistinct("venta_id").alias("total_ventas"))
    )
    
    devs_prod = (
        spark.read.table("silver_devoluciones")
        .filter((F.col("fecha_devolucion") >= "2026-03-01") & (F.col("fecha_devolucion") <= "2026-10-31"))
        .groupBy("producto_id")
        .agg(F.countDistinct("devolucion_id").alias("total_devoluciones"))
    )
    
    return (
        ventas_prod.join(devs_prod, "producto_id", "inner")
        .join(spark.read.table("silver_catalogo").select("producto_id", "nombre_producto", "categoria"), "producto_id", "left")
        .withColumn(
            "tasa_devolucion",
            F.round(F.col("total_devoluciones") / F.col("total_ventas"), 4)
        )
        .orderBy(F.desc("tasa_devolucion"))
        .limit(10)
    )


# -------------------------------------------------------------------------
# Pregunta 3: Dotacion Activa por Sucursal al Cierre de Cada Mes (SCD2)
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q3_dotacion_activa_mensual",
    comment="Q3: Cantidad de colaboradores activos al ultimo dia de cada mes segun SCD Tipo 2",
    table_properties={"quality": "gold"}
)
def gold_q3_dotacion_activa_mensual():
    # Fechas de corte exactas al último día de cada mes (marzo a octubre 2026)
    cortes_df = spark.createDataFrame([
        ("2026-03", "2026-03-31"),
        ("2026-04", "2026-04-30"),
        ("2026-05", "2026-05-31"),
        ("2026-06", "2026-06-30"),
        ("2026-07", "2026-07-31"),
        ("2026-08", "2026-08-31"),
        ("2026-09", "2026-09-30"),
        ("2026-10", "2026-10-31"),
    ], ["anio_mes", "fecha_corte"]).withColumn("fecha_corte", F.to_date("fecha_corte"))
    
    # Silver empleados SCD2 con __START_AT y __END_AT generados por Auto CDC
    empleados_scd2 = spark.read.table("silver_empleados")
    
    # Point-in-time join para determinar vigencia al cierre del mes
    # Un empleado está activo si fecha_corte >= __START_AT y (fecha_corte < __END_AT o __END_AT es null)
    # y el evento vigente no es 'baja'
    return (
        cortes_df.crossJoin(empleados_scd2)
        .filter(
            (F.col("fecha_corte") >= F.to_date(F.col("__START_AT"))) &
            (F.col("__END_AT").isNull() | (F.col("fecha_corte") < F.to_date(F.col("__END_AT")))) &
            (F.lower(F.col("tipo_evento")) != "baja")
        )
        .groupBy("sucursal_id", "anio_mes", "fecha_corte")
        .agg(
            F.countDistinct("id_empleado").alias("colaboradores_activos")
        )
        .orderBy("anio_mes", "sucursal_id")
    )


# -------------------------------------------------------------------------
# Pregunta 4: Ventas por Vendedor segun Sucursal Historica (As-Of Join SCD2)
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q4_ventas_vendedor_historico",
    comment="Q4: Ventas en tienda fisica asignadas a la sucursal donde laboraba el vendedor en la fecha de la transaccion",
    table_properties={"quality": "gold"}
)
def gold_q4_ventas_vendedor_historico():
    ventas_tienda = (
        spark.read.table("silver_ventas")
        .filter(
            (F.col("canal") == "tienda_fisica") &
            (F.col("vendedor_id").isNotNull()) &
            (F.col("fecha_venta") >= "2026-03-01") &
            (F.col("fecha_venta") <= "2026-10-31")
        )
        .withColumn("anio_mes", F.date_format(F.col("fecha_venta"), "yyyy-MM"))
    )
    
    empleados_scd2 = (
        spark.read.table("silver_empleados")
        .select(
            F.col("id_empleado"),
            F.col("sucursal_id").alias("sucursal_historica"),
            F.col("__START_AT").alias("start_date"),
            F.coalesce(F.col("__END_AT"), F.to_date(F.lit("9999-12-31"))).alias("end_date")
        )
    )
    
    # As-of join: fecha_venta entre start_date y end_date
    asignadas = (
        ventas_tienda.join(
            empleados_scd2,
            (ventas_tienda.vendedor_id == empleados_scd2.id_empleado) &
            (ventas_tienda.fecha_venta >= empleados_scd2.start_date) &
            (ventas_tienda.fecha_venta < empleados_scd2.end_date),
            "left"
        )
        .withColumn(
            "sucursal_efectiva",
            F.coalesce(F.col("sucursal_historica"), F.lit("SIN_ASIGNAR"))
        )
    )
    
    return (
        asignadas.groupBy("vendedor_id", "sucursal_efectiva", "anio_mes")
        .agg(
            F.sum("monto_total").cast("decimal(14,2)").alias("monto_ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
        .orderBy("anio_mes", "sucursal_efectiva", F.desc("monto_ventas"))
    )


# -------------------------------------------------------------------------
# Pregunta 5: Reseñas Negativas por Categoria de Producto y Mes
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q5_resenas_negativas_categoria",
    comment="Q5: Porcentaje de resenas negativas (1 o 2 estrellas) por categoria oficial del catalogo y mes",
    table_properties={"quality": "gold"}
)
def gold_q5_resenas_negativas_categoria():
    resenas = (
        spark.read.table("silver_resenas")
        .filter((F.col("fecha_resena") >= "2026-03-01") & (F.col("fecha_resena") <= "2026-10-31"))
        .withColumn("anio_mes", F.date_format(F.col("fecha_resena"), "yyyy-MM"))
    )
    
    catalogo = spark.read.table("silver_catalogo").select("producto_id", "categoria")
    
    # Inner join con catálogo para descartar reseñas de productos no existentes
    resenas_con_cat = resenas.join(catalogo, "producto_id", "inner")
    
    return (
        resenas_con_cat.groupBy("categoria", "anio_mes")
        .agg(
            F.count("resena_id").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion").isin([1, 2]), 1).otherwise(0)).alias("resenas_negativas"),
        )
        .withColumn(
            "tasa_negativas",
            F.round(F.col("resenas_negativas") / F.col("total_resenas"), 4)
        )
        .orderBy("anio_mes", "categoria")
    )


@dlt.table(
    name="gold_q5_resenas_origen_variant",
    comment="Reto Tecnico (+0.5 pts): Analisis de resenas negativas por origen y version de app usando tipo VARIANT",
    table_properties={"quality": "gold"}
)
def gold_q5_resenas_origen_variant():
    resenas = (
        spark.read.table("silver_resenas")
        .filter((F.col("fecha_resena") >= "2026-03-01") & (F.col("fecha_resena") <= "2026-10-31"))
        .withColumn("anio_mes", F.date_format(F.col("fecha_resena"), "yyyy-MM"))
        # Extraer atributos semi-estructurados navegando con ':' sobre la columna VARIANT con sintaxis SQL nativa
        .withColumn("canal_origen", F.coalesce(F.expr("metadata:origen::string"), F.lit("desconocido")))
        .withColumn("version_app", F.expr("metadata:app.version::string"))
        .withColumn("so_dispositivo", F.expr("metadata:app.so::string"))
    )
    
    return (
        resenas.groupBy("canal_origen", "version_app", "so_dispositivo", "anio_mes")
        .agg(
            F.count("resena_id").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion").isin([1, 2]), 1).otherwise(0)).alias("resenas_negativas"),
            F.round(F.avg("calificacion"), 2).alias("calificacion_promedio")
        )
        .withColumn(
            "tasa_negativas",
            F.round(F.col("resenas_negativas") / F.col("total_resenas"), 4)
        )
        .orderBy("anio_mes", "canal_origen", F.desc("total_resenas"))
    )


# -------------------------------------------------------------------------
# Pregunta 6: Indicadores de Despacho por Courier
# -------------------------------------------------------------------------

@dlt.table(
    name="gold_q6_despachos_courier",
    comment="Q6: Horas promedio de entrega y tasa de devolucion por courier sobre envios finalizados",
    table_properties={"quality": "gold"}
)
def gold_q6_despachos_courier():
    tracking = spark.read.table("silver_tracking_envios")
    
    # 1. Tiempos entre 'pendiente' y 'entregado'
    primer_pendiente = (
        tracking.filter(F.col("estado_entrega") == "pendiente")
        .groupBy("tracking_id", "courier")
        .agg(F.min("fecha_modificacion").alias("ts_pendiente"))
    )
    
    primer_entregado = (
        tracking.filter(F.col("estado_entrega") == "entregado")
        .groupBy("tracking_id")
        .agg(F.min("fecha_modificacion").alias("ts_entregado"))
    )
    
    tiempos = (
        primer_pendiente.join(primer_entregado, "tracking_id", "inner")
        .withColumn(
            "horas_entrega",
            (F.col("ts_entregado").cast("long") - F.col("ts_pendiente").cast("long")) / 3600.0
        )
        .filter(F.col("horas_entrega") >= 0)
        .groupBy("courier")
        .agg(
            F.round(F.avg("horas_entrega"), 2).alias("horas_promedio_entrega"),
            F.count("tracking_id").alias("envios_entregados")
        )
    )
    
    # 2. Tasa de devoluciones sobre envíos finalizados (entregados o devueltos)
    estados_finales = (
        tracking.filter(F.col("estado_entrega").isin(["entregado", "devuelto"]))
        .groupBy("tracking_id", "courier")
        .agg(
            F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("es_devuelto"),
            F.max(F.when(F.col("estado_entrega") == "entregado", 1).otherwise(0)).alias("es_entregado")
        )
        .groupBy("courier")
        .agg(
            F.count("tracking_id").alias("total_envios_finalizados"),
            F.sum("es_devuelto").alias("total_envios_devueltos")
        )
        .withColumn(
            "tasa_devolucion",
            F.round(F.col("total_envios_devueltos") / F.col("total_envios_finalizados"), 4)
        )
    )
    
    return (
        tiempos.join(estados_finales, "courier", "outer")
        .select(
            F.col("courier"),
            F.col("horas_promedio_entrega"),
            F.col("envios_entregados"),
            F.col("total_envios_finalizados"),
            F.col("total_envios_devueltos"),
            F.col("tasa_devolucion")
        )
        .orderBy("courier")
    )
