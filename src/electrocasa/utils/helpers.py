"""Funciones auxiliares y expresiones de PySpark para limpieza y normalización.

ElectroCasa Lakehouse Platform
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def clean_currency_amount(col_name: str) -> Column:
    """Limpia cadenas de texto con símbolos de moneda ('S/'), puntos de miles y comas decimales.
    
    Convierte formatos como 'S/ 1,299.90', '1.234,50', ' 500.00 ' a Decimal(12, 2).
    """
    col = F.col(col_name).cast("string")
    # Remover espacios, símbolo de moneda 'S/' y 'S/.'
    cleaned = F.regexp_replace(col, r"[S\/\$\s]", "")
    
    # Manejar caso peruano/europeo '1.234,50' (punto miles, coma decimal)
    # Si contiene coma y punto, asumimos punto como miles y coma como decimal:
    is_comma_decimal = cleaned.rlike(r"^\d{1,3}(\.\d{3})+,\d{2}$") | cleaned.rlike(r"^\d+,\d{2}$")
    
    standardized = F.when(
        is_comma_decimal,
        F.translate(F.translate(cleaned, ".", ""), ",", ".")
    ).otherwise(
        # Caso estándar '1,234.50': remover comas de miles
        F.regexp_replace(cleaned, ",", "")
    )
    
    return standardized.cast("decimal(12,2)")


def parse_multiformat_date(col_name: str) -> Column:
    """Parsea fechas que pueden llegar en múltiples formatos sin fallar bajo modo ANSI:
    - 'yyyy-MM-dd' (ej. 2026-06-07)
    - 'dd/MM/yyyy' (ej. 07/06/2026)
    - 'yyyy-MM-ddTHH:mm:ss' (ej. 2026-06-07T14:30:00)
    - 'yyyy/MM/dd'
    """
    col = F.col(col_name).cast("string")
    col_trimmed = F.trim(col)
    
    # Extraer los primeros 10 caracteres si viene con timestamp ISO
    iso_prefix = F.substring(col_trimmed, 1, 10)
    
    return F.coalesce(
        F.to_date(col_trimmed, "yyyy-MM-dd"),
        F.to_date(col_trimmed, "dd/MM/yyyy"),
        F.to_date(iso_prefix, "yyyy-MM-dd"),
        F.to_date(col_trimmed, "yyyy/MM/dd"),
        F.to_date(col_trimmed, "dd-MM-yyyy")
    )


def normalize_payment_method(col_name: str) -> Column:
    """Estandariza las diversas variantes de método de pago a un catálogo cerrado:
    - 'tarjeta_credito'
    - 'tarjeta_debito'
    - 'efectivo'
    - 'billetera_digital'
    - 'transferencia'
    """
    val = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(val.rlike(r"(tc|credito|crédito|visa credit|mastercard credit)"), F.lit("tarjeta_credito"))
        .when(val.rlike(r"(td|debito|débito|visa debit)"), F.lit("tarjeta_debito"))
        .when(val.rlike(r"(tarjeta|card)"), F.lit("tarjeta_credito")) # Default si no especifica
        .when(val.rlike(r"(efv|efectivo|cash)"), F.lit("efectivo"))
        .when(val.rlike(r"(yape|plin|billetera|wallet)"), F.lit("billetera_digital"))
        .when(val.rlike(r"(transf|transferencia|banco)"), F.lit("transferencia"))
        .otherwise(F.lit("otro"))
    )


def normalize_category(col_name: str) -> Column:
    """Estandariza las variantes de categorías de productos al catálogo oficial:
    - 'linea_blanca'
    - 'cocina'
    - 'electronica'
    - 'climatizacion'
    - 'entretenimiento'
    """
    val = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(val.rlike(r"(blanca|lavado|refrig)"), F.lit("linea_blanca"))
        .when(val.rlike(r"(cocina|microondas|horno)"), F.lit("cocina"))
        .when(val.rlike(r"(electr|celular|computo|laptop|audio)"), F.lit("electronica"))
        .when(val.rlike(r"(clima|aire|ventilador|calefac)"), F.lit("climatizacion"))
        .when(val.rlike(r"(entreten|tv|tele|televis|video)"), F.lit("entretenimiento"))
        .otherwise(val)
    )


def normalize_employee_event(col_name: str) -> Column:
    """Estandariza los tipos de eventos de RR.HH. a los 4 permitidos:
    - 'alta'
    - 'transferencia'
    - 'cambio_salario'
    - 'baja' (incluye cese)
    """
    val = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(val.rlike(r"(alta|ingreso|inicio)"), F.lit("alta"))
        .when(val.rlike(r"(transf|traslado|cambio sucursal|cambio_sucursal)"), F.lit("transferencia"))
        .when(val.rlike(r"(salario|sueldo|remuneracion|aumento)"), F.lit("cambio_salario"))
        .when(val.rlike(r"(baja|cese|despido|renuncia|salida)"), F.lit("baja"))
        .otherwise(val)
    )


def normalize_delivery_status(col_name: str) -> Column:
    """Estandariza el estado de entrega del tracking:
    - 'pendiente'
    - 'en_transito'
    - 'entregado'
    - 'devuelto'
    """
    val = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(val.rlike(r"(pend|prepar|asign)"), F.lit("pendiente"))
        .when(val.rlike(r"(transito|tránsito|camino|ruta)"), F.lit("en_transito"))
        .when(val.rlike(r"(entreg|finaliz|recibido)"), F.lit("entregado"))
        .when(val.rlike(r"(devuel|retorn|fallid|cancel)"), F.lit("devuelto"))
        .otherwise(val)
    )


def normalize_courier(col_name: str) -> Column:
    """Estandariza las variantes de nombres de courier al catálogo cerrado oficial:
    - 'Shalom'
    - 'Servientrega'
    - 'Olva Courier' (unifica 'Olva' y 'Olva Courier')
    """
    val = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(val.rlike(r"olva"), F.lit("Olva Courier"))
        .when(val.rlike(r"shalom"), F.lit("Shalom"))
        .when(val.rlike(r"servientrega"), F.lit("Servientrega"))
        .otherwise(F.initcap(val))
    )


# Alias compatibles para importaciones
normalizar_fecha = parse_multiformat_date
normalizar_monto = clean_currency_amount
limpiar_texto = lambda col_name: F.trim(F.col(col_name))

