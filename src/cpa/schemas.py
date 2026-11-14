"""Esquemas explícitos de las 8 fuentes de Landing (docs/diccionario_datos.md).

Nunca se infiere el esquema: si una fuente cambia, el cambio aparece como nulos o como `_corrupt_record`
en lugar de cambiar los tipos en silencio. Cada esquema termina en `_corrupt_record`, la columna donde
Spark deja la línea original cuando no la puede interpretar (modo PERMISSIVE).
"""
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

CORRUPT_COL = "_corrupt_record"


def _schema(*fields):
    return StructType([StructField(name, dtype, True) for name, dtype in fields]
                      + [StructField(CORRUPT_COL, StringType(), True)])


CUSTOMERS_ORGS = _schema(
    ("org_id", StringType()), ("org_name", StringType()), ("industry", StringType()),
    ("hq_region", StringType()), ("plan_tier", StringType()), ("is_enterprise", BooleanType()),
    ("signup_date", DateType()), ("sales_rep", StringType()), ("lifecycle_stage", StringType()),
    ("marketing_source", StringType()), ("nps_score", DoubleType()))

USERS = _schema(
    ("user_id", StringType()), ("org_id", StringType()), ("email", StringType()), ("role", StringType()),
    ("active", BooleanType()), ("created_at", DateType()), ("last_login", DateType()))

RESOURCES = _schema(
    ("resource_id", StringType()), ("org_id", StringType()), ("service", StringType()),
    ("region", StringType()), ("created_at", DateType()), ("state", StringType()),
    ("tags_json", StringType()))   # arreglo JSON serializado; se parsea en Silver

SUPPORT_TICKETS = _schema(
    ("ticket_id", StringType()), ("org_id", StringType()), ("category", StringType()),
    ("severity", StringType()), ("created_at", DateType()), ("resolved_at", DateType()),
    ("csat", DoubleType()), ("sla_breached", BooleanType()))

MARKETING_TOUCHES = _schema(
    ("touch_id", StringType()), ("org_id", StringType()), ("campaign", StringType()),
    ("channel", StringType()), ("timestamp", DateType()),   # llega solo con fecha
    ("clicked", BooleanType()), ("converted", BooleanType()))

NPS_SURVEYS = _schema(
    ("org_id", StringType()), ("survey_date", DateType()), ("nps_score", DoubleType()),
    ("comment", StringType()))

BILLING_MONTHLY = _schema(
    ("invoice_id", StringType()), ("org_id", StringType()), ("month", DateType()),
    ("subtotal", DoubleType()), ("credits", DoubleType()), ("taxes", DoubleType()),
    ("currency", StringType()), ("exchange_rate_to_usd", DoubleType()))

# Eventos: un solo esquema con los campos de v1 y v2 (en v1, carbon_kg y genai_tokens quedan nulos, D14).
# `value` es texto en Bronze porque llega mezclado (número, texto o nulo) y se convierte en Silver (D13).
USAGE_EVENTS = _schema(
    ("event_id", StringType()), ("timestamp", TimestampType()), ("org_id", StringType()),
    ("resource_id", StringType()), ("service", StringType()), ("region", StringType()),
    ("metric", StringType()), ("value", StringType()), ("unit", StringType()),
    ("cost_usd_increment", DoubleType()), ("schema_version", IntegerType()),
    ("carbon_kg", DoubleType()), ("genai_tokens", LongType()))

# Fuentes CSV de Landing: nombre -> (archivo, esquema, clave natural).
CSV_SOURCES = {
    "customers_orgs": ("customers_orgs.csv", CUSTOMERS_ORGS, ["org_id"]),
    "users": ("users.csv", USERS, ["user_id"]),
    "resources": ("resources.csv", RESOURCES, ["resource_id"]),
    "support_tickets": ("support_tickets.csv", SUPPORT_TICKETS, ["ticket_id"]),
    "marketing_touches": ("marketing_touches.csv", MARKETING_TOUCHES, ["touch_id"]),
    "nps_surveys": ("nps_surveys.csv", NPS_SURVEYS, ["org_id", "survey_date"]),
    "billing_monthly": ("billing_monthly.csv", BILLING_MONTHLY, ["invoice_id"]),
}

EVENTS_DIR = "usage_events_stream"
