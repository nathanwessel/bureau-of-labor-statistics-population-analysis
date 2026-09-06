# Shared catalog and schema configuration for the Rearc pipeline.

CATALOG_NAME = "rearc"

BRONZE_SCHEMA_SHORT_NAME = "bronze"
SILVER_SCHEMA_SHORT_NAME = "silver"
GOLD_SCHEMA_SHORT_NAME = "gold"


# Prebuilt namespaces keep table declarations concise throughout the project.
BRONZE_SCHEMA = f"{CATALOG_NAME}.{BRONZE_SCHEMA_SHORT_NAME}"
SILVER_SCHEMA = f"{CATALOG_NAME}.{SILVER_SCHEMA_SHORT_NAME}"
GOLD_SCHEMA = f"{CATALOG_NAME}.{GOLD_SCHEMA_SHORT_NAME}"