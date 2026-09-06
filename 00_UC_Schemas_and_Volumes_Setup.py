# Databricks notebook source
# DBTITLE 1,UC Catalog, Schemas, and Volume Setup
# MAGIC %md
# MAGIC # 🏗️ UC Catalog, Schemas, and Volume Setup
# MAGIC
# MAGIC This notebook creates the Unity Catalog infrastructure for the BLS productivity analysis project:
# MAGIC - Catalog: `rearc`
# MAGIC - Schemas: `bronze`, `silver`, `gold` (medallion architecture)
# MAGIC - Volume: `rearc.bronze.raw_source_data` (for raw BLS files)
# MAGIC
# MAGIC These statements are idempotent, so re-running the notebook is safe.
# MAGIC
# MAGIC **Run this notebook before any data ingestion notebooks.**

# COMMAND ----------

# DBTITLE 1,Create UC objects
spark.sql("CREATE CATALOG IF NOT EXISTS rearc")
spark.sql("CREATE SCHEMA IF NOT EXISTS rearc.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS rearc.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS rearc.gold")
spark.sql("CREATE VOLUME IF NOT EXISTS rearc.bronze.raw_source_data")