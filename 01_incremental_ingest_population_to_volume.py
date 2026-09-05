# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📥 DataUSA.io Population Raw Ingestion
# MAGIC
# MAGIC Incremental mirror of the DataUSA.io population API into:
# MAGIC `/Volumes/rearc/bronze/raw_source_data/data_usa_io/population/`
# MAGIC
# MAGIC The notebook:
# MAGIC - fetches the full population response from the DataUSA.io API
# MAGIC - validates the response is valid JSON
# MAGIC - saves each fetch as its own immutable timestamped snapshot
# MAGIC - never overwrites or deletes prior snapshots
# MAGIC
# MAGIC **Prerequisites:** Run [00_UC_Schemas_and_Volumes_Setup](#notebook-2668398073890001) first to create the UC infrastructure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ℹ️ Prerequisites
# MAGIC
# MAGIC **Before running this notebook**, ensure the Unity Catalog infrastructure exists by running:
# MAGIC [00_UC_Schemas_and_Volumes_Setup](#notebook-2668398073890001)
# MAGIC
# MAGIC This creates the `rearc` catalog, medallion schemas, and the raw data volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 1. Configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
# UTC timestamps so fetch events are unambiguous across time zones.
from datetime import datetime, timezone
# File system operations for creating directories.
import os

# HTTP client for API calls.
import requests

# COMMAND ----------

# DBTITLE 1,Define constants
# DataUSA.io population API endpoint
API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)

# All raw artifacts live inside a UC Volume so they are governed by UC
OUTPUT_DIR = "/Volumes/rearc/bronze/raw_source_data/data_usa_io/population/"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 2. Execute ingestion

# COMMAND ----------

# DBTITLE 1,Create raw landing folder
# create the raw landing folder if it does not already exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Fetch API response
# Fetch the full response from the population API
fetched_at_utc = datetime.now(timezone.utc)

try:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    # Validate that the response body is valid JSON before saving it.
    response.json()
except requests.exceptions.RequestException as e:
    print(f"HTTP request failed: {e}")
    raise
except ValueError as e:
    print(f"Response is not valid JSON: {e}")
    raise


# COMMAND ----------

# DBTITLE 1,Save immutable timestamped snapshot
# Save every API fetch as its own immutable raw JSON snapshot.
#
# Example:
# population__fetched_2026-09-02__22_31_45_UTC.json
#
# This intentionally does not overwrite or delete prior snapshots.
# If the upstream source adds, changes, removes, or returns identical data,
# this API call is still preserved as a distinct raw fetch event.

timestamp = fetched_at_utc.strftime("%Y-%m-%d__%H_%M_%S_UTC")
file_name = f"population__fetched_{timestamp}.json"
output_file = os.path.join(OUTPUT_DIR, file_name)

# "xb" creates a new binary file and fails if the file already exists.
# This protects the raw landing layer from accidental overwrites.
with open(output_file, "xb") as f:
    f.write(response.content)

# COMMAND ----------

# DBTITLE 1,Display run summary
print(f"Fetched at (UTC): {fetched_at_utc.isoformat()}")
print(f"Saved raw response to:\n {output_file}")