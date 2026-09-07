# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 📥 BLS Productivity Raw Ingestion
# MAGIC
# MAGIC Daily, incremental mirror of the BLS productivity time-series directory into:
# MAGIC `/Volumes/rearc/bronze/raw_source_data/bls/productivity/`
# MAGIC
# MAGIC The notebook:
# MAGIC - discovers files dynamically from the BLS directory page
# MAGIC - downloads only new or changed files
# MAGIC - deletes local files that BLS removed
# MAGIC - saves the directory HTML as a raw artifact
# MAGIC - keeps an audit manifest in the Volume
# MAGIC
# MAGIC **Prerequisites:** Run [00_UC_Schemas_and_Volumes_Setup](#notebook-2668398073890001) first to create the UC infrastructure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 1. Fetch BLS contact email from Unity Catalog secrets

# COMMAND ----------

bls_contact_email = dbutils.secrets.get(
    catalog="rearc",
    schema="secrets",
    key="bls_contact_email",
)

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
# MAGIC ## ⚙️ 2. Configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
import hashlib
import json
import os
import re
# Rate-limit between BLS downloads per their acceptable-use policy.
import time
# UTC timestamps so manifest events are unambiguous across time zones.
from datetime import datetime, timezone
# Object-oriented path handling for Volume locations.
from pathlib import Path
# Resolve relative hrefs in the directory listing to absolute URLs.
from urllib.parse import urljoin

import requests

# COMMAND ----------

# DBTITLE 1,Define constants
# BLS productivity series directory page
BLS_DIRECTORY_URL = "https://download.bls.gov/pub/time.series/pr/"

# All raw artifacts live inside a UC Volume so they are governed by UC
VOLUME_ROOT = Path("/Volumes/rearc/bronze/raw_source_data")
RAW_DIRECTORY = VOLUME_ROOT / "bls/productivity"

# manifest which records which files exist locally, their content hashes, and a changelog of add/change/remove events
MANIFEST_PATH = VOLUME_ROOT / "_control/bls_productivity_manifest.json"
# The raw HTML of the directory listing is saved verbatim as an audit artifact
# so we can later prove exactly what the source page looked like at run time.
DIRECTORY_LISTING_PATH = RAW_DIRECTORY / "_directory_listing.html"

# add header to comply with https://www.bls.gov/bls/pss.htm
REQUEST_HEADERS = {
    "User-Agent": f"rearc-data-quest/1.0 (contact: {bls_contact_email})"
}

# ensure the target directories exist before any download / manifest write
RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ 3. Define helper functions

# COMMAND ----------

def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def save_json(path, value):
    # Write to a temporary file first so a failed write does not corrupt the manifest.
    temporary_path = Path(f"{path}.tmp")
    temporary_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary_path, path)


def get_bls(url):
    response = requests.get(
        url,
        headers=REQUEST_HEADERS,
        timeout=30,
    )

    if response.status_code == 403:
        raise RuntimeError(
            "BLS returned 403 Forbidden. Check that bls_contact_email is a real "
            "contact email and that your Databricks workspace has outbound internet access."
        )

    response.raise_for_status()
    return response


# The BLS page is a simple directory listing. Each file row contains:
# modified timestamp, file size, and then the file link.
FILE_ROW_PATTERN = re.compile(
    r'(?P<modified>\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)'
    r'\s+(?P<size_bytes>\d+)'
    r'\s+<a\s+href="(?P<href>[^"]+)">(?P<file_name>[^<]+)</a>',
    re.IGNORECASE,
)


def discover_files(directory_html):
    discovered = {}

    for match in FILE_ROW_PATTERN.finditer(directory_html):
        file_name = match.group("file_name").strip()
        source_url = urljoin(BLS_DIRECTORY_URL, match.group("href"))

        discovered[file_name] = {
            "source_url": source_url,
            "source_last_modified": match.group("modified").strip(),
            "source_size_bytes": int(match.group("size_bytes")),
        }

    if not discovered:
        raise ValueError("No BLS files were discovered from the directory listing.")

    return discovered


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 4. Load previous manifest

# COMMAND ----------

if MANIFEST_PATH.exists():
    print(f"Manifest already existing. Loading now.")
    manifest = json.loads(MANIFEST_PATH.read_text())
    print("Loaded manifest.")
else:
    manifest = {
        "source_directory_url": BLS_DIRECTORY_URL,
        "files": {},
        "events": [],
        "directory_listing_sha256": None,
    }
    print("Created manifest.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 5. Discover current BLS directory state

# COMMAND ----------

# helps us determine which files appear new, changed, unchanged, or removed before downloading any source-file contents

# Do a GET request to fetch BLS data
directory_response = get_bls(BLS_DIRECTORY_URL)

# show HTML of response
print(directory_response)

# used for tracking when the HTML changes
directory_bytes = directory_response.content
print(type(directory_bytes))
print(f"directory_bytes:")
print(directory_bytes)

directory_html = directory_response.text
print(type(directory_html))
print("directory_html:")
print(directory_html)

discovered_files = discover_files(directory_html)
run_time = utc_now()

print(f"Discovered {len(discovered_files)} BLS files.")

print(f"discovered_files: {discovered_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⬇️ 6. Download only new or changed files

# COMMAND ----------

run_summary = {
    "new": 0,
    "changed": 0,
    "unchanged": 0,
    "removed": 0,
    "downloads": 0,
}

for file_name, source_metadata in sorted(discovered_files.items()):
    previous_metadata = manifest["files"].get(file_name)
    destination_path = RAW_DIRECTORY / file_name

    remote_metadata_unchanged = (
        previous_metadata is not None
        and previous_metadata.get("status") == "active"
        and previous_metadata.get("source_last_modified")
            == source_metadata["source_last_modified"]
        and previous_metadata.get("source_size_bytes")
            == source_metadata["source_size_bytes"]
        and destination_path.exists()
        and destination_path.stat().st_size
            == source_metadata["source_size_bytes"]
    )

    if remote_metadata_unchanged:
        previous_metadata["last_seen_at"] = run_time
        run_summary["unchanged"] += 1
        print(f"[UNCHANGED] {file_name}")
        continue

    action = "NEW" if previous_metadata is None else "CHANGED"

    # BLS asks automated programs not to retrieve files multiple times per second.
    time.sleep(1)

    file_response = get_bls(source_metadata["source_url"])
    file_bytes = file_response.content

    if len(file_bytes) != source_metadata["source_size_bytes"]:
        raise ValueError(
            f"Size mismatch for {file_name}: "
            f"expected {source_metadata['source_size_bytes']}, got {len(file_bytes)}."
        )

    destination_path.write_bytes(file_bytes)

    manifest["files"][file_name] = {
        **source_metadata,
        "local_path": str(destination_path),
        "content_sha256": sha256_bytes(file_bytes),
        "first_seen_at": (
            previous_metadata.get("first_seen_at", run_time)
            if previous_metadata
            else run_time
        ),
        "last_seen_at": run_time,
        "last_downloaded_at": run_time,
        "removed_at": None,
        "status": "active",
    }

    manifest["events"].append({
        "observed_at": run_time,
        "event_type": action.lower(),
        "file_name": file_name,
    })

    # Save after every successful changed-file download so the next run has accurate state.
    save_json(MANIFEST_PATH, manifest)

    run_summary[action.lower()] += 1
    run_summary["downloads"] += 1
    print(f"[{action}] {file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗑️ 7. Remove files that disappeared upstream

# COMMAND ----------

current_source_file_names = set(discovered_files)

previous_active_file_names = {
    file_name
    for file_name, metadata in manifest["files"].items()
    if metadata.get("status") == "active"
}

removed_file_names = previous_active_file_names - current_source_file_names

for file_name in sorted(removed_file_names):
    destination_path = RAW_DIRECTORY / file_name

    if destination_path.exists():
        destination_path.unlink()

    manifest["files"][file_name]["status"] = "removed"
    manifest["files"][file_name]["removed_at"] = run_time

    manifest["events"].append({
        "observed_at": run_time,
        "event_type": "removed",
        "file_name": file_name,
    })

    run_summary["removed"] += 1
    print(f"[REMOVED] {file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 8. Save the raw directory HTML only when it changes

# COMMAND ----------

directory_listing_sha256 = sha256_bytes(directory_bytes)

if (
    manifest.get("directory_listing_sha256") != directory_listing_sha256
    or not DIRECTORY_LISTING_PATH.exists()
):
    DIRECTORY_LISTING_PATH.write_bytes(directory_bytes)
    manifest["directory_listing_sha256"] = directory_listing_sha256
    print("[UPDATED] _directory_listing.html")
else:
    print("[UNCHANGED] _directory_listing.html")

manifest["last_successful_run_at"] = utc_now()
save_json(MANIFEST_PATH, manifest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 9. Run summary

# COMMAND ----------

print(json.dumps(run_summary, indent=2))