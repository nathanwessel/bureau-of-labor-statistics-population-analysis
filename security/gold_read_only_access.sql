-- Unity Catalog access controls for read-only Gold consumers.
-- `gold_readers` represents the account-level analyst group that would consume curated Gold data in a production Databricks environment.
-- Analysts can discover and query the Gold schema (but are not granted permissions to create, modify, or delete Gold objects).

GRANT USE CATALOG
ON CATALOG rearc
TO `gold_readers`;

GRANT USE SCHEMA
ON SCHEMA rearc.gold
TO `gold_readers`;

GRANT SELECT
ON SCHEMA rearc.gold
TO `gold_readers`;

DROP TABLE 