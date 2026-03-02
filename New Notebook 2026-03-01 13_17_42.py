# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT sheet_name, COUNT(*) AS rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND lower(sheet_name) LIKE '%config%'
# MAGIC GROUP BY sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND sheet_name = '4-Configuration Values'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND sheet_name = '4-Configuration Values'
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT server_name, snapshot_date
# MAGIC FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC WHERE server_name = 'SQLDiagnostics';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sheet_name, COUNT(*) AS rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (
# MAGIC       SELECT CAST(snapshot_date AS string)
# MAGIC       FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC       WHERE server_name = 'SQLDiagnostics'
# MAGIC       LIMIT 1
# MAGIC   )
# MAGIC   AND (
# MAGIC        lower(sheet_name) LIKE '%io%'
# MAGIC     OR lower(sheet_name) LIKE '%i/o%'
# MAGIC     OR lower(sheet_name) LIKE '%disk%'
# MAGIC     OR lower(sheet_name) LIKE '%file%'
# MAGIC     OR lower(sheet_name) LIKE '%read%'
# MAGIC     OR lower(sheet_name) LIKE '%write%'
# MAGIC     OR lower(sheet_name) LIKE '%latenc%'
# MAGIC     OR lower(sheet_name) LIKE '%storage%'
# MAGIC   )
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY rows DESC, sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (
# MAGIC       SELECT CAST(snapshot_date AS string)
# MAGIC       FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC       WHERE server_name = 'SQLDiagnostics'
# MAGIC       LIMIT 1
# MAGIC   )
# MAGIC   AND sheet_name = '<PASTE_SHEET_NAME_HERE>'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT snapshot FROM latest;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT sheet_name, COUNT(*) AS rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name IN ('29-Drive Level Latency', '30-IO Latency by File', '37-IO Usage By Database')
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY rows DESC, sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '30-IO Latency by File'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '37-IO Usage By Database'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '29-Drive Level Latency'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '30-IO Latency by File'
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '37-IO Usage By Database'
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT sheet_name
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND lower(sheet_name) LIKE '%wait%'
# MAGIC ORDER BY sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'SQLDiagnostics'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND sheet_name = '40-Top Waits'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC