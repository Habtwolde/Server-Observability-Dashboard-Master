# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   sheet_name,
# MAGIC   COUNT(*) AS row_count
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE lower(sheet_name) LIKE '%wait%'
# MAGIC GROUP BY server_name, snapshot_date, sheet_name
# MAGIC ORDER BY snapshot_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND lower(sheet_name) LIKE '%wait%'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND lower(sheet_name) LIKE '%wait%'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC   SELECT CAST(snapshot_date AS string) AS snapshot
# MAGIC   FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC   WHERE server_name = 'hc1dbsq36pv'
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT sheet_name, COUNT(*) AS rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = (SELECT snapshot FROM latest)
# MAGIC   AND lower(sheet_name) LIKE '%wait%'
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY rows DESC;