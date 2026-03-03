# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DISTINCT server_name
# MAGIC     FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC     ORDER BY server_name

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for sp in w.service_principals.list():
    print(sp.display_name, "->", sp.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ VOLUME
# MAGIC ON VOLUME btris_dbx.observability.server_observability_vol
# MAGIC TO `a70123f0-da9c-444a-bcfa-eed1fa16a9d4`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT server_name, snapshot_date
# MAGIC FROM btris_dbx.observability.v_latest_sql_diagnostics
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sheet_name, COUNT(*) AS rows_in_sheet
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND (
# MAGIC     lower(sheet_name) LIKE '%version info%'
# MAGIC     OR lower(sheet_name) LIKE '%server properties%'
# MAGIC     OR lower(sheet_name) LIKE '%hardware info%'
# MAGIC     OR lower(sheet_name) LIKE '%host info%'
# MAGIC   )
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND lower(sheet_name) LIKE '%version info%'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND lower(sheet_name) LIKE '%hardware info%'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND CAST(snapshot_date AS string) = '2026-02-28'
# MAGIC   AND lower(sheet_name) LIKE '%host info%'
# MAGIC LIMIT 5;