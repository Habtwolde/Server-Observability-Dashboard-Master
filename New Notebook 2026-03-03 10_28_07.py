# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1) Do we have any event-log-like sheets for this server?
# MAGIC SELECT DISTINCT sheet_name
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND (
# MAGIC     lower(sheet_name) LIKE '%event%'
# MAGIC     OR lower(sheet_name) LIKE '%windows%'
# MAGIC     OR lower(sheet_name) LIKE '%log%'
# MAGIC   )
# MAGIC ORDER BY sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sheet_name, COUNT(*) AS n_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND (
# MAGIC     lower(row_json) LIKE '%microsoft-windows%'
# MAGIC     OR lower(row_json) LIKE '%service control manager%'
# MAGIC     OR lower(row_json) LIKE '%eventid%'
# MAGIC     OR lower(row_json) LIKE '%winrm%'
# MAGIC     OR lower(row_json) LIKE '%event log%'
# MAGIC   )
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY n_rows DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sheet_name, COUNT(*) AS n_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'hc1dbsq36pv'
# MAGIC   AND sheet_name IN (
# MAGIC     '11-SQL Server Agent Alerts',
# MAGIC     '25-Login Failures',
# MAGIC     '26-Failed Jobs',
# MAGIC     '30-Failed SQL Agent Jobs',
# MAGIC     '31-Failed SQL Agent Job Steps',
# MAGIC     '45-CPU Utilization History',
# MAGIC     '28-Wait Stats',
# MAGIC     '40-Page Life Expectancy History',
# MAGIC     '41-Buffer Cache Hit Ratio History'
# MAGIC   )
# MAGIC GROUP BY sheet_name
# MAGIC ORDER BY n_rows DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND sheet_name = '11-SQL Server Agent Alerts'
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE server_name = 'SQLDiagnostics'
# MAGIC   AND sheet_name = '45-CPU Utilization History'
# MAGIC LIMIT 1;