# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC WHERE ingestion_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   sheet_name,
# MAGIC   COUNT(*) AS rows_to_delete
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE ingestion_date IS NULL
# MAGIC GROUP BY server_name, snapshot_date, sheet_name
# MAGIC ORDER BY server_name, snapshot_date, sheet_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE ingestion_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC WHERE ingestion_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_registry_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC WHERE ingestion_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS null_bronze_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE ingestion_date IS NULL;