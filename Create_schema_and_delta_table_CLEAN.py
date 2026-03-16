# Databricks notebook source
display(
    spark.sql("""
    DESCRIBE TABLE btris_dbx.observability.sql_diagnostics_files_delta
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Server Observability (PerfMon + SQLDiagnostics)
# MAGIC
# MAGIC This notebook sets up:
# MAGIC - `btris_dbx.observability` schema
# MAGIC - UC Volume storage for raw files
# MAGIC - Delta ingestion for:
# MAGIC   - PerfMon metrics (CSV/Excel)
# MAGIC   - SQLDiagnostics weekly files (one Excel per server per week)
# MAGIC
# MAGIC Note: Windows Events ingestion is not used in this version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS btris_dbx.observability;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set catalog and schema context
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG btris_dbx;
# MAGIC USE SCHEMA observability;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define UC Volume base path
# MAGIC

# COMMAND ----------

BASE_PATH = "/Volumes/btris_dbx/observability/server_observability_vol"
BASE_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create required folders (PerfMon + SQLDiagnostics)
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs(f"{BASE_PATH}/raw/perfmon")
dbutils.fs.mkdirs(f"{BASE_PATH}/raw/sql_diagnostics/inbox")
dbutils.fs.mkdirs(f"{BASE_PATH}/raw/sql_diagnostics/by_server")
display(dbutils.fs.ls(f"{BASE_PATH}/raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SQLDiagnostics file registry Delta table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS btris_dbx.observability.sql_diagnostics_files_delta (
# MAGIC   server_name STRING,
# MAGIC   snapshot_date DATE,
# MAGIC   ingestion_date DATE,
# MAGIC   file_path STRING,
# MAGIC   inbox_path STRING,
# MAGIC   file_size_bytes BIGINT,
# MAGIC   modified_ts TIMESTAMP,
# MAGIC   ingested_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview SQLDiagnostics inbox
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE_PATH}/raw/sql_diagnostics/inbox"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQLDiagnostics inbox scanner & registry writer
# MAGIC

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

TABLE   = "btris_dbx.observability.sql_diagnostics_files_delta"
INBOX   = f"{BASE_PATH}/raw/sql_diagnostics/inbox"
BY_SVR  = f"{BASE_PATH}/raw/sql_diagnostics/by_server"

def server_from_filename(filename: str) -> str:
    return re.sub(r"\.xlsx$", "", filename, flags=re.IGNORECASE).strip()

items = [x for x in dbutils.fs.ls(INBOX) if x.path.lower().endswith(".xlsx")]

if not items:
    print(f"No .xlsx files found in: {INBOX}")
else:
    rows = []
    for it in items:
        server = server_from_filename(it.name)
        dest_dir  = f"{BY_SVR}/{server}"
        dest_path = f"{dest_dir}/{it.name}"

        dbutils.fs.mkdirs(dest_dir)
        dbutils.fs.cp(it.path, dest_path, True)

        rows.append((server, it.path, dest_path, int(it.size), int(it.modificationTime)))

    schema = StructType([
        StructField("server_name", StringType(), False),
        StructField("inbox_path", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("file_size_bytes", LongType(), True),
        StructField("modified_time_ms", LongType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    df = (
        df.withColumn("modified_ts", (F.col("modified_time_ms") / 1000).cast("timestamp"))
          .withColumn("snapshot_date", F.to_date(F.col("modified_ts")))
          .withColumn("ingestion_date", F.current_date())
          .withColumn("ingested_ts", F.current_timestamp())
          .drop("modified_time_ms")
    )

    table_cols = [
        r["col_name"]
        for r in spark.sql(f"DESCRIBE TABLE {TABLE}").collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    ]

    if "ingestion_date" in table_cols:
        existing = (
            spark.sql(f"""
            SELECT server_name, file_path, ingestion_date
            FROM {TABLE}
            """).dropDuplicates()
        )
    else:
        existing = (
            spark.sql(f"""
            SELECT server_name, file_path
            FROM {TABLE}
            """)
            .dropDuplicates()
            .withColumn("ingestion_date", F.lit(None).cast("date"))
        )

    to_insert = (
        df.alias("n")
          .join(
              existing.alias("e"),
              on=[
                  F.col("n.server_name") == F.col("e.server_name"),
                  F.col("n.file_path") == F.col("e.file_path"),
                  F.col("n.ingestion_date") == F.col("e.ingestion_date"),
              ],
              how="left_anti"
          )
          .select("n.*")
    )

    to_insert.write.mode("append").format("delta").saveAsTable(TABLE)

    display(
        to_insert.select(
            "server_name",
            "snapshot_date",
            "ingestion_date",
            "file_path",
            "modified_ts",
            "ingested_ts"
        ).orderBy(F.col("ingested_ts").desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate registry (keep latest per server + snapshot_date)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE btris_dbx.observability.sql_diagnostics_files_delta AS
# MAGIC SELECT
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   file_path,
# MAGIC   inbox_path,
# MAGIC   file_size_bytes,
# MAGIC   modified_ts,
# MAGIC   ingested_ts
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (
# MAGIC            PARTITION BY server_name, file_path, ingestion_date
# MAGIC            ORDER BY modified_ts DESC, ingested_ts DESC
# MAGIC          ) AS rn
# MAGIC   FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify registry
# MAGIC

# COMMAND ----------

display(spark.sql("""
SELECT
  server_name,
  snapshot_date,
  ingestion_date,
  file_path,
  modified_ts,
  ingested_ts
FROM btris_dbx.observability.sql_diagnostics_files_delta
ORDER BY ingested_ts DESC, server_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest SQLDiagnostics snapshot view (1 row per server)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW btris_dbx.observability.v_latest_sql_diagnostics AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC       server_name,
# MAGIC       snapshot_date,
# MAGIC       ingestion_date,
# MAGIC       file_path,
# MAGIC       inbox_path,
# MAGIC       file_size_bytes,
# MAGIC       modified_ts,
# MAGIC       ingested_ts,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC           PARTITION BY server_name
# MAGIC           ORDER BY ingestion_date DESC, snapshot_date DESC, ingested_ts DESC
# MAGIC       ) AS rn
# MAGIC   FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze table for SQLDiagnostics sheets (generic JSON rows)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS btris_dbx.observability.sql_diagnostics_bronze (
# MAGIC   server_name STRING,
# MAGIC   snapshot_date DATE,
# MAGIC   ingestion_date DATE,
# MAGIC   sheet_name STRING,
# MAGIC   row_json STRING,
# MAGIC   ingested_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel dependency (openpyxl)
# MAGIC Install if missing. If already installed, this is a no-op.
# MAGIC

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-file, multi-sheet ingestion (first 52 sheets)
# MAGIC Skips empty/NoData sheets; writes idempotently to Bronze.
# MAGIC

# COMMAND ----------

import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType

REGISTRY_VIEW = "btris_dbx.observability.v_latest_sql_diagnostics"
BRONZE_TABLE  = "btris_dbx.observability.sql_diagnostics_bronze"

def is_nodata(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return True
    txt = df.astype(str).to_string().lower()
    return ("nodata" in txt) or ("no diagnostic data found" in txt)

schema = StructType([
    StructField("server_name", StringType(), False),
    StructField("snapshot_date", DateType(), False),
    StructField("ingestion_date", DateType(), False),
    StructField("sheet_name", StringType(), False),
    StructField("row_json", StringType(), False),
])

reg = spark.sql(f"""
SELECT server_name, snapshot_date, ingestion_date, file_path
FROM {REGISTRY_VIEW}
ORDER BY ingestion_date DESC, snapshot_date DESC, server_name
""").collect()

print(f"Registry rows to ingest: {len(reg)}")

for r in reg:
    server_name    = r["server_name"]
    snapshot_date  = r["snapshot_date"]
    ingestion_date = r["ingestion_date"]
    file_path      = r["file_path"]

    vol_path = file_path.replace("dbfs:", "")

    print(
        f"\n--- Ingesting: server={server_name} | "
        f"snapshot_date={snapshot_date} | ingestion_date={ingestion_date} | file={vol_path} ---"
    )

    try:
        xls = pd.ExcelFile(vol_path)
        target_sheets = xls.sheet_names[:52]
    except Exception as e:
        print(f"FAILED to open Excel for {server_name}: {e}")
        continue

    out = []
    valid_sheet_count = 0

    for sheet_name in target_sheets:
        try:
            pdf = pd.read_excel(vol_path, sheet_name=sheet_name)

            if is_nodata(pdf):
                continue

            pdf.columns = [str(c).strip() for c in pdf.columns]
            pdf = pdf.dropna(how="all").dropna(axis=1, how="all")
            if pdf.empty:
                continue

            valid_sheet_count += 1

            for rec in pdf.to_dict(orient="records"):
                safe = {str(k): (None if pd.isna(v) else v) for k, v in rec.items()}
                out.append((
                    server_name,
                    snapshot_date,
                    ingestion_date,
                    sheet_name,
                    json.dumps(safe, default=str)
                ))

        except Exception as e:
            print(f"  Skipped sheet (error): {sheet_name} -> {e}")

    print(f"Valid sheets: {valid_sheet_count} | Rows prepared: {len(out)}")

    spark.sql(f"""
    DELETE FROM {BRONZE_TABLE}
    WHERE server_name = '{server_name}'
      AND snapshot_date = DATE('{snapshot_date}')
      AND ingestion_date = DATE('{ingestion_date}')
    """)

    if out:
        sdf = spark.createDataFrame(out, schema=schema).withColumn("ingested_ts", F.current_timestamp())
        sdf.write.mode("append").format("delta").saveAsTable(BRONZE_TABLE)

        display(spark.sql(f"""
        SELECT sheet_name, COUNT(*) AS row_count
        FROM {BRONZE_TABLE}
        WHERE server_name = '{server_name}'
          AND snapshot_date = DATE('{snapshot_date}')
          AND ingestion_date = DATE('{ingestion_date}')
        GROUP BY sheet_name
        ORDER BY row_count DESC
        LIMIT 10
        """))
    else:
        print("No rows to write for this server+snapshot+ingestion_date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Bronze verification (rows per server and snapshot)
# MAGIC

# COMMAND ----------

display(spark.sql("""
SELECT
  server_name,
  snapshot_date,
  ingestion_date,
  COUNT(*) AS bronze_rows
FROM btris_dbx.observability.sql_diagnostics_bronze
GROUP BY server_name, snapshot_date, ingestion_date
ORDER BY ingestion_date DESC, snapshot_date DESC, server_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest SQL Diagnostics Files View (App-Facing Contract)
# MAGIC
# MAGIC This view provides a single, stable interface for the Streamlit application to retrieve:
# MAGIC
# MAGIC - The list of available servers
# MAGIC - The latest snapshot per server
# MAGIC - The file path for downloading the original SQLDiagnostics Excel
# MAGIC - Metadata such as ingestion timestamp and file size
# MAGIC
# MAGIC ### Purpose
# MAGIC
# MAGIC The ingestion pipeline may process multiple snapshots per server over time.  
# MAGIC The application, however, must always reference **only the most recent snapshot** for each server.
# MAGIC
# MAGIC This view:
# MAGIC
# MAGIC - Ranks snapshots per server using `snapshot_date` and `ingested_ts`
# MAGIC - Selects only the latest row per server
# MAGIC - Ensures deterministic behavior for:
# MAGIC   - Server dropdown population
# MAGIC   - Download Metrics button
# MAGIC   - Health Assessment Report generation
# MAGIC
# MAGIC ### Source Table
# MAGIC
# MAGIC `btris_dbx.observability.sql_diagnostics_files_delta`
# MAGIC
# MAGIC ### Output View
# MAGIC
# MAGIC `btris_dbx.observability.v_sql_diagnostics_latest_files`
# MAGIC
# MAGIC This view acts as the contract layer between the ingestion pipeline and the Streamlit application.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW btris_dbx.observability.v_sql_diagnostics_latest_files AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     server_name,
# MAGIC     snapshot_date,
# MAGIC     ingestion_date,
# MAGIC     file_path,
# MAGIC     inbox_path,
# MAGIC     file_size_bytes,
# MAGIC     modified_ts,
# MAGIC     ingested_ts,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY server_name
# MAGIC       ORDER BY ingestion_date DESC, snapshot_date DESC, ingested_ts DESC
# MAGIC     ) AS rn
# MAGIC   FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC )
# MAGIC SELECT
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   file_path,
# MAGIC   inbox_path,
# MAGIC   file_size_bytes,
# MAGIC   modified_ts,
# MAGIC   ingested_ts
# MAGIC FROM ranked
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW btris_dbx.observability.v_sql_diagnostics_latest_bronze AS
# MAGIC WITH latest AS (
# MAGIC     SELECT server_name, snapshot_date, ingestion_date
# MAGIC     FROM btris_dbx.observability.v_sql_diagnostics_latest_files
# MAGIC )
# MAGIC SELECT
# MAGIC     b.server_name,
# MAGIC     b.snapshot_date,
# MAGIC     b.ingestion_date,
# MAGIC     b.sheet_name,
# MAGIC     b.row_json,
# MAGIC     b.ingested_ts
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze b
# MAGIC INNER JOIN latest l
# MAGIC     ON b.server_name = l.server_name
# MAGIC    AND b.snapshot_date = l.snapshot_date
# MAGIC    AND b.ingestion_date = l.ingestion_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   sheet_name,
# MAGIC   COUNT(*) AS rows_per_sheet
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC GROUP BY server_name, snapshot_date, sheet_name
# MAGIC ORDER BY rows_per_sheet DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC LIMIT 5;

# COMMAND ----------

display(spark.sql("""
DESCRIBE TABLE btris_dbx.observability.sql_diagnostics_files_delta
"""))

# COMMAND ----------

display(spark.sql("""
DESCRIBE TABLE btris_dbx.observability.sql_diagnostics_bronze
"""))

# COMMAND ----------

display(spark.sql("""
SELECT *
FROM btris_dbx.observability.v_sql_diagnostics_latest_files
LIMIT 5
"""))

display(spark.sql("""
SELECT *
FROM btris_dbx.observability.v_sql_diagnostics_latest_bronze
LIMIT 5
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS null_registry_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_files_delta
# MAGIC WHERE ingestion_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS null_bronze_rows
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE ingestion_date IS NULL;