# Databricks notebook source
# MAGIC %md
# MAGIC # AI Observability Assistant — Vector Search Preparation
# MAGIC
# MAGIC This notebook prepares SQL Server diagnostic data for semantic retrieval.
# MAGIC
# MAGIC The goal is to allow the Server Observability Dashboard to answer questions such as:
# MAGIC
# MAGIC • Why is CPU high on a server?
# MAGIC • Which waits dominate performance?
# MAGIC • Which queries cause high logical reads?
# MAGIC • What changed between snapshots?
# MAGIC
# MAGIC The pipeline converts diagnostic rows into documents that can be embedded
# MAGIC using the Databricks BGE embedding model.
# MAGIC
# MAGIC Pipeline:
# MAGIC
# MAGIC SQL Diagnostics Bronze Table
# MAGIC         ↓
# MAGIC Vector Document Table
# MAGIC         ↓
# MAGIC Databricks Vector Search Index
# MAGIC         ↓
# MAGIC LLM-based AI assistant

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.1 — Build Vector Document Table
# MAGIC
# MAGIC This step converts rows from the SQL diagnostics bronze table into a
# MAGIC document format suitable for vector search.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE btris_dbx.observability.sql_diag_vector_docs AS
# MAGIC SELECT
# MAGIC   sha2(
# MAGIC       concat_ws('||',
# MAGIC           coalesce(server_name,''),
# MAGIC           coalesce(cast(snapshot_date as string),''),
# MAGIC           coalesce(cast(ingestion_date as string),''),
# MAGIC           coalesce(sheet_name,''),
# MAGIC           coalesce(row_json,'')
# MAGIC       ),256
# MAGIC   ) AS doc_id,
# MAGIC   server_name,
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   sheet_name,
# MAGIC   concat(
# MAGIC       'Server: ', server_name, '\n',
# MAGIC       'Snapshot Date: ', snapshot_date, '\n',
# MAGIC       'Ingestion Date: ', ingestion_date, '\n',
# MAGIC       'Sheet: ', sheet_name, '\n',
# MAGIC       'Content: ', row_json
# MAGIC   ) AS content,
# MAGIC   row_json
# MAGIC FROM btris_dbx.observability.sql_diagnostics_bronze
# MAGIC WHERE row_json IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.2 — Verify Document Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     server_name,
# MAGIC     ingestion_date,
# MAGIC     sheet_name,
# MAGIC     doc_id
# MAGIC FROM btris_dbx.observability.sql_diag_vector_docs
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.3 — Optimize Vector Documents Table

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE btris_dbx.observability.sql_diag_vector_docs
# MAGIC ZORDER BY (server_name, ingestion_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Databricks Vector Search Index
# MAGIC
# MAGIC This step creates a vector search index using the Databricks BGE embedding model.
# MAGIC
# MAGIC The index will generate embeddings for the `content` column in the
# MAGIC `sql_diag_vector_docs` table.
# MAGIC
# MAGIC This enables semantic search across:
# MAGIC
# MAGIC • all SQL servers  
# MAGIC • all ingestion dates  
# MAGIC • all diagnostic sheets  
# MAGIC
# MAGIC The index will allow the AI assistant to retrieve relevant diagnostics
# MAGIC before generating recommendations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Install Vector Search SDK
# MAGIC
# MAGIC The Vector Search Python client is not installed by default in this notebook environment.
# MAGIC
# MAGIC This step installs the SDK required to:
# MAGIC - create vector search endpoints
# MAGIC - create vector indexes
# MAGIC - query vector indexes

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.1 — Ensure Vector Search Endpoint Exists
# MAGIC
# MAGIC This step checks whether the vector search endpoint already exists.
# MAGIC If it does not exist, it creates it.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

ENDPOINT_NAME = "sql-observability-vector-endpoint"

vsc = VectorSearchClient()

# Safer handling for SDK response shape
resp = vsc.list_endpoints()
endpoints = resp.get("endpoints", []) if isinstance(resp, dict) else getattr(resp, "endpoints", []) or []

existing_names = []
for e in endpoints:
    if isinstance(e, dict):
        existing_names.append(e.get("name"))
    else:
        existing_names.append(getattr(e, "name", None))

if ENDPOINT_NAME in existing_names:
    print(f"Vector Search endpoint already exists: {ENDPOINT_NAME}")
else:
    print(f"Creating Vector Search endpoint: {ENDPOINT_NAME}")
    vsc.create_endpoint(
        name=ENDPOINT_NAME,
        endpoint_type="STANDARD"
    )
    print("Endpoint creation requested. It may take a few minutes to become ONLINE.")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
resp = vsc.list_endpoints()
display(resp if isinstance(resp, dict) else [getattr(x, "name", None) for x in getattr(resp, "endpoints", []) or []])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 Fix — Enable Change Data Feed on Vector Documents Table
# MAGIC
# MAGIC Delta Sync vector indexes require Change Data Feed (CDF) on the source table.
# MAGIC
# MAGIC This step enables CDF on the vector document table so Databricks Vector Search
# MAGIC can track inserts and updates.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE btris_dbx.observability.sql_diag_vector_docs
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

INDEX_NAME = "sql-diag-vector-index"

vsc.create_delta_sync_index(
    endpoint_name="sql-observability-vector-endpoint",
    index_name=f"btris_dbx.observability.{INDEX_NAME}",
    source_table_name="btris_dbx.observability.sql_diag_vector_docs",
    pipeline_type="TRIGGERED",
    primary_key="doc_id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

print("Vector index creation requested.")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

index = vsc.get_index(
    endpoint_name="sql-observability-vector-endpoint",
    index_name="btris_dbx.observability.sql-diag-vector-index"
)

display(index)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

index = vsc.get_index(
    endpoint_name="sql-observability-vector-endpoint",
    index_name="btris_dbx.observability.sql-diag-vector-index",
)

index.sync()

print("Vector index sync requested.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rebuild Vector Index in CONTINUOUS Sync Mode
# MAGIC
# MAGIC This step replaces the current Triggered-sync index with a Continuous-sync index.
# MAGIC
# MAGIC Why:
# MAGIC - Continuous sync updates automatically when the source Delta table changes.
# MAGIC - Triggered sync requires manual sync calls.
# MAGIC
# MAGIC Important:
# MAGIC - Continuous sync has a higher cost because Databricks provisions compute to keep the index updated.
# MAGIC - Rebuilding is the correct approach when changing index sync mode.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import time

ENDPOINT_NAME = "sql-observability-vector-endpoint"
INDEX_NAME = "btris_dbx.observability.sql_diag_vector_index"
SOURCE_TABLE = "btris_dbx.observability.sql_diag_vector_docs"
PRIMARY_KEY = "doc_id"
EMBEDDING_SOURCE_COLUMN = "content"
EMBEDDING_MODEL = "databricks-bge-large-en"

vsc = VectorSearchClient(disable_notice=True)

# 1) Delete existing index if it exists
try:
    vsc.delete_index(index_name=INDEX_NAME)
    print(f"Delete requested for existing index: {INDEX_NAME}")
    time.sleep(10)  # brief wait for deletion to register
except Exception as e:
    print(f"No existing index to delete, or delete not needed: {e}")

# 2) Recreate as CONTINUOUS
resp = vsc.create_delta_sync_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
    source_table_name=SOURCE_TABLE,
    pipeline_type="CONTINUOUS",
    primary_key=PRIMARY_KEY,
    embedding_source_column=EMBEDDING_SOURCE_COLUMN,
    embedding_model_endpoint_name=EMBEDDING_MODEL,
)

print("Continuous-sync index creation requested.")
display(resp)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

idx = vsc.get_index(
    endpoint_name="sql-observability-vector-endpoint",
    index_name="btris_dbx.observability.sql_diag_vector_index",
)

display(idx)