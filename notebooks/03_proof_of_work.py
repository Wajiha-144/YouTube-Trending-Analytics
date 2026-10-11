# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.path.abspath(".."))

from src.config import *
from src.params import resolve_params
from src.pipelines.raw_to_bronze import run_raw_to_bronze
from src.pipelines.reference import load_categories
from src.pipelines.bronze_to_silver import run_bronze_to_silver

def run_pipeline(load_type, start, end, regions="", batch_id=""):
    """Raw -> Bronze -> Silver for any date range. Parameters only, no code edits."""
    s, e, r, b = resolve_params(load_type, start, end, regions, batch_id)
    print("BRONZE:", run_raw_to_bronze(spark, ["api"], s, e, r, load_type, b))
    load_categories(spark, load_type, b)
    run_bronze_to_silver(spark, s, e, load_type, b)
    print("SILVER done | batch_id =", b)
    return b

def fingerprint(table, row_expr, where="1=1"):
    """Row count + a hash of the table's content + latest load_timestamp."""
    return spark.sql(f"""
        SELECT COUNT(*) AS row_count,
               md5(concat_ws('', sort_array(collect_list({row_expr})))) AS content_hash,
               MAX(load_timestamp) AS max_load_ts
        FROM {table} WHERE {where}""").first()

SILVER_ROW = "concat(video_id, region_code, CAST(snapshot_date AS STRING), row_hash)"
BRONZE_ROW = "bronze_record_id"

def show_logs(batch_id):
    display(spark.table(T_RUN_LOGS).filter(f"batch_id = '{batch_id}'")
        .select("layer", "load_type", "parameter", "status", "rows_read",
                "rows_inserted", "rows_updated", "rows_quarantined", "start_time", "end_time")
        .orderBy("start_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Explicit Schema

# COMMAND ----------

from src.schemas import API_ITEM_SCHEMA, KAGGLE_CSV_SCHEMA
print("READ schema used before reading raw files (no inferSchema):")
print(API_ITEM_SCHEMA.simpleString())
print()
for t in (T_BRONZE_API, T_BRONZE_KAGGLE, T_SILVER_VIDEO, T_SILVER_CAT, T_QUARANTINE, T_RUN_LOGS):
    print("=====", t); spark.table(t).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Idempotency

# COMMAND ----------

W_B = "ingestion_date BETWEEN '2026-09-26' AND '2026-09-27'"
W_S = "snapshot_date BETWEEN '2026-09-26' AND '2026-09-27'"
before_b = fingerprint(T_BRONZE_API, BRONZE_ROW, W_B)
before_s = fingerprint(T_SILVER_VIDEO, SILVER_ROW, W_S)
print("BEFORE bronze:", before_b); print("BEFORE silver:", before_s)

# COMMAND ----------

bid = run_pipeline("incremental", "2026-09-26", "2026-09-27", "", "idem_rerun")

# COMMAND ----------

after_b = fingerprint(T_BRONZE_API, BRONZE_ROW, W_B)
after_s = fingerprint(T_SILVER_VIDEO, SILVER_ROW, W_S)
print("AFTER  bronze:", after_b); print("AFTER  silver:", after_s)
print("Bronze identical:", before_b == after_b)
print("Silver identical:", before_s == after_s)

display(spark.sql(f"""
  SELECT COUNT(*) AS rows, COUNT(DISTINCT video_id, region_code, snapshot_date) AS distinct_keys
  FROM {T_SILVER_VIDEO}"""))
show_logs("idem_rerun")

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T_SILVER_VIDEO}")
        .filter("operation = 'MERGE'")
        .select("version", "timestamp", "operationMetrics").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Backfill

# COMMAND ----------

ref_s = fingerprint(T_SILVER_VIDEO, SILVER_ROW, "snapshot_date = '2026-09-26'")
ref_b = fingerprint(T_BRONZE_API, BRONZE_ROW, "ingestion_date = '2026-09-26'")
print("reference silver 09-26:", ref_s); print("reference bronze 09-26:", ref_b)

spark.sql(f"DELETE FROM {T_SILVER_VIDEO} WHERE snapshot_date = '2026-09-26'")
spark.sql(f"DELETE FROM {T_BRONZE_API} WHERE ingestion_date = '2026-09-26'")

for name, t, c in [("bronze", T_BRONZE_API, "ingestion_date"), ("silver", T_SILVER_VIDEO, "snapshot_date")]:
    print(name, "after delete:")
    display(spark.sql(f"SELECT {c} AS d, COUNT(*) AS rows FROM {t} GROUP BY {c} ORDER BY {c}"))

# COMMAND ----------

bid = run_pipeline("backfill", "2026-09-26", "2026-09-27", "", "backfill_demo")

# COMMAND ----------

new_s = fingerprint(T_SILVER_VIDEO, SILVER_ROW, "snapshot_date = '2026-09-26'")
new_b = fingerprint(T_BRONZE_API, BRONZE_ROW, "ingestion_date = '2026-09-26'")
print("rows restored  | bronze:", new_b.row_count == ref_b.row_count, "| silver:", new_s.row_count == ref_s.row_count)
print("content identical | bronze:", new_b.content_hash == ref_b.content_hash,
      "| silver:", new_s.content_hash == ref_s.content_hash)
show_logs("backfill_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Schema Drift

# COMMAND ----------

spark.sql(f"DELETE FROM {T_BRONZE_API} WHERE ingestion_date = '2026-09-28'")
spark.sql(f"DELETE FROM {T_SILVER_VIDEO} WHERE snapshot_date = '2026-09-28'")
for t in (T_QUARANTINE, T_DRIFT_LOGS):
    try: spark.sql(f"DELETE FROM {t} WHERE batch_id = 'drift_demo'")
    except Exception: pass          # tables may not exist on the very first run

# COMMAND ----------

d = f"{RAW_ROOT}/youtube_api/ingestion_date=2026-09-28/region=PK"
os.makedirs(d, exist_ok=True)

def item(vid, stats):
    return {"kind": "youtube#video", "id": vid,
            "snippet": {"publishedAt": "2026-09-27T10:00:00Z", "channelId": "UCdemo",
                        "title": f"Drift test {vid}", "categoryId": "10"},
            "contentDetails": {"duration": "PT3M5S"}, "statistics": stats}

lines = [
    json.dumps(item("drift001", {"viewCount": "100", "likeCount": "5", "commentCount": "1", "shareCount": "9"})),  # new column
    json.dumps(item("drift002", {"viewCount": "N/A", "likeCount": "5", "commentCount": "1"})),                      # bad type
    json.dumps(item("drift003", {"viewCount": 250, "likeCount": 7, "commentCount": 2})),                            # numbers, not strings
    "{this is not valid json",                                                                                       # corrupt
]
open(f"{d}/videos.jsonl", "w", encoding="utf-8").write("\n".join(lines))
print("written:", f"{d}/videos.jsonl")

# COMMAND ----------

bid = run_pipeline("incremental", "2026-09-28", "2026-09-28", "PK", "drift_demo")

# COMMAND ----------

print("1) New column detected and logged:")
display(spark.table(T_DRIFT_LOGS).filter("batch_id = 'drift_demo'")
        .select("source_file", "scope", "new_columns", "load_timestamp"))

print("2) Bronze keeps everything, including the corrupt line:")
display(spark.table(T_BRONZE_API).filter("ingestion_date = '2026-09-28'")
        .select("is_parseable", "payload.id", "raw_json"))

print("3) Silver holds only the valid rows:")
display(spark.table(T_SILVER_VIDEO).filter("snapshot_date = '2026-09-28'")
        .select("video_id", "view_count", "like_count", "comment_count", "batch_id"))

print("4) Quarantine holds the bad records with reasons:")
display(spark.table(T_QUARANTINE).filter("batch_id = 'drift_demo'")
        .select("source_table", "reason", "raw_record"))

show_logs("drift_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Final Log Table

# COMMAND ----------

display(spark.table(T_RUN_LOGS)
        .select("batch_id", "layer", "load_type", "parameter", "status",
                "start_time", "end_time", "rows_read", "rows_inserted", "rows_updated", "rows_quarantined")
        .orderBy("start_time", ascending=False))