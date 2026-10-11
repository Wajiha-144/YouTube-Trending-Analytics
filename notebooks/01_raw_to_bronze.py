# Databricks notebook source
import sys, os
sys.path.insert(0, os.path.abspath(".."))  

# COMMAND ----------

from src.params import resolve_params
from src.pipelines.raw_to_bronze import run_raw_to_bronze

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental", "backfill"])
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("regions", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("source", "api", ["all", "api", "kaggle"])

# COMMAND ----------

load_type = dbutils.widgets.get("load_type")
start, end, regions, batch_id = resolve_params(
    load_type, 
    dbutils.widgets.get("start_date"), 
    dbutils.widgets.get("end_date"),
    dbutils.widgets.get("regions"), 
    dbutils.widgets.get("batch_id"))
src = dbutils.widgets.get("source")
sources = ["api", "kaggle"] if src == "all" else [src]
print("USING:", load_type, start, end, regions, sources)

# COMMAND ----------

print(run_raw_to_bronze(spark, sources, start, end, regions, load_type, batch_id))
result = run_raw_to_bronze(spark, sources, start, end, regions, load_type, batch_id)
print(result)
if result["failed"]:
    raise RuntimeError(f"{len(result['failed'])} file(s) failed. See pipeline_execution_logs.")