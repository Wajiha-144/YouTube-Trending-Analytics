# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath(".."))



# COMMAND ----------

from src.params import resolve_params
from src.pipelines.reference import load_categories
from src.pipelines.bronze_to_silver import run_bronze_to_silver

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental", "backfill"])
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

load_type = dbutils.widgets.get("load_type")
start, end, _, batch_id = resolve_params(
    load_type, 
    dbutils.widgets.get("start_date"), 
    dbutils.widgets.get("end_date"), 
    "", 
    dbutils.widgets.get("batch_id"))
print("USING:", load_type, start, end)

# COMMAND ----------

load_categories(spark, load_type, batch_id)
run_bronze_to_silver(spark, start, end, load_type, batch_id)