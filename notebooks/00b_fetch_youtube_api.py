# Databricks notebook source
import os, sys
sys.path.insert(0, os.path.abspath(".."))

from src.config import ALL_REGIONS
from src.params import resolve_params
from src.pipelines.api_to_raw import run_api_to_raw

def get_api_key():
    try:
        return dbutils.secrets.get("youtube", "api_key")
    except Exception:
        with open("/Volumes/workspace/youtube_trending/secreta/api_key.txt") as f:
            return f.read().strip()

API_KEY = get_api_key()
print("key loaded:", bool(API_KEY))        # prints True or False, never the key itself

# COMMAND ----------

load_type = "incremental"
start, end, regions, batch_id = resolve_params(load_type, "", "", "", "")   # blank date means today (UTC)
print("fetching for", start, regions)
print(run_api_to_raw(spark, API_KEY, regions, start, load_type, batch_id))
result = run_raw_to_bronze(spark, sources, start, end, regions, load_type, batch_id)
print(result)
if result["failed"]:
    raise RuntimeError(f"{len(result['failed'])} file(s) failed. See pipeline_execution_logs.")