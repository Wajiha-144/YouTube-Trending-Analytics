from delta.tables import DeltaTable

def enable_schema_evolution(spark):
    try:  
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception:
        pass  

def ensure_table(spark, name, schema, partition_cols=None):
    w = spark.createDataFrame([], schema).write.format("delta").mode("ignore")
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.saveAsTable(name)

def _last_merge_metrics(spark, table):
    m = DeltaTable.forName(spark, table).history(1).select("operationMetrics").first()[0]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))

def _cond(keys):
    return " AND ".join(f"t.{k} = s.{k}" for k in keys)

def merge_insert_only(spark, df, target, keys):
    """Bronze/quarantine: insert new rows, ignore existing ones."""
    (DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), _cond(keys))
        .whenNotMatchedInsertAll().execute())
    return _last_merge_metrics(spark, target)

def merge_upsert(spark, df, target, keys, change_col="row_hash"):
    """Silver: insert new, update ONLY if content changed -> re-runs are no-ops."""
    (DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), _cond(keys))
        .whenMatchedUpdateAll(condition=f"t.{change_col} <> s.{change_col}")
        .whenNotMatchedInsertAll().execute())
    return _last_merge_metrics(spark, target)