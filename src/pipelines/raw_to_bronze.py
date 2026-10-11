import os
from pyspark.sql import functions as F
from src.config import *
from src.schemas import *
from src.common.dates import daterange
from src.common.audit import PipelineRun
from src.common.delta_utils import ensure_table, merge_insert_only, enable_schema_evolution
from src.common.drift import detect_new_json_keys, detect_new_csv_columns, record_drift


def _meta(df, source_system, region, ing_date, path, batch_id):
    return (df.withColumn("region_code", F.lit(region))
              .withColumn("ingestion_date", F.lit(ing_date).cast("date"))
              .withColumn("source_system", F.lit(source_system))
              .withColumn("source_file", F.lit(path))
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("load_timestamp", F.current_timestamp()))


def ingest_api_file(spark, path, region, ing_date, load_type, batch_id):
    with PipelineRun(spark, "raw_to_bronze", path, load_type, batch_id, T_BRONZE_API) as run:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        raw = (spark.read.schema(RAW_LINE_SCHEMA).text(path)          # explicit schema
                    .filter(F.trim("value") != "").withColumnRenamed("value", "raw_json"))

        for scope, cols in detect_new_json_keys(raw, "raw_json", API_ITEM_SCHEMA).items():
            record_drift(spark, batch_id, path, scope, cols)           # log, don't crash

        df = (raw.withColumn("payload", F.from_json("raw_json", API_ITEM_SCHEMA))
                 .withColumn("is_parseable", F.col("payload.id").isNotNull()))
        df = _meta(df, "youtube_api", region, ing_date, path, batch_id)
        df = (df.withColumn("bronze_record_id", F.sha2(F.concat_ws("||", "source_file", "raw_json"), 256))
                .dropDuplicates(["bronze_record_id"]).select(*BRONZE_API_SCHEMA.names))

        run.rows_read = df.count()
        run.rows_inserted, run.rows_updated = merge_insert_only(spark, df, T_BRONZE_API, ["bronze_record_id"])


def ingest_kaggle_file(spark, path, region, ing_date, load_type, batch_id):
    with PipelineRun(spark, "raw_to_bronze", path, load_type, batch_id, T_BRONZE_KAGGLE) as run:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        new_cols = detect_new_csv_columns(spark, path, KAGGLE_CSV_SCHEMA)
        if new_cols:
            record_drift(spark, batch_id, path, "<csv header>", new_cols)

        raw = (spark.read.schema(KAGGLE_CSV_SCHEMA).option("header", True)
                    .option("multiLine", True).option("escape", '"')
                    .option("mode", "PERMISSIVE").csv(path))
        df = _meta(raw, "kaggle_csv", region, ing_date, path, batch_id)
        rec = F.to_json(F.struct(*KAGGLE_CSV_SCHEMA.names))
        df = (df.withColumn("bronze_record_id", F.sha2(F.concat_ws("||", "source_file", rec), 256))
                .dropDuplicates(["bronze_record_id"]).select(*BRONZE_KAGGLE_SCHEMA.names))

        run.rows_read = df.count()
        run.rows_inserted, run.rows_updated = merge_insert_only(spark, df, T_BRONZE_KAGGLE, ["bronze_record_id"])


def run_raw_to_bronze(spark, sources, start_date, end_date, regions, load_type, batch_id):
    """Loops over (date x region); a bad file is logged and skipped, the batch finishes."""
    enable_schema_evolution(spark)
    ensure_table(spark, T_BRONZE_API, BRONZE_API_SCHEMA, ["ingestion_date"])
    ensure_table(spark, T_BRONZE_KAGGLE, BRONZE_KAGGLE_SCHEMA, ["ingestion_date"])
    ok, failed = 0, []
    for d in daterange(start_date, end_date):
        for r in regions:
            jobs = []
            if "api" in sources:
                jobs.append((ingest_api_file, f"{RAW_ROOT}/youtube_api/ingestion_date={d}/region={r}/videos.jsonl"))
            if "kaggle" in sources:
                jobs.append((ingest_kaggle_file, f"{RAW_ROOT}/kaggle/ingestion_date={d}/{r}videos.csv"))
            for fn, path in jobs:
                try:
                    fn(spark, path, r, d, load_type, batch_id); ok += 1
                except Exception as e:
                    failed.append((path, str(e)[:150]))
    return {"succeeded": ok, "failed": failed}