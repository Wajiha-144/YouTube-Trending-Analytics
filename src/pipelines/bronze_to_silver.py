from src.config import *
from src.schemas import *
from src.common.audit import PipelineRun
from src.common.delta_utils import ensure_table, merge_upsert, merge_insert_only, enable_schema_evolution
from src.transforms.staging import stage_api, stage_kaggle
from src.transforms.quality import cast_and_validate, build_silver, to_quarantine

SILVER_KEYS = ["video_id", "region_code", "snapshot_date"]

def run_bronze_to_silver(spark, start_date, end_date, load_type, batch_id):
    enable_schema_evolution(spark)
    ensure_table(spark, T_SILVER_VIDEO, SILVER_VIDEO_SCHEMA)
    ensure_table(spark, T_QUARANTINE, QUARANTINE_SCHEMA)
    param = f"ingestion_date {start_date}..{end_date}"

    with PipelineRun(spark, "bronze_to_silver", param, load_type, batch_id, T_SILVER_VIDEO) as run:
        api_good, api_bad = stage_api(spark, start_date, end_date)
        staged = api_good.unionByName(stage_kaggle(spark, start_date, end_date))
        valid, invalid = cast_and_validate(staged)

        cats = spark.table(T_SILVER_CAT).select("category_id", "category_name")
        silver = build_silver(valid, cats, batch_id)

        run.rows_read = staged.count() + api_bad.count()
        run.rows_inserted, run.rows_updated = merge_upsert(spark, silver, T_SILVER_VIDEO, SILVER_KEYS)

        quarantine = (to_quarantine(invalid, batch_id)
              .unionByName(to_quarantine(api_bad, batch_id)))
        run.rows_quarantined = quarantine.count()
        merge_insert_only(spark, quarantine, T_QUARANTINE, ["quarantine_id"])