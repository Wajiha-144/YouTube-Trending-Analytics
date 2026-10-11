CATALOG = "workspace"
SCHEMA = "youtube_trending"
RAW_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
ALL_REGIONS = ["PK", "US", "IN", "GB", "CA", "AU", "DE", "FR", "JP", "BR"]

def tbl(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

T_BRONZE_API    = tbl("bronze_api_videos")
T_BRONZE_KAGGLE = tbl("bronze_kaggle_videos")
T_SILVER_VIDEO  = tbl("silver_video_daily")
T_SILVER_CAT    = tbl("silver_category")
T_QUARANTINE    = tbl("silver_quarantine")
T_RUN_LOGS      = tbl("pipeline_execution_logs")
T_DRIFT_LOGS    = tbl("schema_drift_log")

PII_SALT = "replace-me"   