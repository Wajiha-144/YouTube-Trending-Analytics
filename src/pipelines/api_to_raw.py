from src.config import RAW_ROOT
from src.common.audit import PipelineRun
from src.ingestion.youtube_api import fetch_trending, write_jsonl

def run_api_to_raw(spark, api_key, regions, ingestion_date, load_type, batch_id, max_pages=1):
    ok, failed = 0, []
    for r in regions:
        path = f"{RAW_ROOT}/youtube_api/ingestion_date={ingestion_date}/region={r}/videos.jsonl"
        try:
            with PipelineRun(spark, "api_to_raw", path, load_type, batch_id, None) as run:
                items = fetch_trending(api_key, r, max_pages)
                run.rows_read = len(items)
                write_jsonl(items, path)
                run.rows_inserted = len(items)
            ok += 1
        except Exception as e:
            failed.append((r, str(e)[:200]))
    return {"succeeded": ok, "failed": failed}