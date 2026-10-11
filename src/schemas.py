from pyspark.sql.types import *

S, L, I, D, TS, B = (StringType(), LongType(), IntegerType(),DateType(), TimestampType(), BooleanType())
def f(name, dtype, nullable=True):
    return StructField(name, dtype, nullable)

# READ schemas
RAW_LINE_SCHEMA = StructType([f("value", S)])
API_ITEM_SCHEMA = StructType([                         
    f("kind", S), f("etag", S), f("id", S),
    f("snippet", StructType([
        f("publishedAt", S), f("channelId", S), f("title", S),
        f("description", S), f("channelTitle", S),
        f("tags", ArrayType(S)), f("categoryId", S)])),
    f("contentDetails", StructType([f("duration", S)])),
    f("statistics", StructType([                       # API sends counts as strings
        f("viewCount", S), f("likeCount", S), f("commentCount", S)])),
])

KAGGLE_CSV_SCHEMA = StructType([f(n, S) for n in [
    "video_id", "trending_date", "title", "channel_title", "category_id", "publish_time", "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link", "comments_disabled", "ratings_disabled", "video_error_or_removed", "description"]])

CATEGORY_CSV_SCHEMA = StructType([f("category_id", S), f("category_name", S)])

# TABLE schemas 
_BRONZE_META = [f("region_code", S), f("ingestion_date", D), f("source_system", S), f("source_file", S), f("batch_id", S), f("bronze_record_id", S, False), f("load_timestamp", TS, False)]

BRONZE_API_SCHEMA = StructType([f("raw_json", S), f("payload", API_ITEM_SCHEMA), f("is_parseable", B)] + _BRONZE_META) 

BRONZE_KAGGLE_SCHEMA = StructType(KAGGLE_CSV_SCHEMA.fields + _BRONZE_META)

SILVER_VIDEO_SCHEMA = StructType([
    f("video_id", S, False), f("region_code", S, False), f("snapshot_date", D, False), f("title", S), f("channel_id_hash", S), f("channel_title", S),
    f("category_id", I), f("category_name", S),
    f("description_redacted", S), f("tags_redacted", ArrayType(S)), f("published_at", TS), f("duration_seconds", I), f("view_count", L), f("like_count", L), f("dislike_count", L), f("comment_count", L), f("source_system", S), f("source_file", S), f("batch_id", S), f("row_hash", S), f("load_timestamp", TS, False)])

SILVER_CATEGORY_SCHEMA = StructType([f("category_id", I, False), f("category_name", S), f("load_timestamp", TS, False)])

QUARANTINE_SCHEMA = StructType([
    f("quarantine_id", S, False), f("source_table", S), f("bronze_record_id", S), f("reason", S), f("raw_record", S), f("batch_id", S), f("load_timestamp", TS, False)])

RUN_LOG_SCHEMA = StructType([
    f("run_id", S), f("batch_id", S), f("layer", S), f("load_type", S), f("parameter", S), f("target_table", S), f("start_time", TS), f("end_time", TS), f("status", S), f("rows_read", L), f("rows_inserted", L), f("rows_updated", L), f("rows_quarantined", L), f("error_message", S), f("load_timestamp", TS)])

DRIFT_LOG_SCHEMA = StructType([
    f("batch_id", S), f("source_file", S), f("scope", S),
    f("new_columns", S), f("load_timestamp", TS)])