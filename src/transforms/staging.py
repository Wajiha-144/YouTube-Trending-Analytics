from pyspark.sql import functions as F
from src.config import *
from src.schemas import KAGGLE_CSV_SCHEMA

def _between(df, start, end):
    return df.filter(F.col("ingestion_date").between(start, end))

def stage_api(spark, start, end):
    b = _between(spark.table(T_BRONZE_API), start, end)
    p = F.col("payload")
    good = b.filter("is_parseable").select(
        p["id"].alias("video_id"), "region_code",
        F.col("ingestion_date").cast("string").alias("snapshot_date"),
        p["snippet"]["title"].alias("title"),
        p["snippet"]["channelId"].alias("channel_id"),
        p["snippet"]["channelTitle"].alias("channel_title"),
        p["snippet"]["categoryId"].alias("category_id"),
        p["snippet"]["publishedAt"].alias("published_at"),
        p["contentDetails"]["duration"].alias("duration"),
        p["snippet"]["description"].alias("description"),
        F.concat_ws("|", p["snippet"]["tags"]).alias("tags"),
        p["statistics"]["viewCount"].alias("view_count"),
        p["statistics"]["likeCount"].alias("like_count"),
        F.lit(None).cast("string").alias("dislike_count"),
        p["statistics"]["commentCount"].alias("comment_count"),
        F.lit(2).alias("source_priority"), "source_system", "source_file",
        "bronze_record_id", F.col("raw_json").alias("raw_record"),
        F.col("load_timestamp").alias("bronze_load_ts"))
    bad = (b.filter(~F.col("is_parseable"))
             .withColumn("raw_record", F.col("raw_json"))
             .withColumn("dq_reason", F.lit("unparseable_json_or_missing_id")))
    return good, bad

def stage_kaggle(spark, start, end):
    k = _between(spark.table(T_BRONZE_KAGGLE), start, end)
    return k.select(
        "video_id", "region_code",
        F.date_format(F.expr("try_to_timestamp(trending_date, 'yy.dd.MM')"), "yyyy-MM-dd").alias("snapshot_date"),
        "title", F.lit(None).cast("string").alias("channel_id"), "channel_title", "category_id",
        F.col("publish_time").alias("published_at"), F.lit(None).cast("string").alias("duration"),
        "description", "tags", F.col("views").alias("view_count"), F.col("likes").alias("like_count"),
        F.col("dislikes").alias("dislike_count"), "comment_count",
        F.lit(1).alias("source_priority"), "source_system", "source_file", "bronze_record_id",
        F.to_json(F.struct(*KAGGLE_CSV_SCHEMA.names)).alias("raw_record"),
        F.col("load_timestamp").alias("bronze_load_ts"))