from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.schemas import SILVER_VIDEO_SCHEMA
from src.transforms.pii import hash_id, redact

CAST_RULES = {"snapshot_date": "date", "published_at": "timestamp", "category_id": "int",
              "view_count": "bigint", "like_count": "bigint",
              "dislike_count": "bigint", "comment_count": "bigint"}

def cast_and_validate(df):
    """try_cast never throws: failures become NULL and are reported as reasons."""
    reasons = []
    for c, t in CAST_RULES.items():
        df = (df.withColumn(f"_raw_{c}", F.col(c))
                .withColumn(c, F.expr(f"try_cast({c} as {t})")))
        reasons.append(F.when(F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull(),
                              F.lit(f"cast_failed:{c}")))
    reasons += [
        F.when(F.col("video_id").isNull() | (F.trim("video_id") == ""), F.lit("missing_video_id")),
        F.when(F.col("region_code").isNull(), F.lit("missing_region")),
        F.when(F.col("snapshot_date").isNull(), F.lit("missing_snapshot_date")),
        F.when(F.col("view_count") < 0, F.lit("negative_views")),
    ]
    df = df.withColumn("dq_reason", F.concat_ws(";", *reasons))   # concat_ws skips NULLs
    df = df.drop(*[f"_raw_{c}" for c in CAST_RULES])
    return df.filter("dq_reason = ''"), df.filter("dq_reason <> ''")

def to_quarantine(df, batch_id):
    return df.select(
        F.sha2(F.concat_ws("||", "bronze_record_id", "dq_reason"), 256).alias("quarantine_id"),
        F.col("source_system").alias("source_table"), "bronze_record_id",
        F.col("dq_reason").alias("reason"), "raw_record",
        F.lit(batch_id).alias("batch_id"), F.current_timestamp().alias("load_timestamp"))

def _iso_seconds(col):
    def part(unit):
        m = F.regexp_extract(col, rf"(\d+){unit}", 1)
        return F.when(m != "", m.cast("int")).otherwise(F.lit(0))
    return F.when(col.rlike("^PT"), part("H") * 3600 + part("M") * 60 + part("S"))

HASH_COLS = ["title", "channel_id_hash", "channel_title", "category_id", "category_name",
             "description_redacted", "tags_redacted", "published_at", "duration_seconds",
             "view_count", "like_count", "dislike_count", "comment_count"]

def build_silver(valid_df, categories_df, batch_id):
    w = (Window.partitionBy("video_id", "region_code", "snapshot_date")
               .orderBy(F.col("source_priority").desc(), F.col("bronze_load_ts").desc()))
    df = valid_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")  # dedup
    df = (df.join(F.broadcast(categories_df), "category_id", "left")
            .withColumn("channel_id_hash",
                        hash_id(F.coalesce("channel_id", F.concat(F.lit("title:"), "channel_title"))))
            .withColumn("description_redacted", redact(F.col("description")))
            .withColumn("tags_redacted",
                        F.split(redact(F.regexp_replace("tags", '"', "")), r"\|"))
            .withColumn("duration_seconds", _iso_seconds(F.col("duration"))))
    df = df.withColumn("row_hash", F.sha2(F.concat_ws("||", *[
        F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in HASH_COLS]), 256))
    df = df.withColumn("batch_id", F.lit(batch_id)) 
    df = df.withColumn("load_timestamp", F.current_timestamp())
    return df.select(*SILVER_VIDEO_SCHEMA.names)