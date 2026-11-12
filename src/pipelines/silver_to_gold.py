"""
Silver -> Gold (star schema) for YouTube Trending Analytics.

Place at: src/pipelines/silver_to_gold.py

Gold tables (workspace.youtube_trending):
  gold_dim_date          PK date_key (yyyyMMdd int)
  gold_dim_region        PK region_code
  gold_dim_category      PK category_id   (-1 = "Unknown" member)
  gold_dim_channel       PK channel_key   (= silver channel_id_hash)
  gold_dim_video         PK video_id
  gold_fact_video_daily  PK (video_id, region_code, snapshot_date)
                         one row = one video trending in one region on one day
FKs (logical, Delta does not enforce them):
  fact.date_key -> dim_date, fact.region_code -> dim_region,
  fact.category_id -> dim_category, fact.channel_key -> dim_channel,
  fact.video_id -> dim_video

Idempotency: every table is loaded with MERGE and only updates a row when its
row_hash changed, so re-running the same date range inserts 0 and updates 0.
Dimensions are Type 1 (overwrite) and only accept attribute changes from a
snapshot that is as new as, or newer than, what they already hold, so an old
backfill cannot overwrite newer attributes.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import tbl, T_SILVER_VIDEO, T_SILVER_CAT
from src.common.audit import PipelineRun
from src.common.delta_utils import _cond, _last_merge_metrics

LAYER = "silver_to_gold"

T_DIM_DATE = tbl("gold_dim_date")
T_DIM_REGION = tbl("gold_dim_region")
T_DIM_CATEGORY = tbl("gold_dim_category")
T_DIM_CHANNEL = tbl("gold_dim_channel")
T_DIM_VIDEO = tbl("gold_dim_video")
T_FACT = tbl("gold_fact_video_daily")

GOLD_TABLES = [T_DIM_DATE, T_DIM_REGION, T_DIM_CATEGORY, T_DIM_CHANNEL, T_DIM_VIDEO, T_FACT]

REGION_NAMES = {
    "PK": "Pakistan", "US": "United States", "IN": "India", "GB": "United Kingdom",
    "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France",
    "JP": "Japan", "BR": "Brazil",
}

# ---- explicit table definitions (DDL) --------------------------------------
DDL = {
    T_DIM_DATE: ("date_key INT NOT NULL, full_date DATE NOT NULL, year INT, quarter INT, month INT, "
                 "month_name STRING, day INT, day_of_week INT, day_name STRING, week_of_year INT, "
                 "is_weekend BOOLEAN, row_hash STRING, load_timestamp TIMESTAMP NOT NULL", None),
    T_DIM_REGION: ("region_code STRING NOT NULL, region_name STRING, row_hash STRING, "
                   "load_timestamp TIMESTAMP NOT NULL", None),
    T_DIM_CATEGORY: ("category_id INT NOT NULL, category_name STRING, row_hash STRING, "
                     "load_timestamp TIMESTAMP NOT NULL", None),
    T_DIM_CHANNEL: ("channel_key STRING NOT NULL, channel_title STRING, last_seen_date DATE, "
                    "row_hash STRING, load_timestamp TIMESTAMP NOT NULL", None),
    T_DIM_VIDEO: ("video_id STRING NOT NULL, title STRING, channel_key STRING, published_at TIMESTAMP, "
                  "duration_seconds INT, last_seen_date DATE, row_hash STRING, "
                  "load_timestamp TIMESTAMP NOT NULL", None),
    T_FACT: ("date_key INT NOT NULL, video_id STRING NOT NULL, region_code STRING NOT NULL, "
             "snapshot_date DATE NOT NULL, category_id INT NOT NULL, channel_key STRING, "
             "view_count BIGINT, like_count BIGINT, dislike_count BIGINT, comment_count BIGINT, "
             "engagement_rate DOUBLE, source_system STRING, batch_id STRING, row_hash STRING, "
             "load_timestamp TIMESTAMP NOT NULL", "snapshot_date, region_code"),  # Liquid Clustering
}


def _ensure_tables(spark):
    for name, (cols, cluster) in DDL.items():
        base = f"CREATE TABLE IF NOT EXISTS {name} ({cols}) USING DELTA"
        if cluster:
            try:
                spark.sql(f"{base} CLUSTER BY ({cluster})")
                continue
            except Exception:           # runtime without Liquid Clustering: plain Delta table
                pass
        spark.sql(base)


def _hash(*cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def _merge(spark, df, target, keys, newer_col=None):
    """Insert new rows; update only when content changed (and, for dims, only from a newer-or-equal snapshot)."""
    change = "t.row_hash <> s.row_hash"
    if newer_col:
        change = (f"(s.{newer_col} > t.{newer_col}) OR "
                  f"(s.{newer_col} = t.{newer_col} AND t.row_hash <> s.row_hash)")
    (DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), _cond(keys))
        .whenMatchedUpdateAll(condition=change)
        .whenNotMatchedInsertAll().execute())
    return _last_merge_metrics(spark, target)


def _latest_per(df, key, extra_order=("load_timestamp",)):
    w = Window.partitionBy(key).orderBy(F.col("snapshot_date").desc(),
                                        *[F.col(c).desc() for c in extra_order])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---- builders ---------------------------------------------------------------
def _build_dim_date(silver):
    d = silver.select(F.col("snapshot_date").alias("full_date")).distinct()
    df = d.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
        F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"), F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"), F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"))
    return (df.withColumn("row_hash", _hash("full_date", "year", "quarter", "month", "month_name", "day",
                                            "day_of_week", "day_name", "week_of_year", "is_weekend"))
              .withColumn("load_timestamp", F.current_timestamp()))


def _build_dim_region(spark, silver):
    mapping = F.create_map(*[F.lit(x) for kv in REGION_NAMES.items() for x in kv])
    df = (silver.select("region_code").distinct()
                .withColumn("region_name", F.coalesce(mapping[F.col("region_code")], F.col("region_code"))))
    return (df.withColumn("row_hash", _hash("region_name"))
              .withColumn("load_timestamp", F.current_timestamp()))


def _build_dim_category(spark):
    cats = spark.table(T_SILVER_CAT).select("category_id", "category_name")
    unknown = spark.createDataFrame([(-1, "Unknown")], "category_id INT, category_name STRING")
    df = cats.unionByName(unknown).dropDuplicates(["category_id"])
    return (df.withColumn("row_hash", _hash("category_name"))
              .withColumn("load_timestamp", F.current_timestamp()))


def _build_dim_channel(silver):
    df = (_latest_per(silver.filter("channel_id_hash IS NOT NULL"), "channel_id_hash")
          .select(F.col("channel_id_hash").alias("channel_key"), "channel_title",
                  F.col("snapshot_date").alias("last_seen_date")))
    return (df.withColumn("row_hash", _hash("channel_title"))
              .withColumn("load_timestamp", F.current_timestamp()))


def _build_dim_video(silver):
    df = (_latest_per(silver, "video_id")
          .select("video_id", "title", F.col("channel_id_hash").alias("channel_key"), "published_at",
                  "duration_seconds", F.col("snapshot_date").alias("last_seen_date")))
    return (df.withColumn("row_hash", _hash("title", "channel_key", "published_at", "duration_seconds"))
              .withColumn("load_timestamp", F.current_timestamp()))


def _build_fact(spark, silver, batch_id):
    cat_ids = spark.table(T_DIM_CATEGORY).select(F.col("category_id").alias("_cid"))
    df = (silver.join(F.broadcast(cat_ids), silver["category_id"] == F.col("_cid"), "left")
          .select(
              F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
              "video_id", "region_code", "snapshot_date",
              F.coalesce(F.col("_cid"), F.lit(-1)).alias("category_id"),       # unknown -> -1 member
              F.col("channel_id_hash").alias("channel_key"),
              "view_count", "like_count", "dislike_count", "comment_count",
              F.when(F.col("view_count") > 0,
                     (F.coalesce("like_count", F.lit(0)) + F.coalesce("comment_count", F.lit(0)))
                     / F.col("view_count")).alias("engagement_rate"),
              "source_system"))
    return (df.withColumn("batch_id", F.lit(batch_id))
              .withColumn("row_hash", _hash("category_id", "channel_key", "view_count", "like_count",
                                            "dislike_count", "comment_count", "source_system"))
              .withColumn("load_timestamp", F.current_timestamp()))


# ---- public API -------------------------------------------------------------
def run_silver_to_gold(spark, start_date, end_date, load_type, batch_id, reset=False, optimize=False):
    """
    Build/refresh Gold for snapshot_date in [start_date, end_date].
    reset=True drops the 6 Gold tables first (use once if old Gold tables have a different schema).
    optimize=True runs OPTIMIZE on every Gold table at the end.
    Raises if any step fails (so a Databricks job goes red); every step is logged first.
    """
    if reset:
        for t in GOLD_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    _ensure_tables(spark)

    param = f"snapshot_date {start_date}..{end_date}"
    silver = (spark.table(T_SILVER_VIDEO)
                   .filter(F.col("snapshot_date").between(start_date, end_date)).cache())
    stats = {}
    try:
        silver_rows = silver.count()

        steps = [  # order matters: dimensions first, fact last
            (T_DIM_DATE,     lambda: _build_dim_date(silver),               ["date_key"],       None),
            (T_DIM_REGION,   lambda: _build_dim_region(spark, silver),      ["region_code"],    None),
            (T_DIM_CATEGORY, lambda: _build_dim_category(spark),            ["category_id"],    None),
            (T_DIM_CHANNEL,  lambda: _build_dim_channel(silver),            ["channel_key"],    "last_seen_date"),
            (T_DIM_VIDEO,    lambda: _build_dim_video(silver),              ["video_id"],       "last_seen_date"),
            (T_FACT,         lambda: _build_fact(spark, silver, batch_id),
                             ["video_id", "region_code", "snapshot_date"], None),
        ]
        for target, build, keys, newer in steps:
            with PipelineRun(spark, LAYER, param, load_type, batch_id, target) as run:
                df = build()
                run.rows_read = df.count()
                run.rows_inserted, run.rows_updated = _merge(spark, df, target, keys, newer)
                stats[target] = {"read": run.rows_read, "inserted": run.rows_inserted,
                                 "updated": run.rows_updated}
        if optimize:
            optimize_gold(spark)
    finally:
        silver.unpersist()

    return {"batch_id": batch_id, "silver_rows": silver_rows,
            "fact_rows": spark.table(T_FACT).count(), "tables": stats}


def optimize_gold(spark):
    """OPTIMIZE every Gold table (compacts files; also applies Liquid Clustering on the fact)."""
    for t in GOLD_TABLES:
        spark.sql(f"OPTIMIZE {t}")
    return GOLD_TABLES


def validate_gold(spark):
    """Integrity checks to show as test evidence. Everything except row_counts should be 0."""
    fact = spark.table(T_FACT)
    keys = ["video_id", "region_code", "snapshot_date"]
    out = {
        "row_counts": {t: spark.table(t).count() for t in GOLD_TABLES},
        "duplicate_fact_keys": fact.groupBy(*keys).count().filter("count > 1").count(),
        "null_fact_keys": fact.filter(" OR ".join(f"{k} IS NULL" for k in keys + ["date_key"])).count(),
    }
    for name, tbl_, fk, pk in [("date", T_DIM_DATE, "date_key", "date_key"),
                               ("region", T_DIM_REGION, "region_code", "region_code"),
                               ("category", T_DIM_CATEGORY, "category_id", "category_id"),
                               ("video", T_DIM_VIDEO, "video_id", "video_id"),
                               ("channel", T_DIM_CHANNEL, "channel_key", "channel_key")]:
        dim = spark.table(tbl_).select(F.col(pk).alias("_pk"))
        out[f"orphan_fact_rows_vs_dim_{name}"] = (
            fact.filter(F.col(fk).isNotNull())
                .join(dim, fact[fk] == F.col("_pk"), "left_anti").count())
    return out
