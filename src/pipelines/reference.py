from pyspark.sql import functions as F
from src.config import *
from src.schemas import *
from src.common.audit import PipelineRun
from src.common.delta_utils import ensure_table, merge_upsert

def load_categories(spark, load_type, batch_id, path=f"{RAW_ROOT}/reference/categories.csv"):
    ensure_table(spark, T_SILVER_CAT, SILVER_CATEGORY_SCHEMA)
    with PipelineRun(spark, "reference_to_silver", path, load_type, batch_id, T_SILVER_CAT) as run:
        df = (spark.read.schema(CATEGORY_CSV_SCHEMA).option("header", True).csv(path)
                   .select(F.col("category_id").cast("int"), "category_name",
                           F.current_timestamp().alias("load_timestamp")))
        run.rows_read = df.count()
        run.rows_inserted, run.rows_updated = merge_upsert(
            spark, df, T_SILVER_CAT, ["category_id"], change_col="category_name")