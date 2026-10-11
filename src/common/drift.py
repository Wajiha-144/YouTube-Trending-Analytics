from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, MapType, StringType
from src.config import T_DRIFT_LOGS
from src.schemas import DRIFT_LOG_SCHEMA
from src.common.delta_utils import ensure_table

def expected_struct_keys(schema, prefix=""):
    out = {prefix: {fld.name for fld in schema.fields}}
    for fld in schema.fields:
        if isinstance(fld.dataType, StructType):
            out.update(expected_struct_keys(fld.dataType, f"{prefix}.{fld.name}" if prefix else fld.name))
    return out

def detect_new_json_keys(df, json_col, schema):
    found = {}
    for path, expected in expected_struct_keys(schema).items():
        col = F.col(json_col) if path == "" else F.get_json_object(F.col(json_col), f"$.{path}")
        keys = {r["k"] for r in df.select(F.explode(F.map_keys(
                F.from_json(col, MapType(StringType(), StringType())))).alias("k")).distinct().collect()}
        if keys - expected:
            found[path or "<root>"] = sorted(keys - expected)
    return found

def detect_new_csv_columns(spark, path, schema):
    from src.schemas import RAW_LINE_SCHEMA
    import csv, io
    header = spark.read.schema(RAW_LINE_SCHEMA).text(path).first()[0]
    cols = next(csv.reader(io.StringIO(header)))
    return sorted(set(cols) - set(schema.names))

def record_drift(spark, batch_id, source_file, scope, new_cols):
    ensure_table(spark, T_DRIFT_LOGS, DRIFT_LOG_SCHEMA)
    row = (batch_id, source_file, scope, ",".join(new_cols), datetime.now(timezone.utc).replace(tzinfo=None))
    spark.createDataFrame([row], DRIFT_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(T_DRIFT_LOGS)