import uuid
from datetime import datetime, timezone
from src.config import T_RUN_LOGS
from src.schemas import RUN_LOG_SCHEMA
from src.common.delta_utils import ensure_table

def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

class PipelineRun:
    """with PipelineRun(...) as run: ... run.rows_inserted = n"""
    def __init__(self, spark, layer, parameter, load_type, batch_id, target_table=None):
        self.spark, self.layer, self.parameter = spark, layer, parameter
        self.load_type, self.batch_id, self.target = load_type, batch_id, target_table
        self.rows_read = self.rows_inserted = self.rows_updated = self.rows_quarantined = 0

    def __enter__(self):
        ensure_table(self.spark, T_RUN_LOGS, RUN_LOG_SCHEMA)
        self.run_id, self.start = str(uuid.uuid4()), _now()
        return self

    def __exit__(self, exc_type, exc, tb):
        end = _now()
        row = (self.run_id, self.batch_id, self.layer, self.load_type, self.parameter,
               self.target, self.start, end, "FAILURE" if exc_type else "SUCCESS",
               self.rows_read, self.rows_inserted, self.rows_updated,
               self.rows_quarantined, str(exc)[:2000] if exc_type else None, end)
        (self.spark.createDataFrame([row], RUN_LOG_SCHEMA)
             .write.format("delta").mode("append").saveAsTable(T_RUN_LOGS))
        return False  # re-raise; the caller decides to continue with the next file