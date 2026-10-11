import uuid
from datetime import datetime, timezone
from src.config import ALL_REGIONS

def resolve_params(load_type, start_date, end_date, regions, batch_id):
    today = datetime.now(timezone.utc).date().isoformat()
    if load_type == "incremental":
        start_date = start_date or today
        end_date = end_date or start_date
    elif not (start_date and end_date):
        raise ValueError("full/backfill loads require start_date and end_date")
    regions = [r.strip().upper() for r in regions.split(",") if r.strip()] or ALL_REGIONS
    batch_id = batch_id or f"{load_type}_{start_date}_{end_date}_{uuid.uuid4().hex[:8]}"
    return start_date, end_date, regions, batch_id