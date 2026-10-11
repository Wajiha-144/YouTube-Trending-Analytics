from pyspark.sql import functions as F
from src.config import PII_SALT

EMAIL_RE  = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
HANDLE_RE = r"(?<!\w)@[A-Za-z0-9_.]{2,}"

def hash_id(col):
    return F.sha2(F.concat(F.lit(PII_SALT), col), 256)

def redact(col):
    return F.regexp_replace(F.regexp_replace(col, EMAIL_RE, "[REDACTED_EMAIL]"), HANDLE_RE, "[REDACTED_HANDLE]")