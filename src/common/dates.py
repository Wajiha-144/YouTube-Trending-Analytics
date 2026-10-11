from datetime import datetime, timedelta

def daterange(start: str, end: str):
    d, e = (datetime.strptime(x, "%Y-%m-%d").date() for x in (start, end))
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)