import datetime as dt
if not hasattr(dt, "UTC"):
    dt.UTC = dt.timezone.utc
