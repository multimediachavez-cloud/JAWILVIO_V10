def safe_int(value):
    if value in (None, ''):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def safe_float(value, default=None):
    if value in (None, ''):
        return default
    try:
        return float(str(value).strip())
    except Exception:
        return default
