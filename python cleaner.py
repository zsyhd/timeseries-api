import json
import re
from datetime import datetime, timedelta, time
from typing import Any, Dict, List, Optional


BASE_DATE = datetime(2024, 1, 1)

TIME_ONLY_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$")
DAY_TIME_RE = re.compile(r"day\s*[-_]?\s*(\d+)\s*(.*)$", re.IGNORECASE)


def normalize_key(key: str) -> str:
    return key.strip().lower().replace("-", "_")


def try_parse_time_part(s: str) -> Optional[time]:
    s = (s or "").strip()
    if not s:
        return time(0, 0, 0)

    # try a few common formats
    for fmt in ("%I:%M:%S %p", "%I:%M %p", "%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    return None


def parse_timestamp_smart(
    raw_ts: Any,
    last_dt: Optional[datetime],
) -> Optional[datetime]:
    """
    Handles:
      - "day1 12:01:00 AM" / "day 2 13:20" / "day-3 00:10:00"
      - "00:02:00" (time-only)  -> inferred day via rollover
      - ISO "2024-01-01T12:01:00"
    """
    if not raw_ts or not isinstance(raw_ts, str):
        return None

    ts = raw_ts.strip()

    # ISO
    if "T" in ts:
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            pass

    # dayX ...
    m = DAY_TIME_RE.match(ts)
    if m:
        day_num = int(m.group(1))  # day1 => 1
        time_part = (m.group(2) or "").strip()
        t = try_parse_time_part(time_part)
        if t is None:
            return None
        day_date = (BASE_DATE + timedelta(days=day_num - 1)).date()
        return datetime.combine(day_date, t)

    # time-only HH:MM(:SS)
    m2 = TIME_ONLY_RE.match(ts)
    if m2:
        hh = int(m2.group(1))
        mm = int(m2.group(2))
        ss = int(m2.group(3) or 0)
        t = time(hh, mm, ss)

        if last_dt is None:
            # first time-only point => base date
            return datetime.combine(BASE_DATE.date(), t)

        # infer day rollover: if time goes "backwards", advance day
        candidate = datetime.combine(last_dt.date(), t)
        if candidate < last_dt:
            candidate = candidate + timedelta(days=1)
        return candidate

    return None


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def clean_rows(raw_rows: List[Dict[str, Any]], default_well_id: int = 1) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    last_dt: Optional[datetime] = None

    for row in raw_rows:
        raw_ts = row.get("Timesteap") or row.get("Timestamp") or row.get("timestamp")
        dt = parse_timestamp_smart(raw_ts, last_dt)
        if dt is None:
            continue

        last_dt = dt

        out: Dict[str, Any] = {
            "timestamp": dt.isoformat(timespec="seconds"),
            "well_id": int(row.get("well_id", default_well_id) or default_well_id),
            "class": int(row.get("class", 0) or 0),
        }

        for k, v in row.items():
            if k in ("Timesteap", "Timestamp", "timestamp", "well_id", "class"):
                continue
            nk = normalize_key(k)
            fv = to_float(v)
            if fv is not None:
                out[nk] = fv

        cleaned.append(out)

    return cleaned


def main():
    in_path = "MData.json"
    out_path = "CleanedData.json"

    with open(in_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("MData.json must be a JSON array of rows")

    cleaned = clean_rows(raw)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"OK: {len(raw)} -> {len(cleaned)} rows written to {out_path}")


if __name__ == "__main__":
    main()
