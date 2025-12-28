from fastapi import FastAPI, Query
from typing import Optional
import json
import os
from datetime import datetime
from collections import defaultdict

app = FastAPI(title="Well Time Series API - Final Safe Version")

# --------------------------------------------------
# Load Data (Safe)
# --------------------------------------------------
def load_data():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "MData.json")

    if not os.path.exists(json_path):
        return []

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def normalize_key(key: str) -> str:
    return key.lower().replace("-", "_")

# --------------------------------------------------
# Parse Timesteap (REAL FIX)
# --------------------------------------------------
def parse_timestamp(raw_ts: str) -> Optional[datetime]:
    """
    پشتیبانی از:
    - 'day1 12:01:00 AM'
    - '00:02:00'
    """
    base_date = datetime(2024, 1, 1)

    try:
        parts = raw_ts.split()

        # حالت: day1 12:01:00 AM
        if len(parts) >= 3 and parts[0].lower().startswith("day"):
            time_part = f"{parts[1]} {parts[2]}"
            t = datetime.strptime(time_part, "%I:%M:%S %p").time()
            return datetime.combine(base_date.date(), t)

        # حالت: 00:02:00
        if ":" in raw_ts:
            t = datetime.strptime(raw_ts, "%H:%M:%S").time()
            return datetime.combine(base_date.date(), t)

    except Exception:
        return None

    return None

# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.get("/")
def read_root():
    return {
        "status": "Running",
        "message": "Use /api/well/timeseries?well_id=1"
    }

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    event_id: Optional[int] = Query(None),
    granularity: str = Query("day")
):
    data = load_data()

    if not data:
        return {"error": "No data available"}

    # --------------------------------------------------
    # 1. Filtering + Safe datetime
    # --------------------------------------------------
    filtered = []

    for row in data:
        raw_ts = row.get("Timesteap") or row.get("timestamp") or row.get("Timestamp")
        if not raw_ts or not isinstance(raw_ts, str):
            continue

        dt = parse_timestamp(raw_ts)
        if not dt:
            continue

        # start_date filter
        if start_date:
            try:
                filter_date = datetime.strptime(start_date, "%Y-%m-%d")
                if dt.date() < filter_date.date():
                    continue
            except Exception:
                pass

        # event filter
        if event_id is not None and row.get("class") != event_id:
            continue

        row["_safe_datetime"] = dt
        filtered.append(row)

    # --------------------------------------------------
    # 2. Daily Aggregation (REAL)
    # --------------------------------------------------
    daily_bucket = defaultdict(list)

    for row in filtered:
        day_key = row["_safe_datetime"].date().isoformat()
        daily_bucket[day_key].append(row)

    # --------------------------------------------------
    # 3. Average Calculation
    # --------------------------------------------------
    points = []

    for day, rows in daily_bucket.items():
        values_map = defaultdict(list)

        for r in rows:
            for k, v in r.items():
                if k in ["Timesteap", "timestamp", "class", "well_id", "_safe_datetime"]:
                    continue
                if isinstance(v, (int, float)):
                    values_map[normalize_key(k)].append(v)

        aggregated = {
            k: round(sum(v) / len(v), 2)
            for k, v in values_map.items() if v
        }

        points.append({
            "name": day,
            "value": aggregated
        })

    points_sorted = sorted(points, key=lambda x: x["name"])
    final_points = points_sorted[-60:]

    return {
        "well_id": well_id,
        "granularity": granularity,
        "count": len(final_points),
        "points": final_points
    }
