from fastapi import FastAPI, Query
from typing import Any, Dict, List, Optional, Tuple
import json
import os
from datetime import datetime
from collections import defaultdict


app = FastAPI(title="Well Time Series API (CleanedData.json)")


def load_cleaned_data() -> List[Dict[str, Any]]:
    base_dir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(base_dir, "CleanedData.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def parse_iso_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def bucket_key(dt: datetime, granularity: str) -> str:
    g = granularity.lower()
    if g == "minute":
        return dt.strftime("%Y-%m-%d %H:%M")
    if g == "hour":
        return dt.strftime("%Y-%m-%d %H:00")
    # default day
    return dt.strftime("%Y-%m-%d")


def safe_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    return None


@app.get("/")
def root():
    return {
        "status": "Running",
        "hint": "First run cleaner.py to generate CleanedData.json, then call /api/well/timeseries"
    }


@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    event_id: Optional[int] = Query(None, description="Filter by class/event id"),
    granularity: str = Query("day", description="minute | hour | day"),
    limit: Optional[int] = Query(None, description="Return last N buckets (after sorting)"),
):
    data = load_cleaned_data()
    if not data:
        return {"error": "CleanedData.json not found or empty. Run cleaner.py first."}

    g = granularity.lower()
    if g not in ("minute", "hour", "day"):
        return {"error": "Invalid granularity. Use minute, hour, or day."}

    sd = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    ed = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    # 1) filter + parse dt
    filtered: List[Tuple[datetime, Dict[str, Any]]] = []
    for row in data:
        if int(row.get("well_id", -1)) != well_id:
            continue

        if event_id is not None:
            try:
                if int(row.get("class", -999)) != int(event_id):
                    continue
            except Exception:
                continue

        dt = parse_iso_dt(row.get("timestamp", ""))
        if not dt:
            continue

        if sd and dt.date() < sd:
            continue
        if ed and dt.date() > ed:
            continue

        filtered.append((dt, row))

    if not filtered:
        return {"well_id": well_id, "granularity": g, "count": 0, "points": []}

    # 2) bucket
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for dt, row in filtered:
        k = bucket_key(dt, g)
        buckets[k].append(row)

    # 3) aggregate mean per numeric field
    points: List[Dict[str, Any]] = []
    for k, rows in buckets.items():
        sums: Dict[str, float] = defaultdict(float)
        counts: Dict[str, int] = defaultdict(int)

        for r in rows:
            for kk, vv in r.items():
                if kk in ("timestamp", "well_id", "class"):
                    continue
                fv = safe_float(vv)
                if fv is None:
                    continue
                sums[kk] += fv
                counts[kk] += 1

        agg = {kk: round(sums[kk] / counts[kk], 4) for kk in sums.keys() if counts[kk] > 0}
        points.append({"name": k, "value": agg})

    points.sort(key=lambda x: x["name"])

    if limit is not None and limit > 0:
        points = points[-limit:]

    return {
        "well_id": well_id,
        "granularity": g,
        "count": len(points),
        "points": points,
    }
