from fastapi import FastAPI, Query
from typing import Optional
import json
import os
from collections import defaultdict

app = FastAPI(title="Well Time Series API")

def load_data():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "CleanedData.json")
    if not os.path.exists(json_path):
        return []
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/")
def root():
    return {"status": "OK"}

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(...),
    start_date: Optional[str] = Query(None),
    event_id: Optional[int] = Query(None),
    granularity: str = Query("day")
):
    data = load_data()
    if not data:
        return {"error": "No data"}

    daily_bucket = defaultdict(list)

    for row in data:
        if row.get("well_id") != well_id:
            continue

        if event_id is not None and row.get("class") != event_id:
            continue

        ts = row["timestamp"]
        day = ts.split("T")[0]

        if start_date and day < start_date:
            continue

        daily_bucket[day].append(row)

    points = []

    for day, rows in daily_bucket.items():
        sums = defaultdict(float)
        counts = defaultdict(int)

        for r in rows:
            for k, v in r.items():
                if k in ["timestamp", "class", "well_id"]:
                    continue
                if isinstance(v, (int, float)):
                    sums[k] += v
                    counts[k] += 1

        avg = {k: round(sums[k] / counts[k], 2) for k in sums}
        points.append({"name": day, "value": avg})

    points.sort(key=lambda x: x["name"])

    return {
        "well_id": well_id,
        "granularity": granularity,
        "count": len(points),
        "points": points
    }
