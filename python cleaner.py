from fastapi import FastAPI, Query
from typing import Optional
import json
import os
from datetime import datetime
from collections import defaultdict

app = FastAPI(title="Well Time Series API")

# --- بارگذاری دیتای تمیز ---
def load_data():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    # توجه: داریم فایل تمیز شده را می‌خوانیم
    json_path = os.path.join(base_dir, "CleanedData.json")
    
    if not os.path.exists(json_path):
        return []
    
    with open(json_path, "r", encoding='utf-8') as f:
        return json.load(f)

@app.get("/")
def read_root():
    return {"status": "Running", "message": "Go to /api/well/timeseries?well_id=1"}

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None),
    event_id: Optional[int] = Query(None),
    granularity: str = Query("day")
):
    data = load_data()
    
    if not data:
        return {"error": "CleanedData.json is missing or empty"}

    points = []
    
    # چون دیتا تمیز است، پردازش خیلی ساده می‌شود
    daily_bucket = defaultdict(list)

    for row in data:
        # فیلتر Well ID
        if row.get("well_id") != well_id:
            continue

        # فیلتر Event ID
        if event_id is not None and row.get("class") != event_id:
            continue

        # پردازش تاریخ (چون فرمت ISO است، ۱۰ کاراکتر اول همیشه تاریخ است)
        # مثال: "2024-01-01T12:00:00" -> "2024-01-01"
        ts_full = row["timestamp"]
        date_str = ts_full.split("T")[0]

        # فیلتر Start Date
        if start_date and date_str < start_date:
            continue
            
        daily_bucket[date_str].append(row)

    # میانگین‌گیری روزانه
    for day, rows in daily_bucket.items():
        if not rows: continue
        
        # جمع‌آوری مقادیر عددی برای میانگین
        sums = defaultdict(float)
        counts = defaultdict(int)
        
        for r in rows:
            for k, v in r.items():
                if k not in ["timestamp", "class", "well_id"] and isinstance(v, (int, float)):
                    sums[k] += v
                    counts[k] += 1
        
        avg_values = {k: round(sums[k]/counts[k], 2) for k in sums}
        
        points.append({
            "name": day,
            "value": avg_values
        })

    # سورت کردن بر اساس تاریخ
    points.sort(key=lambda x: x["name"])

    return {
        "well_id": well_id,
        "count": len(points),
        "points": points[-60:] # ۶۰ روز آخر
    }