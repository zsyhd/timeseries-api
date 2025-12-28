from fastapi import FastAPI, Query
from typing import Optional, List, Dict, Any
import json
import os
from datetime import datetime
from collections import defaultdict

app = FastAPI(title="Well Time Series API")

# --- توابع کمکی ---

def load_data():
    # استفاده از آدرس‌دهی نسبی برای پیدا کردن فایل در هر محیطی (Vercel یا Local)
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "MData.json")
    
    try:
        with open(json_path, "r", encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def normalize_key(key: str) -> str:
    """تبدیل نام ستون‌ها به فرمت استاندارد"""
    return key.lower().replace("-", "_")

# --- روت اصلی برای جلوگیری از ارور Not Found ---
@app.get("/")
def read_root():
    return {
        "status": "Running",
        "message": "To see data, go to: /api/well/timeseries?well_id=1"
    }

# --- روت اصلی برنامه (دقیقاً طبق منطق شما) ---
@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    event_id: Optional[int] = Query(None, description="Event Class ID"),
    granularity: str = Query("day")
):
    data = load_data()
    
    if not data:
        return {"error": "No data found or MData.json is missing"}

    # 1. فیلتر کردن داده‌ها
    filtered = []
    for row in data:
        # فیلتر تاریخ
        if start_date:
            try:
                # هندل کردن فرمت تاریخ (فقط بخش روز)
                ts_str = row.get("Timesteap", "").split()[0]
                row_date = datetime.strptime(ts_str, "%Y-%m-%d")
                filter_date = datetime.strptime(start_date, "%Y-%m-%d")
                if row_date < filter_date:
                    continue
            except:
                pass # اگر تاریخ خراب بود رد شو

        # فیلتر کلاس (Event ID)
        if event_id is not None:
            if row.get("class") != event_id:
                continue

        filtered.append(row)

    # 2. تجمیع روزانه (Daily Bucket)
    daily_bucket = defaultdict(list)
    for row in filtered:
        ts = row.get("Timesteap", "unknown").split()[0] # گرفتن تاریخ بدون ساعت
        daily_bucket[ts].append(row)

    points = []
    
    # 3. محاسبه میانگین برای هر روز
    for day, rows in daily_bucket.items():
        if day == "unknown": continue
        
        values_map = defaultdict(list)
        
        # جمع‌آوری مقادیر عددی
        for r in rows:
            for k, v in r.items():
                # ستون‌های غیر عددی را رد کن
                if k in ["Timesteap", "class", "well_id"]: 
                    continue
                if isinstance(v, (int, float)):
                    values_map[normalize_key(k)].append(v)
        
        # میانگین‌گیری
        aggregated = {}
        for k, v_list in values_map.items():
            if v_list:
                aggregated[k] = round(sum(v_list) / len(v_list), 2)
        
        points.append({
            "name": day,
            "value": aggregated
        })

    # 4. مرتب‌سازی و محدود کردن به ۶۰ رکورد آخر
    points_sorted = sorted(points, key=lambda x: x["name"])
    final_points = points_sorted[-60:]

    return {
        "well_id": well_id,
        "granularity": granularity,
        "filters": {
            "start_date": start_date,
            "event_id": event_id
        },
        "points": final_points
    }