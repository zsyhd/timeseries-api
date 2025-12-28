from fastapi import FastAPI, Query
from typing import Optional
import json
import os
from datetime import datetime
from collections import defaultdict

app = FastAPI(title="Well Time Series API - Safe Mode")

# --- تابع امن برای خواندن فایل ---
def load_data():
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "MData.json")
    
    if not os.path.exists(json_path):
        print(f"Error: File not found at {json_path}")
        return []
    
    try:
        with open(json_path, "r", encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return []

def normalize_key(key: str) -> str:
    return key.lower().replace("-", "_")

@app.get("/")
def read_root():
    return {
        "status": "Running",
        "message": "To see data, go to: /api/well/timeseries?well_id=1"
    }

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None),
    event_id: Optional[int] = Query(None),
    granularity: str = Query("day")
):
    data = load_data()
    
    if not data:
        return {"error": "No data available (File missing or empty)"}

    # 1. فیلتر کردن داده‌ها (با بلوک try-except برای جلوگیری از کرش)
    filtered = []
    for row in data:
        try:
            # دریافت تاریخ با پشتیبانی از غلط املایی احتمالی
            # اگر هیچکدام نبود، مقدار پیش‌فرض None می‌شود
            raw_ts = row.get("Timesteap") or row.get("timestamp") or row.get("Timestamp")
            
            # اگر تاریخ وجود نداشت یا رشته خالی بود، این سطر را رد کن (کرش نکن!)
            if not raw_ts or not isinstance(raw_ts, str):
                continue
                
            # تلاش برای جدا کردن تاریخ
            ts_parts = raw_ts.split()
            if not ts_parts:
                continue
            
            date_part = ts_parts[0] # تاریخ بدون ساعت

            # فیلتر تاریخ (اگر کاربر خواسته بود)
            if start_date:
                row_date = datetime.strptime(date_part, "%Y-%m-%d")
                filter_date = datetime.strptime(start_date, "%Y-%m-%d")
                if row_date < filter_date:
                    continue

            # فیلتر کلاس
            if event_id is not None:
                if row.get("class") != event_id:
                    continue
            
            # اضافه کردن یک کلید امن برای استفاده در مرحله بعد
            row["_safe_date"] = date_part
            filtered.append(row)

        except Exception as e:
            # اگر هر مشکلی در پردازش این سطر بود، نادیده‌اش بگیر و برو بعدی
            continue

    # 2. تجمیع روزانه
    daily_bucket = defaultdict(list)
    for row in filtered:
        # اینجا دیگه مطمئنیم _safe_date وجود داره چون بالا ساختیمش
        daily_bucket[row["_safe_date"]].append(row)

    points = []
    
    # 3. محاسبه میانگین
    for day, rows in daily_bucket.items():
        values_map = defaultdict(list)
        
        for r in rows:
            for k, v in r.items():
                # ستون‌های غیر عددی و کمکی را رد کن
                if k in ["Timesteap", "timestamp", "class", "well_id", "_safe_date"]: 
                    continue
                if isinstance(v, (int, float)):
                    values_map[normalize_key(k)].append(v)
        
        # محاسبه میانگین (با شرط اینکه لیست خالی نباشه)
        aggregated = {}
        for k, v_list in values_map.items():
            if v_list:
                aggregated[k] = round(sum(v_list) / len(v_list), 2)
        
        points.append({
            "name": day,
            "value": aggregated
        })

    # 4. خروجی نهایی
    points_sorted = sorted(points, key=lambda x: x["name"])
    final_points = points_sorted[-60:]

    return {
        "well_id": well_id,
        "count": len(final_points),
        "points": final_points
    }