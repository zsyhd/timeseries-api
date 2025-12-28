from fastapi import FastAPI, Query
from typing import Optional
import json
import os
import re  # اضافه کردن کتابخانه Regex برای شکار کردن اعداد
from datetime import datetime, timedelta
from collections import defaultdict

app = FastAPI(title="Well Time Series API - Regex Powered")

# --------------------------------------------------
# 1. بارگذاری ایمن داده‌ها
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
# 2. موتور تبدیل تاریخ (Regex Engine)
# --------------------------------------------------
def parse_smart_timestamp(raw_ts: str) -> Optional[datetime]:
    """
    این تابع هر مدلی از نوشتن روز را می‌فهمد.
    مثال‌هایی که الان کار می‌کنند:
    - "day1 12:00"
    - "Day 2 12:00" (با فاصله)
    - "day-3 12:00"
    """
    if not raw_ts or not isinstance(raw_ts, str):
        return None
    
    # تاریخ مبدا (اول سال ۲۰۲۴)
    BASE_DATE = datetime(2024, 1, 1)
    
    clean_ts = raw_ts.strip()

    try:
        # --- استراتژی ۱: پیدا کردن الگوی "day X" با Regex ---
        # این الگو میگه: کلمه day باشه، شاید فاصله باشه، بعدش عدد باشه
        day_match = re.search(r'day\s*[-_]?\s*(\d+)', clean_ts, re.IGNORECASE)
        
        if day_match:
            # عدد روز را استخراج کن (مثلاً 2)
            day_num = int(day_match.group(1))
            
            # حالا سعی کن ساعت را پیدا کنی
            # هر چیزی که بعد از الگوی day+عدد مانده باشد، ساعته
            time_part = clean_ts[day_match.end():].strip()
            
            # اگر ساعت خالی بود، پیش‌فرض 00:00:00
            t = datetime.min.time() 
            
            if time_part:
                # تلاش برای خواندن فرمت‌های مختلف ساعت
                for fmt in ["%I:%M:%S %p", "%H:%M:%S", "%H:%M", "%I:%M %p"]:
                    try:
                        t = datetime.strptime(time_part, fmt).time()
                        break
                    except:
                        pass
            
            # محاسبه تاریخ نهایی: مبدا + (تعداد روز - ۱)
            final_dt = BASE_DATE + timedelta(days=day_num - 1)
            return datetime.combine(final_dt.date(), t)

        # --- استراتژی ۲: فقط ساعت (مثل 00:02:00) ---
        # اگر کلمه day نبود ولی : داشتیم
        if ":" in clean_ts and len(clean_ts) < 15:
             # تلاش برای پارس کردن ساعت
             for fmt in ["%H:%M:%S", "%H:%M"]:
                 try:
                     t = datetime.strptime(clean_ts, fmt).time()
                     return datetime.combine(BASE_DATE.date(), t)
                 except:
                     pass

        # --- استراتژی ۳: فرمت استاندارد ایزو ---
        if "T" in clean_ts:
             return datetime.fromisoformat(clean_ts)

    except Exception:
        return None

    return None

# --------------------------------------------------
# 3. روت‌ها
# --------------------------------------------------
@app.get("/")
def read_root():
    return {"status": "Running", "message": "Use /api/well/timeseries?well_id=1"}

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None),
    event_id: Optional[int] = Query(None),
    granularity: str = Query("day")
):
    data = load_data()
    if not data:
        return {"error": "No data available"}

    # --- فیلتر و پردازش ---
    filtered = []
    
    # شمارنده برای دیباگ (که اگر صفر شد بفهمیم)
    debug_parsed_count = 0 
    
    for row in data:
        raw_ts = row.get("Timesteap") or row.get("timestamp") or row.get("Timestamp")
        
        # تبدیل تاریخ با موتور جدید Regex
        dt = parse_smart_timestamp(raw_ts)
        
        if not dt:
            continue
            
        debug_parsed_count += 1

        # فیلتر تاریخ شروع
        if start_date:
            try:
                if dt.date() < datetime.strptime(start_date, "%Y-%m-%d").date():
                    continue
            except:
                pass

        # فیلتر کلاس
        if event_id is not None:
            try:
                if int(row.get("class", -1)) != int(event_id):
                    continue
            except:
                pass

        row["_clean_date"] = dt.date().isoformat()
        filtered.append(row)

    # --- اگر هیچ دیتایی پارس نشد ---
    if debug_parsed_count == 0:
        return {"error": "Could not parse any dates. Check date format in MData.json"}

    # --- تجمیع روزانه ---
    daily_bucket = defaultdict(list)
    for row in filtered:
        daily_bucket[row["_clean_date"]].append(row)

    # --- محاسبه میانگین ---
    points = []
    for day, rows in daily_bucket.items():
        values_map = defaultdict(list)
        for r in rows:
            for k, v in r.items():
                if k in ["Timesteap", "timestamp", "class", "well_id", "_clean_date"]:
                    continue
                
                # تبدیل هوشمند اعداد (حتی اگر رشته باشند)
                val = v
                if isinstance(v, str):
                    try:
                        val = float(v)
                    except:
                        continue
                
                if isinstance(val, (int, float)):
                    values_map[normalize_key(k)].append(val)

        aggregated = {
            k: round(sum(v) / len(v), 2)
            for k, v in values_map.items() if v
        }

        points.append({"name": day, "value": aggregated})

    # مرتب‌سازی و خروجی
    points_sorted = sorted(points, key=lambda x: x["name"])
    
    # نکته: اگر می‌خواهی "همه" روزها را ببینی، عدد ۶۰ را بردار
    final_points = points_sorted # [-60:] را برداشتم تا همه را ببینی

    return {
        "well_id": well_id,
        "granularity": granularity,
        "count": len(final_points),
        "points": final_points
    }