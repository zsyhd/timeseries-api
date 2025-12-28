from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict

app = FastAPI(
    title="Well Time Series API",
    description="API حرفه‌ای برای داده‌های سری زمانی چاه (دقیقه‌ای)",
    version="3.0.0"
)

def load_data():
    """بارگذاری داده تمیز شده"""
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "MData_Cleaned.json")

    if not os.path.exists(json_path):
        raise HTTPException(
            status_code=500,
            detail="فایل MData_Cleaned.json یافت نشد. ابتدا 'python preprocess_data.py' را اجرا کنید"
        )

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطا: {str(e)}")

@app.get("/")
def root():
    """صفحه اصلی"""
    return {
        "status": "running",
        "version": "3.0.0",
        "data_frequency": "minute-by-minute",
        "message": "API آماده دریافت درخواست است",
        "endpoints": {
            "/api/well/timeseries": "دریافت سری زمانی",
            "/api/health": "وضعیت سیستم",
            "/api/stats": "آمار کلی",
            "/docs": "مستندات Swagger"
        }
    }

@app.get("/api/health")
def health():
    """بررسی سلامت"""
    try:
        data = load_data()
        timestamps = [r['timestamp'] for r in data if 'timestamp' in r]

        return {
            "status": "healthy",
            "data_quality": "cleaned",
            "records": len(data),
            "frequency": "1 minute",
            "time_range": {
                "start": min(timestamps) if timestamps else None,
                "end": max(timestamps) if timestamps else None
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stats")
def statistics():
    """آمار کلی دیتاست"""
    data = load_data()

    # آمار کلاس‌ها
    classes = defaultdict(int)
    for r in data:
        classes[r.get('class', 'unknown')] += 1

    # آمار سنسورها
    sensors = ['p_pdg', 'p_tpt', 't_tpt', 'p_mon_ckp', 't_jus_ckp', 'p_jus_ckgl', 'qgl']
    sensor_stats = {}

    for sensor in sensors:
        values = [r[sensor] for r in data if r.get(sensor) is not None]
        if values:
            sensor_stats[sensor] = {
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "mean": round(sum(values) / len(values), 2),
                "samples": len(values)
            }

    return {
        "total_records": len(data),
        "data_frequency": "1 minute",
        "classes": dict(classes),
        "sensors": sensor_stats
    }

@app.get("/api/well/timeseries")
def timeseries(
    well_id: int = Query(1, description="شناسه چاه"),
    start_time: Optional[str] = Query(None, description="زمان شروع (YYYY-MM-DD HH:MM:SS)"),
    end_time: Optional[str] = Query(None, description="زمان پایان (YYYY-MM-DD HH:MM:SS)"),
    class_id: Optional[int] = Query(None, description="فیلتر کلاس"),
    aggregation: str = Query("minute", description="تجمیع: minute, hour, day"),
    limit: int = Query(1000, ge=1, le=10000, description="حداکثر تعداد نقاط")
):
    """
    دریافت سری زمانی

    داده‌ها با فاصله 1 دقیقه هستند.
    می‌توانید با aggregation آنها را ساعتی یا روزانه کنید.
    """
    data = load_data()

    # فیلتر
    filtered = []
    for r in data:
        if r.get('well_id') != well_id:
            continue

        timestamp = r.get('timestamp', '')

        if start_time and timestamp < start_time:
            continue

        if end_time and timestamp > end_time:
            continue

        if class_id is not None and r.get('class') != class_id:
            continue

        filtered.append(r)

    if not filtered:
        return {
            "well_id": well_id,
            "aggregation": aggregation,
            "count": 0,
            "message": "داده‌ای یافت نشد",
            "points": []
        }

    # تجمیع
    if aggregation == "minute":
        # بدون تجمیع - داده خام
        points = []
        for r in filtered[:limit]:
            values = {k: v for k, v in r.items() 
                     if k not in ['timestamp', 'well_id', 'class', 'original_timestamp']}
            points.append({
                "timestamp": r['timestamp'],
                "values": values
            })
    else:
        # تجمیع ساعتی یا روزانه
        groups = defaultdict(list)

        for r in filtered:
            ts = r.get('timestamp', '')

            if aggregation == "hour":
                key = ts[:13] + ":00:00"  # YYYY-MM-DD HH:00:00
            elif aggregation == "day":
                key = ts[:10] + " 00:00:00"  # YYYY-MM-DD 00:00:00
            else:
                key = ts

            groups[key].append(r)

        # محاسبه میانگین
        points = []
        for time_key in sorted(groups.keys())[:limit]:
            rows = groups[time_key]

            sensor_vals = defaultdict(list)
            for r in rows:
                for k, v in r.items():
                    if k in ['timestamp', 'well_id', 'class', 'original_timestamp']:
                        continue
                    if v is not None and isinstance(v, (int, float)):
                        sensor_vals[k].append(v)

            aggregated = {
                k: round(sum(v) / len(v), 2)
                for k, v in sensor_vals.items() if v
            }

            points.append({
                "timestamp": time_key,
                "values": aggregated,
                "sample_count": len(rows)
            })

    return {
        "well_id": well_id,
        "aggregation": aggregation,
        "count": len(points),
        "total_filtered": len(filtered),
        "time_range": {
            "start": points[0]['timestamp'] if points else None,
            "end": points[-1]['timestamp'] if points else None
        },
        "points": points
    }

if __name__ == "__main__":
    import uvicorn
    print("=" * 70)
    print("🚀 Well Time Series API")
    print("=" * 70)
    print("✅ داده تمیز شده (بدون NULL)")
    print("✅ فاصله زمانی: 1 دقیقه")
    print("✅ تجمیع: دقیقه‌ای، ساعتی، روزانه")
    print("=" * 70)
    print("🌐 http://localhost:8000")
    print("📖 http://localhost:8000/docs")
    print("=" * 70)

    uvicorn.run(app, host="0.0.0.0", port=8000)
