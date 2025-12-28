from fastapi import FastAPI, Query
from typing import Optional
import json
import os
from collections import defaultdict
from datetime import datetime

app = FastAPI(title="Well Time Series API")

def load_data():
    """بارگذاری داده‌های تمیز‌شده"""
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "CleanedData.json")
    
    if not os.path.exists(json_path):
        return []
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []

@app.get("/")
def root():
    """بررسی سلامت API"""
    return {"status": "OK", "message": "Well Time Series API"}

@app.get("/api/well/timeseries")
def get_timeseries(
    well_id: int = Query(..., description="Well ID"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD format"),
    event_id: Optional[int] = Query(None, description="Event/Class ID"),
    granularity: str = Query("day", description="Aggregation level: day, week, month")
):
    """
    دریافت سری زمانی برای یک چاه
    
    Parameters:
    - well_id: شناسه چاه (الزامی)
    - start_date: تاریخ شروع (اختیاری)
    - event_id: شناسه رویداد (اختیاری)
    - granularity: سطح تجمیع (روزی، هفتگی، ماهانه)
    """
    try:
        data = load_data()
        
        if not data:
            return {
                "error": "No data available",
                "well_id": well_id,
                "count": 0,
                "points": []
            }
        
        daily_bucket = defaultdict(list)
        
        # فیلتر کردن داده‌ها
        for row in data:
            if row.get("well_id") != well_id:
                continue
            
            if event_id is not None and row.get("class") != event_id:
                continue
            
            try:
                ts = row.get("timestamp", "")
                if not ts:
                    continue