from fastapi import FastAPI, Query
from typing import Optional
import json
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict

app = FastAPI(title="Well Time Series API")

# --------------------------------------------------
# 1. بارگذاری ایمن داده‌ها
# --------------------------------------------------
def load_data():
    """بارگذاری داده از فایل JSON"""
    base_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(base_dir, "MData.json")

    if not os.path.exists(json_path):
        return []

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []

def normalize_key(key: str) -> str:
    """تبدیل نام کلیدها به فرمت استاندارد"""
    return key.lower().replace("-", "_")

# --------------------------------------------------
# 2. موتور تبدیل تاریخ با Regex
# --------------------------------------------------
def parse_smart_timestamp(raw_ts: str) -> Optional[datetime]:
    """
    تبدیل فرمت‌های مختلف تاریخ به datetime
    مثال: "day1 12:01:00 AM", "00:02:00", "day 2 12:00"
    """
    if not raw_ts or not isinstance(raw_ts, str):
        return None

    BASE_DATE = datetime(2024, 1, 1)
    clean_ts = raw_ts.strip()

    try:
        # استراتژی 1: الگوی "day X" با Regex
        day_match = re.search(r'day\s*[-_]?\s*(\d+)', clean_ts, re.IGNORECASE)

        if day_match:
            day_num = int(day_match.group(1))
            time_part = clean_ts[day_match.end():].strip()

            t = datetime.min.time()
            if time_part:
                for fmt in ["%I:%M:%S %p", "%H:%M:%S", "%H:%M", "%I:%M %p"]:
                    try:
                        t = datetime.strptime(time_part, fmt).time()
                        break
                    except ValueError:
                        continue

            final_dt = BASE_DATE + timedelta(days=day_num - 1)