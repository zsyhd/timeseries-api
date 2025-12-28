import json
import re
from datetime import datetime, timedelta

INPUT_FILE = "MData.json"
OUTPUT_FILE = "CleanedData.json"
BASE_DATE = datetime(2024, 1, 1)
RECORDS_PER_DAY = 1440

def parse_timestamp(raw_ts, index):
    if raw_ts is None:
        return None
    
    # بخش ۱: پشتیبانی از اعداد (Timestamp اکسل)
    if isinstance(raw_ts, (int, float)):
        try:
            ts_val = raw_ts
            # تبدیل میلیثانیه به ثانیه
            if abs(ts_val) > 20000000000:
                ts_val = ts_val / 1000
            dt = datetime.fromtimestamp(ts_val)
            # حل مشکل سال ۱۹۰۰
            if dt.year < 2000:
                dt = dt.replace(year=2024, month=1, day=1) + timedelta(days=(dt.day - 1))
            return dt
        except:
            pass
    
    # تبدیل به رشته برای حالتهای متنی
    raw_str = str(raw_ts).strip()
    
    # حالت ۱: dayX (مثلاً "day 5 14:30:45")
    day_match = re.search(r'day\s*[-_]?\s*(\d+)', raw_str, re.IGNORECASE)
    if day_match:
        day_num = int(day_match.group(1))
        time_part = raw_str[day_match.end():].strip()
        t = datetime.min.time()
        
        for fmt in ["%I:%M:%S %p", "%H:%M:%S", "%H:%M"]:
            try:
                t = datetime.strptime(time_part, fmt).time()
                break
            except:
                pass
        
        return datetime.combine(
            (BASE_DATE + timedelta(days=day_num - 1)).date(),
            t
        )
    
    # حالت ۲: فقط ساعت (مثلاً "14:30:45")
    if ":" in raw_str:
        day_num = index // RECORDS_PER_DAY
        try:
            t = datetime.strptime(raw_str, "%H:%M:%S").time()
        except:
            try:
                t = datetime.strptime(raw_str, "%H:%M").time()
            except:
                return None
        
        return datetime.combine(
            (BASE_DATE + timedelta(days=day_num)).date(),
            t
        )
    
    return None

# اجرای عملیات
print("🔄 Running cleaner...")
try:
    with open(INPUT_FI
