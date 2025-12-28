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

    # --- بخش جدید: پشتیبانی از اعداد (Timestamp اکسل) ---
    # این همان چیزی است که ۶۰۰۰ رکورد شما را زنده می‌کند
    if isinstance(raw_ts, (int, float)):
        try:
            ts_val = raw_ts
            # تبدیل میلی‌ثانیه به ثانیه
            if abs(ts_val) > 20000000000:
                ts_val = ts_val / 1000
            
            dt = datetime.fromtimestamp(ts_val)
            
            # حل مشکل سال ۱۹۰۰
            if dt.year < 2000:
                dt = dt.replace(year=2024, month=1, day=1) + timedelta(days=(dt.day - 1))
            return dt
        except:
            pass

    # تبدیل به رشته برای حالت‌های متنی
    raw_str = str(raw_ts).strip()

    # --- حالت ۱: dayX ---
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

    # --- حالت ۲: فقط ساعت ---
    if ":" in raw_str:
        day_num = index // RECORDS_PER_DAY
        try:
            t = datetime.strptime(raw_str, "%H:%M:%S").time()
        except:
            t = datetime.strptime(raw_str, "%H:%M").time()

        return datetime.combine(
            (BASE_DATE + timedelta(days=day_num)).date(),
            t
        )

    return None

# --- اجرای عملیات ---
print("Running cleaner...")
try:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned = []
    for idx, row in enumerate(data):
        # گرفتن مقدار خام زمان
        raw_ts = row.get("Timesteap") or row.get("timestamp") or row.get("Timestamp")

        dt = parse_timestamp(raw_ts, idx)
        if dt:
            # ساخت رکورد تمیز
            new_row = row.copy()
            new_row["timestamp"] = dt.isoformat()
            
            # استانداردسازی کلیدها (همه حروف کوچک)
            final_row = {}
            for k, v in new_row.items():
                if k not in ["Timesteap", "Timestamp"]: # حذف ستون‌های قدیمی
                    clean_key = k.lower().replace("-", "_")
                    final_row[clean_key] = v
            
            cleaned.append(final_row)

    # ذخیره فایل نهایی
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"✅ Success! Converted {len(cleaned)} records. (Should be > 6000)")

except Exception as e:
    print(f"❌ Error: {e}")