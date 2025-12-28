import json
import re
from datetime import datetime, timedelta

# تنظیمات پایه (فرض می‌کنیم شروع داده‌ها از اول سال ۲۰۲۴ است)
BASE_YEAR = 2024
BASE_MONTH = 1
BASE_DAY = 1

def clean_time(raw_time):
    """
    این تابع زمان‌های عجیب و غریب را به فرمت استاندارد ISO تبدیل می‌کند.
    مثل: '2024-01-01T12:01:00'
    """
    if not raw_time or not isinstance(raw_time, str):
        return None

    # حذف فاصله‌های اضافی
    raw_time = raw_time.strip()

    # مدل ۱: "day1 12:01:00 AM"
    # استخراج روز و ساعت
    day_match = re.search(r'day(\d+)', raw_time, re.IGNORECASE)
    day_offset = int(day_match.group(1)) - 1 if day_match else 0

    # پیدا کردن ساعت
    # مدل ساعت خالی: "00:02:00"
    # مدل ساعت با AM/PM: "12:01:00 AM"
    
    time_str = raw_time
    if "day" in raw_time:
        parts = raw_time.split(' ', 1)
        if len(parts) > 1:
            time_str = parts[1]
    
    try:
        # تلاش برای پارس کردن ساعت
        if "M" in time_str.upper(): # مثل AM یا PM
            t = datetime.strptime(time_str.strip(), "%I:%M:%S %p")
        else:
            t = datetime.strptime(time_str.strip(), "%H:%M:%S")
            
        # ساخت تاریخ نهایی
        final_date = datetime(BASE_YEAR, BASE_MONTH, BASE_DAY) + timedelta(days=day_offset)
        final_date = final_date.replace(hour=t.hour, minute=t.minute, second=t.second)
        
        return final_date.isoformat() # فرمت استاندارد: YYYY-MM-DDTHH:MM:SS
        
    except Exception as e:
        print(f"Skipping invalid time: {raw_time}")
        return None

def main():
    try:
        with open("MData.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("MData.json not found!")
        return

    cleaned_data = []
    
    for row in data:
        # ۱. اصلاح نام فیلد (Timesteap -> timestamp)
        raw_ts = row.get("Timesteap") or row.get("Timestamp") or row.get("timestamp")
        
        standard_ts = clean_time(raw_ts)
        
        if standard_ts:
            new_row = {
                "timestamp": standard_ts,     # نام درست + فرمت درست
                "well_id": row.get("well_id", 1), # اگر نداشت پیش‌فرض ۱ بذار
                "class": row.get("class", 0)      # اگر نداشت پیش‌فرض ۰ (نرمال)
            }
            
            # کپی کردن بقیه ستون‌های عددی (فشار، دما و...)
            for k, v in row.items():
                if k not in ["Timesteap", "Timestamp", "timestamp", "class", "well_id"]:
                    # فقط اعداد را نگه دار
                    try:
                        float(v)
                        new_row[k.lower().replace("-", "_")] = v # استاندارد سازی نام ستون‌ها
                    except:
                        pass
            
            cleaned_data.append(new_row)

    # ذخیره فایل جدید
    with open("CleanedData.json", "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2)
    
    print(f"Success! Converted {len(data)} rows to {len(cleaned_data)} clean rows in 'CleanedData.json'.")

if __name__ == "__main__":
    main()