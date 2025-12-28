import json
import re
from datetime import datetime, timedelta

# تنظیمات پایه
BASE_YEAR = 2024
BASE_MONTH = 1
BASE_DAY = 1

def clean_time(raw_time):
    """
    تبدیل فرمت‌های مختلف زمان به فرمت استاندارد ISO
    مثال: "day1 12:01:00 AM" -> "2024-01-01T00:01:00"
    """
    if not raw_time or not isinstance(raw_time, str):
        return None

    # حذف فاصله‌های اضافی
    raw_time = raw_time.strip()

    # پیدا کردن روز با Regex
    day_match = re.search(r'day(\d+)', raw_time, re.IGNORECASE)
    day_offset = int(day_match.group(1)) - 1 if day_match else 0

    # استخراج قسمت زمان
    time_str = raw_time
    if "day" in raw_time.lower():
        parts = raw_time.split(' ', 1)
        if len(parts) > 1:
            time_str = parts[1]

    try:
        # پارس کردن زمان
        if "M" in time_str.upper():  # AM/PM
            t = datetime.strptime(time_str.strip(), "%I:%M:%S %p")
        else:
            t = datetime.strptime(time_str.strip(), "%H:%M:%S")

        # ساخت تاریخ نهایی
        final_date = datetime(BASE_YEAR, BASE_MONTH, BASE_DAY) + timedelta(days=day_offset)
        final_date = final_date.replace(hour=t.hour, minute=t.minute, second=t.second)

        return final_date.isoformat()

    except Exception as e:
        print(f"خطا در پردازش تاریخ '{raw_time}': {e}")
        return None

def main():
    """تابع اصلی برای تمیز کردن داده‌ها"""
    print("=" * 70)
    print("شروع پردازش داده‌ها...")
    print("=" * 70)

    # بارگذاری داده
    try:
        with open("MData.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✓ {len(data)} رکورد از MData.json بارگذاری شد")
    except FileNotFoundError:
        print("✗ فایل MData.json پیدا نشد!")
        return
    except json.JSONDecodeError:
        print("✗ فایل MData.json فرمت صحیح JSON ندارد!")
        return

    cleaned_data = []
    skipped_count = 0

    # پردازش هر رکورد
    for idx, row in enumerate(data, 1):
        # پیدا کردن فیلد timestamp با نام‌های مختلف
     
