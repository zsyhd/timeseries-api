import json
import re
from datetime import datetime, timedelta

INPUT_FILE = "MData.json"
OUTPUT_FILE = "CleanedData.json"

BASE_DATE = datetime(2024, 1, 1)
RECORDS_PER_DAY = 1440  # هر ۱۴۴۰ رکورد = یک روز

def parse_timestamp(raw_ts, index):
    if not raw_ts:
        return None

    raw_ts = str(raw_ts).strip()

    # --- حالت ۱: dayX داریم ---
    day_match = re.search(r'day\s*[-_]?\s*(\d+)', raw_ts, re.IGNORECASE)
    if day_match:
        day_num = int(day_match.group(1))
        time_part = raw_ts[day_match.end():].strip()

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

    # --- حالت ۲: فقط ساعت داریم ---
    if ":" in raw_ts:
        day_num = index // RECORDS_PER_DAY
        try:
            t = datetime.strptime(raw_ts, "%H:%M:%S").time()
        except:
            t = datetime.strptime(raw_ts, "%H:%M").time()

        return datetime.combine(
            (BASE_DATE + timedelta(days=day_num)).date(),
            t
        )

    return None


with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

cleaned = []

for idx, row in enumerate(data):
    raw_ts = row.get("Timesteap") or row.get("timestamp")

    dt = parse_timestamp(raw_ts, idx)
    if not dt:
        continue

    row["timestamp"] = dt.isoformat()
    cleaned.append(row)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(cleaned, f, ensure_ascii=False, indent=2)

print(f"✅ Cleaned {len(cleaned)} records into {OUTPUT_FILE}")
