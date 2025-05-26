import json, os, sys, requests
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple
from bs4 import BeautifulSoup
import pandas as pd

import warnings
warnings.filterwarnings("ignore")



# ──────────────────────────────────────────────────────────────
# Scraper helper
# ──────────────────────────────────────────────────────────────
HEADERS = {"User-Agent": "tee-watcher/1.0"}

def extract_available_slots(url: str) -> List[Tuple[str, int]]:
    html = requests.get(url, headers=HEADERS, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")

    slots = []
    for row in soup.select("div.row-time"):
        h3 = row.find("h3")
        if not h3:
            continue
        try:
            t_std = datetime.strptime(h3.get_text(strip=True), "%I:%M %p")
        except ValueError:
            continue
        free = len(row.select("div.cell-available"))
        if free:
            slots.append((t_std.strftime("%I:%M %p"), free))
    return sorted(slots, key=lambda x: datetime.strptime(x[0], "%I:%M %p"))


# ──────────────────────────────────────────────────────────────
# Date helpers & course loading
# ──────────────────────────────────────────────────────────────

def next_n_full_weeks(n: int = 3) -> List[date]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())  # start from this week's Monday
    days = []
    for i in range(n * 7):
        days.append(monday + timedelta(days=i))
    return days




def main():
    course_path = Path(__file__).parent / "venues" / "golf_venues.json"
    try:
        COURSES = json.loads(course_path.read_text())
    except Exception as e:
        raise RuntimeError(f"❌ Could not load {course_path}: {e}")

    results = []
    for dia in next_n_full_weeks(3):
        date_iso = dia.isoformat()
        fecha_fmt = dia.strftime("%Y%m%d")

        for club, data in COURSES.items():
            domain = data["domain"]
            booking_id = data["bookingResourceId"]
            fee_groups = data["feeGroupIds"]

            for hoyos_str, fee_id in fee_groups.items():
                url = (
                    f"https://{domain}/guests/bookings/ViewPublicTimesheet.msp"
                    f"?bookingResourceId={booking_id}&selectedDate={date_iso}&feeGroupId={fee_id}"
                )

                for time_str, free in extract_available_slots(url):
                    results.append({
                        "venue": club,
                        "fecha": fecha_fmt,
                        "hora": time_str,
                        "hoyos": int(hoyos_str),
                        "lugares": free,
                        "link": url
                    })

    df = pd.DataFrame(results)
    print(df)
    return df


# ──────────────────────────────────────────────────────────────
# Main logic
# ──────────────────────────────────────────────────────────────


if __name__ == "__main__":
    main()
