import json, os, sys, requests
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import warnings
warnings.filterwarnings("ignore")

HEADERS = {"User-Agent": "tee-watcher/1.0"}

def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

def crear_tabla_golf_postgres():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS golf_horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            hora TEXT,
            hoyos INTEGER,
            lugares INTEGER,
            link TEXT,
            UNIQUE(venue, fecha, hora, hoyos)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def guardar_golf_df_postgres(df):
    if df.empty:
        return
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            venues = df['venue'].unique()
            fechas = df['fecha'].unique()
            for venue in venues:
                for fecha in fechas:
                    cur.execute("DELETE FROM golf_horarios WHERE venue=%s AND fecha=%s", (venue, fecha))
            rows = list(df[['venue', 'fecha', 'hora', 'hoyos', 'lugares', 'link']].itertuples(index=False, name=None))
            execute_values(
                cur,
                "INSERT INTO golf_horarios (venue, fecha, hora, hoyos, lugares, link) VALUES %s ON CONFLICT DO NOTHING",
                rows
            )
    conn.close()

def borrar_registros_viejos():
    conn = get_conn()
    cur = conn.cursor()
    hoy = datetime.date.today().strftime("%Y%m%d")
    cur.execute("DELETE FROM horarios_golf WHERE fecha < %s;", (hoy,))
    conn.commit()
    cur.close()
    conn.close()

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

def next_n_full_weeks(n: int = 3) -> List[date]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
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

    crear_tabla_golf_postgres()
    borrar_registros_viejos()
    results = []
    for dia in next_n_full_weeks(4):
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
    guardar_golf_df_postgres(df)
    return df

if __name__ == "__main__":
    main()
