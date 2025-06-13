import os
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_values
import asyncio
import time

# ──────────────────────────────────────────────────────────────
# Configuración general
# ──────────────────────────────────────────────────────────────
DAYS_TO_SCRAPE = 28

KIKOFF_OWNER = "d84901c1"
KIKOFF_DURATION_IDS = {
    60: "39069226",
    75: "39496231",
    90: "40598384",
    120: "60569034"
}
KIKOFF_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# ──────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ──────────────────────────────────────────────────────────────
def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

def crear_tabla_futsal():
    conn = get_conn()
    cur = conn.cursor()
    # Crea la tabla con el constraint correcto
    cur.execute("""
        CREATE TABLE IF NOT EXISTS futsal_horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            hora TEXT,
            minutos INTEGER,
            court TEXT,
            link TEXT,
            UNIQUE(venue, fecha, hora, court, minutos)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def borrar_registros_viejos():
    conn = get_conn()
    cur = conn.cursor()
    hoy = date.today().strftime("%Y%m%d")
    cur.execute("DELETE FROM futsal_horarios WHERE fecha < %s;", (hoy,))
    conn.commit()
    cur.close()
    conn.close()

def guardar_futsal_df(df):
    if df.empty:
        print("No hay datos de futsal para guardar.")
        return
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            venues = df['venue'].unique()
            fechas = df['fecha'].unique()
            for venue in venues:
                print(venue)
                for fecha in fechas:
                    print(fecha)
                    cur.execute("DELETE FROM futsal_horarios WHERE venue=%s AND fecha=%s", (venue, fecha))
            rows = list(df[['venue', 'fecha', 'hora', 'minutos', 'court', 'link']].itertuples(index=False, name=None))
            execute_values(
                cur,
                "INSERT INTO futsal_horarios (venue, fecha, hora, minutos, court, link) VALUES %s ON CONFLICT DO NOTHING",
                rows
            )
    conn.close()

# ──────────────────────────────────────────────────────────────
# Scraper KIKOFF (Squarespace Scheduling)
# ──────────────────────────────────────────────────────────────
def scrape_kikoff():
    base_url = "https://app.squarespacescheduling.com/api/scheduling/v1/availability/times"
    today = date.today()
    day_range = [today + timedelta(days=i) for i in range(DAYS_TO_SCRAPE)]
    all_rows = []
    for duration, appointment_id in KIKOFF_DURATION_IDS.items():
        for day in day_range:
            params = {
                "owner": KIKOFF_OWNER,
                "appointmentTypeId": appointment_id,
                "calendarId": "any",
                "startDate": day.isoformat(),
                "maxDays": 1,
                "timezone": "Australia/Sydney"
            }
            try:
                response = requests.get(base_url, params=params, headers=KIKOFF_HEADERS)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"KIKOFF error: {e}")
                continue
            for _, slots in data.items():
                for slot in slots:
                    dt = parser.parse(slot["time"])
                    time_iso = slot["time"]
                    time_encoded = requests.utils.quote(time_iso)
                    booking_url = (
                        f"https://app.squarespacescheduling.com/schedule/"
                        f"{KIKOFF_OWNER}/appointment/{appointment_id}/calendar/any/datetime/"
                        f"{time_encoded}?categories%5B%5D=Pitch+Hire"
                    )
                    all_rows.append({
                        "venue": "Kikoff Harbord",
                        "fecha": dt.strftime("%Y%m%d"),
                        "hora": dt.strftime("%I:%M %p"),
                        "minutos": duration,
                        "court": "N/A",
                        "link": booking_url
                    })
    return pd.DataFrame(all_rows)

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
async def main():
    start = time.time()
    crear_tabla_futsal()
    borrar_registros_viejos()
    df = scrape_kikoff()
    # Unificá outputs y columnas
    guardar_futsal_df(df)
    print(f"Guardados {len(df)} registros de Kikoff.")
    end = time.time()
    print(f"\nTiempo total Kikoff: {end - start:.2f} segundos")
    return df

# Para ejecutar manualmente (agregá esto en tu cron, script, etc):
if __name__ == "__main__":
    asyncio.run(main())
