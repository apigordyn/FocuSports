import os
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_values
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import time

nest_asyncio.apply()

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
    # Borra la tabla si existe
    cur.execute("DROP TABLE IF EXISTS futsal_horarios;")
    # Crea la tabla con el constraint correcto
    cur.execute("""
        CREATE TABLE futsal_horarios (
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
                for fecha in fechas:
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
                        "venue": "KIKOFF",
                        "fecha": dt.strftime("%Y%m%d"),
                        "hora": dt.strftime("%I:%M %p"),
                        "minutos": duration,
                        "court": "N/A",
                        "link": booking_url
                    })
    return pd.DataFrame(all_rows)

# ──────────────────────────────────────────────────────────────
# Scraper Pittwater (YepBooking)
# ──────────────────────────────────────────────────────────────
async def scrape_pittwater_multiple_days(days_to_scrap=28):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://pittwater-rsl-futsal.yepbooking.com.au/", timeout=60000)

        all_data = []
        for day_index in range(days_to_scrap):
            #await page.wait_for_selector("a.empty", timeout=10000)
            #await page.wait_for_selector("h3")
            date_header = await page.query_selector("h3")
            date_text = await date_header.inner_text()
            current_date = parser.parse(date_text).strftime("%d-%m-%Y")

            slots = await page.query_selector_all("a.empty")

            for slot in slots:
                title = await slot.get_attribute("title") or await slot.get_attribute("aria-label")
                lc = await slot.get_attribute("lc")
                if title and "Available" in title and lc:
                    try:
                        hora_inicio, hora_fin = title.split(" - ")[0].split("–")
                        court_number = lc.split("|")[0].strip()

                        all_data.append({
                            "venue": "Pittwater RSL",
                            "fecha": current_date,
                            "hora_inicio": hora_inicio.strip(),
                            "hora_fin": hora_fin.strip(),
                            "court": f"Court {court_number}"
                        })
                    except:
                        continue

            # Click to next day if not the last day
            if day_index < days_to_scrap - 1:
                next_button = await page.query_selector("#nextDateMover")
                if next_button:
                    await next_button.click()
                    await page.wait_for_timeout(3000)

        await browser.close()
        return pd.DataFrame(all_data)


# Función para expandir bloques

def expand_consecutive_blocks(df):
    expanded_rows = []
    for (venue, court, fecha), group in df.groupby(["venue", "court", "fecha"]):
        slots = []
        for _, row in group.iterrows():
            start_str = f"{row['fecha']} {row['hora_inicio']}"
            end_str = f"{row['fecha']} {row['hora_fin']}"
            start_dt = datetime.strptime(start_str, "%d-%m-%Y %I:%M%p")
            end_dt = datetime.strptime(end_str, "%d-%m-%Y %I:%M%p")
            slots.append((start_dt, end_dt))

        slots.sort()
        available = set()
        for start, end in slots:
            t = start
            while t + timedelta(minutes=30) <= end:
                available.add(t)
                t += timedelta(minutes=30)
        available = sorted(available)

        for i in range(len(available)):
            t0 = available[i]
            for duration in [30, 60, 90, 120]:
                end_t = t0 + timedelta(minutes=duration)
                if all(t0 + timedelta(minutes=30 * k) in available for k in range(duration // 30)):
                    expanded_rows.append({
                        "venue": venue,
                        "fecha": t0.strftime("%Y%m%d"),
                        "hora": t0.strftime("%I:%M %p"),
                        "minutos": duration,
                        "court": court,
                        "link": "https://pittwater-rsl-futsal.yepbooking.com.au/"
                    })
    return pd.DataFrame(expanded_rows)

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
async def main():
    start = time.time()
    crear_tabla_futsal()
    df_kikoff = scrape_kikoff()
    df_raw = await scrape_pittwater_multiple_days(days_to_scrap=DAYS_TO_SCRAPE)
    df_pittwater = expand_consecutive_blocks(df_raw)
    # Unificá outputs y columnas
    df = pd.concat([df_kikoff, df_pittwater], ignore_index=True)
    guardar_futsal_df(df)
    print(f"Guardados {len(df)} registros de futsal.")
    end = time.time()
    print(f"\nTiempo total: {end - start:.2f} segundos")
    return df

# Para ejecutar manualmente (agregá esto en tu cron, script, etc):
if __name__ == "__main__":
    asyncio.run(main())
