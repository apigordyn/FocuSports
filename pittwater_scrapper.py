import os
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_values
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import time
import random
import sys

nest_asyncio.apply()

# ──────────────────────────────────────────────────────────────
# Configuración general
# ──────────────────────────────────────────────────────────────
DAYS_AHEAD = int(sys.argv[1]) if len(sys.argv) > 1 else 0

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
# Scraper Pittwater (YepBooking)
# ──────────────────────────────────────────────────────────────
async def scrape_one_day(DAYS_AHEAD):
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
    ]
    selected_ua = random.choice(user_agents)
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=selected_ua)
        page = await context.new_page()
        await page.goto("https://pittwater-rsl-futsal.yepbooking.com.au/", timeout=60000)

        # Avanzá DAYS_AHEAD veces con click Next
        for i in range(DAYS_AHEAD):
            next_button = await page.query_selector("#nextDateMover")
            if next_button:
                await next_button.click()
                await page.wait_for_timeout(int(random.uniform(1800, 3200)))  # entre 1.8 y 3.2s

        await page.wait_for_selector("h3", timeout=12000)
        date_header = await page.query_selector("h3")
        date_text = await date_header.inner_text()
        current_date = parser.parse(date_text).strftime("%d-%m-%Y")
        print(f"Scrapeando día: {current_date}")

        slots = await page.query_selector_all("a.empty")
        print(f"Slots encontrados: {len(slots)}")

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

        await page.close()
        await context.close()
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
    borrar_registros_viejos()
    df_raw = await scrape_one_day(DAYS_AHEAD)
    df = expand_consecutive_blocks(df_raw)
    # Unificá outputs y columnas
    guardar_futsal_df(df)
    print(f"Guardados {len(df)} registros de futsal Pittwater.")
    end = time.time()
    print(f"\nTiempo total Pittwater dia {DAYS_AHEAD}: {end - start:.2f} segundos")
    return df

# Para ejecutar manualmente (agregá esto en tu cron, script, etc):
if __name__ == "__main__":
    asyncio.run(main())
