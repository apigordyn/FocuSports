import time
import datetime
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from urllib.parse import urlparse, parse_qs
import sqlite3

nest_asyncio.apply()

VENUES = [
    "oxford-fall-racquet-club",
    "allambie-heights-tennis",
    "narraweena-tennis-club",
    "collaroy-tc",
]

def crear_tabla_sqlite(db_path="disponibilidad.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT,
            fecha TEXT,
            cancha TEXT,
            hora TEXT,
            link TEXT,
            UNIQUE(venue, fecha, cancha, hora)
        )
    """)
    conn.commit()
    conn.close()

async def extraer_disponibilidad(venue, fecha="20250528"):
    url = f"https://www.tennisvenues.com.au/booking/{venue}?date={fecha}"
    base = "https://www.tennisvenues.com.au"
    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url)
            await page.wait_for_selector("td.TimeCell.Available a", timeout=8000)
        except Exception as e:
            print(f"❌ Error en {venue}-{fecha}: {e}")
            await browser.close()
            return pd.DataFrame()  # Devolver vacío y continuar

        enlaces = await page.query_selector_all("td.TimeCell.Available a")
        for a in enlaces:
            hora = await a.inner_text()
            href = await a.get_attribute("href")
            if not href:
                continue
            full_url = f"{base}{href}"
            cancha = parse_qs(urlparse(href).query).get("id", ["Desconocida"])[0]

            resultados.append({
                "venue": venue,
                "fecha": fecha,
                "cancha": cancha,
                "hora": hora,
                "link": full_url
            })

        await browser.close()

    df = pd.DataFrame(resultados).drop_duplicates()
    return df

def guardar_df_sqlite(df, db_path="disponibilidad.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for venue in df["venue"].unique():
        for fecha in df["fecha"].unique():
            cur.execute("DELETE FROM horarios WHERE venue=? AND fecha=?", (venue, fecha))
    for _, row in df.iterrows():
        cur.execute("""
            INSERT OR IGNORE INTO horarios (venue, fecha, cancha, hora, link)
            VALUES (?, ?, ?, ?, ?)
        """, (row.venue, row.fecha, row.cancha, row.hora, row.link))
    conn.commit()
    conn.close()

# ⏩ Scraping concurrente (por fecha)
async def scrapear_concurrente(venues, fechas, db_path="disponibilidad.db", max_concurrent=4):
    from asyncio import Semaphore, create_task, gather

    crear_tabla_sqlite(db_path)
    sem = Semaphore(max_concurrent)

    async def scrapear_venue_fecha(venue, fecha):
        async with sem:
            t0 = time.time()
            print(f"[INICIO] {venue} - {fecha} - {t0:.2f}")
            df = await extraer_disponibilidad(venue, fecha)
            guardar_df_sqlite(df, db_path)
            t1 = time.time()
            print(f"[FIN]    {venue} - {fecha} - {t1:.2f} (Duración: {t1-t0:.2f}s)")
            await asyncio.sleep(4)   # <-- Espacia los requests 2 segundos

    tareas = [
        create_task(scrapear_venue_fecha(venue, fecha))
        for venue in venues
        for fecha in fechas
    ]
    await gather(*tareas)

# === USO ===
if __name__ == "__main__":
    hoy = datetime.date.today()
    fechas = [(hoy + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(7)]

    start = time.time()
    asyncio.run(scrapear_concurrente(VENUES, fechas, max_concurrent=4))
    end = time.time()
    print(f"\nTiempo total: {end - start:.2f} segundos")