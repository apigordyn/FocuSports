import os
import time
import datetime
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from urllib.parse import urlparse, parse_qs
import psycopg2
from psycopg2.extras import execute_values

nest_asyncio.apply()

VENUES = [
    "oxford-fall-racquet-club",
    "allambie-heights-tennis",
    "narraweena-tennis-club",
    "collaroy-tc",
    "bareena-park-tc",
    "manly-lawn-tc",
    "koobilya-st-tennis-court",
    "wyatt-park-tc",
    "forestville-park-tc"
    
]

# 1. Conexión a Postgres
def get_conn():
    # Tomá el string de conexión de la variable de entorno o editá acá
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

# 2. Crear tabla si no existe
def crear_tabla_postgres():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS horarios;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            cancha TEXT,
            hora TEXT,
            link TEXT,
            UNIQUE(venue, fecha, cancha, hora)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# 3. Scraper
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
            return pd.DataFrame()  # Vacío

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

# 4. Guardado en Postgres (Bulk)
def guardar_df_postgres(df):
    if df.empty:
        return
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            # Borrar todos los registros de ese venue/fecha
            venues = df['venue'].unique()
            fechas = df['fecha'].unique()
            for venue in venues:
                for fecha in fechas:
                    cur.execute("DELETE FROM horarios WHERE venue=%s AND fecha=%s", (venue, fecha))
            # Insertar todo el dataframe de una vez
            rows = list(df[['venue', 'fecha', 'cancha', 'hora', 'link']].itertuples(index=False, name=None))
            execute_values(
                cur,
                "INSERT INTO horarios (venue, fecha, cancha, hora, link) VALUES %s ON CONFLICT DO NOTHING",
                rows
            )
    conn.close()

# 5. Scraping concurrente
async def scrapear_concurrente(venues, fechas, max_concurrent=4):
    from asyncio import Semaphore, create_task, gather

    crear_tabla_postgres()
    sem = Semaphore(max_concurrent)

    async def scrapear_venue_fecha(venue, fecha):
        async with sem:
            t0 = time.time()
            print(f"[INICIO] {venue} - {fecha} - {t0:.2f}")
            df = await extraer_disponibilidad(venue, fecha)
            guardar_df_postgres(df)
            t1 = time.time()
            print(f"[FIN]    {venue} - {fecha} - {t1:.2f} (Duración: {t1-t0:.2f}s)")
            await asyncio.sleep(4)   # <-- Espaciá requests para evitar baneos

    tareas = [
        create_task(scrapear_venue_fecha(venue, fecha))
        for venue in venues
        for fecha in fechas
    ]
    await gather(*tareas)

# 6. Main
if __name__ == "__main__":
    hoy = datetime.date.today()
    fechas = [(hoy + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(28)] # 7 días
    start = time.time()
    asyncio.run(scrapear_concurrente(VENUES, fechas, max_concurrent=2))
    end = time.time()
    print(f"\nTiempo total: {end - start:.2f} segundos")
