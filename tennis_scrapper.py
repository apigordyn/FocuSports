import os
import time
import datetime
import nest_asyncio
import asyncio
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from playwright.async_api import async_playwright
import re

nest_asyncio.apply()

# ------------------------- SCRAPERS POR VENUE -------------------------

async def extraer_disponibilidad_little_alfred(venue, fecha="20250815"):
    url = "https://www.littlealfredtennis.com.au/booknow"
    base = "https://littlealfred.intrac.com.au"
    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(url)

            try:
                await page.wait_for_selector("input[type='checkbox']", timeout=5000)
                checkbox = await page.query_selector("input[type='checkbox']")
                if checkbox:
                    await checkbox.click()
                    await page.wait_for_timeout(2000)
            except:
                pass

            await page.wait_for_selector("iframe", timeout=8000)
            iframe_element = await page.query_selector("iframe")
            iframe = await iframe_element.content_frame()

            if not iframe:
                raise Exception("No se pudo acceder al iframe")

            target_date = datetime.datetime.strptime(fecha, "%Y%m%d").date()
            today = datetime.date.today()

            if target_date != today:
                target_date_str = target_date.strftime("%Y-%m-%d")
                day_num = target_date.day
                day_links = await iframe.query_selector_all("a")

                for day_link in day_links:
                    href = await day_link.get_attribute("href")
                    text = await day_link.inner_text()
                    if (href and target_date_str in href) or \
                       (text.strip() == str(day_num) and href and target_date_str in href):
                        await day_link.click()
                        await iframe.wait_for_timeout(2000)
                        break

            await iframe.wait_for_timeout(2000)

        except Exception as e:
            print(f"❌ Error en {venue}-{fecha}: {e}")
            await browser.close()
            return pd.DataFrame()

        enlaces = await iframe.query_selector_all("a.book")

        for a in enlaces:
            href = await a.get_attribute("href")
            if not href or "reserve.cfm" not in href:
                continue

            match = re.search(r"date=([^&']+)&start=([^&']+)&court=(\d+)", href)
            if not match:
                continue

            fecha_str, hora_str, court_id = match.groups()
            link_date_fmt = datetime.datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%Y%m%d")
            if link_date_fmt != fecha:
                continue

            try:
                t_std = datetime.datetime.strptime(hora_str, "%H:%M")
                hora = t_std.strftime("%I:%M %p")
            except ValueError:
                hora = hora_str

            encoded_params = f"reserve.cfm%3Flocation%3D81_date%3D{fecha_str}_start%3D{hora_str}_court%3D{court_id}"
            full_url = f"{base}/tennis/login.cfm?return={encoded_params}"

            cancha = {
                "489": "Court 1",
                "490": "Court 2",
                "491": "Court 3"
            }.get(court_id, f"Court {court_id}")

            resultados.append({
                "venue": venue,
                "fecha": fecha,
                "cancha": cancha,
                "hora": hora,
                "link": full_url
            })

        await browser.close()

    return procesar_dataframe(resultados, venue)


async def extraer_disponibilidad_sks(venue, fecha="20250815"):
    url_base = "https://sks.intrac.com.au"
    fecha_formatted = f"{fecha[:4]}-{fecha[4:6]}-{fecha[6:8]}"
    url = f"{url_base}/school/facility.cfm?location=166&date={fecha_formatted}"
    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(url)
            await page.wait_for_selector("table", timeout=8000)
        except Exception as e:
            print(f"❌ Error en {venue}-{fecha}: {e}")
            await browser.close()
            return pd.DataFrame()

        enlaces = await page.query_selector_all("a[href*='facility.cfm'][href*='start=']")

        for a in enlaces:
            href = await a.get_attribute("href")
            hora = await a.inner_text()

            match = re.search(r"date=([^&]+)&start=([^&]+)&.*facility=(\d+)", href or "")
            if not match:
                continue

            fecha_str, hora_str, facility_id = match.groups()
            link_date_fmt = datetime.datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%Y%m%d")
            if link_date_fmt != fecha:
                continue

            try:
                t_std = datetime.datetime.strptime(hora_str, "%H:%M")
                hora_formatted = t_std.strftime("%I:%M %p")
            except ValueError:
                hora_formatted = hora_str

            full_url = f"{url_base}/school/{href}"

            cancha = {
                "205": "Court 1",
                "206": "Court 2",
                "207": "Court 3",
                "208": "Court 4",
                "209": "Court 5",
                "210": "Court 6"
            }.get(facility_id, f"Court {facility_id}")

            resultados.append({
                "venue": venue,
                "fecha": fecha,
                "cancha": cancha,
                "hora": hora_formatted,
                "link": full_url
            })

        await browser.close()

    return procesar_dataframe(resultados, venue)

# ------------------------- PROCESAMIENTO Y GUARDADO -------------------------

def procesar_dataframe(resultados, venue):
    df = pd.DataFrame(resultados).drop_duplicates()

    if not df.empty:
        df['hora_dt'] = pd.to_datetime(df['hora'].str.strip().str.upper(), format='%I:%M %p')
        df['hora_min'] = df['hora_dt'].dt.hour * 60 + df['hora_dt'].dt.minute
        df = df.sort_values(['venue', 'fecha', 'cancha', 'hora_min'])

        for (venue_g, fecha_g, cancha_g), group in df.groupby(['venue', 'fecha', 'cancha']):
            horas = sorted(group['hora_min'].tolist())
            durs = [30] * len(horas)

            for i in range(len(horas)):
                actual = horas[i]
                dur = 30
                j = i + 1
                while j < len(horas) and horas[j] == actual + 30:
                    dur += 30
                    actual += 30
                    j += 1
                durs[i] = dur

            df.loc[group.index, 'duracion_max_min'] = durs

        df.drop(columns=['hora_dt', 'hora_min'], inplace=True)

    return df

# ------------------------- DB FUNCTIONS -------------------------

def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

def crear_tabla_postgres():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            cancha TEXT,
            hora TEXT,
            duracion_max_min INTEGER,
            link TEXT,
            UNIQUE(venue, fecha, cancha, hora)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def borrar_registros_viejos():
    conn = get_conn()
    cur = conn.cursor()
    hoy = datetime.date.today().strftime("%Y%m%d")
    cur.execute("DELETE FROM horarios WHERE fecha < %s;", (hoy,))
    conn.commit()
    cur.close()
    conn.close()

def guardar_df_postgres(df):
    if df.empty:
        return
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            venues = df['venue'].unique()
            fechas = df['fecha'].unique()
            for venue in venues:
                for fecha in fechas:
                    cur.execute("DELETE FROM horarios WHERE venue=%s AND fecha=%s", (venue, fecha))
            df['duracion_max_min'] = df['duracion_max_min'].where(pd.notnull(df['duracion_max_min']), None)
            rows = list(df[['venue', 'fecha', 'cancha', 'hora', 'duracion_max_min', 'link']].itertuples(index=False, name=None))
            execute_values(
                cur,
                "INSERT INTO horarios (venue, fecha, cancha, hora, duracion_max_min, link) VALUES %s ON CONFLICT DO NOTHING",
                rows
            )
    conn.close()

# ------------------------- SCRAPING CONCURRENTE -------------------------

NUEVOS_VENUES = {
    "little-alfred": extraer_disponibilidad_little_alfred,
    "sks-tennis": extraer_disponibilidad_sks
}

async def scrapear_concurrente_nuevos_venues(nuevos_venues, fechas, max_concurrent=4):
    from asyncio import Semaphore, create_task, gather

    crear_tabla_postgres()
    borrar_registros_viejos()

    sem = Semaphore(max_concurrent)

    async def scrapear(venue_name, extractor_func, fecha):
        async with sem:
            print(f"[INICIO] {venue_name} - {fecha}")
            try:
                df = await extractor_func(venue=venue_name, fecha=fecha)
                guardar_df_postgres(df)
            except Exception as e:
                print(f"❌ Error en scraping {venue_name}-{fecha}: {e}")
            print(f"[FIN]    {venue_name} - {fecha}")

    tareas = [
        create_task(scrapear(venue_name, extractor_func, fecha))
        for venue_name, extractor_func in nuevos_venues.items()
        for fecha in fechas
    ]

    await gather(*tareas)

# ------------------------- MAIN -------------------------

if __name__ == "__main__":
    hoy = datetime.date.today()
    fechas = [(hoy + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(21)]
    start = time.time()
    asyncio.run(scrapear_concurrente_nuevos_venues(NUEVOS_VENUES, fechas, max_concurrent=2))
    end = time.time()
    print(f"\nTiempo total: {end - start:.2f} segundos")
