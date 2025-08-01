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
import json

nest_asyncio.apply()

VENUES = [
    "haberfield-tc",
    "eastside-tennis-centre",
    "latham-park-tc",
    "snape-park-tc",
    "trinity-tennis-centre",
    "bexley-tennis-courts",
    "rockdale-tc",
    "cammeray-tc",
    "meadowbank-park-tc"
]

# 🔌 Conexión a Postgres
def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

# 🗂 Crear tabla si no existe
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

# 🎾 Scraper de horarios disponibles
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
            return pd.DataFrame()

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

    # Calcular duración máxima consecutiva
    if not df.empty:
        df['hora_dt'] = pd.to_datetime(df['hora'].str.strip().str.upper(), format='%I:%M%p')
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
                df.loc[group.index, 'duracion_max_min'] = durs

        df.drop(columns=['hora_dt', 'hora_min'], inplace=True)

    return df

# 💾 Guardar en Postgres
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

# ⚙️ Scraping concurrente
async def scrapear_concurrente(venues, fechas, max_concurrent=4):
    from asyncio import Semaphore, create_task, gather

    crear_tabla_postgres()
    borrar_registros_viejos()
    sem = Semaphore(max_concurrent)

    async def scrapear_venue_fecha(venue, fecha):
        async with sem:
            t0 = time.time()
            print(f"[INICIO] {venue} - {fecha} - {t0:.2f}")
            df = await extraer_disponibilidad(venue, fecha)
            guardar_df_postgres(df)
            t1 = time.time()
            print(f"[FIN]    {venue} - {fecha} - {t1:.2f} (Duración: {t1-t0:.2f}s)")
            await asyncio.sleep(4)

    tareas = [
        create_task(scrapear_venue_fecha(venue, fecha))
        for venue in venues
        for fecha in fechas
    ]
    await gather(*tareas)

# 🧱 Crear tabla de resumen si no existe
def crear_tabla_resumen_si_no_existe():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS disponibilidad_resumen (
            fecha TEXT,
            deporte TEXT,
            resumen JSONB,
            PRIMARY KEY (fecha, deporte)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ⏱️ Normalizar hora a formato redondeado
def normalizar_hora(hora_str):
    s = hora_str.replace('.', '').replace('AM', ' AM').replace('PM', ' PM').strip().upper()
    for fmt in ["%I:%M %p", "%H:%M"]:
        try:
            dt = datetime.datetime.strptime(s, fmt)
            break
        except:
            continue
    minute = dt.minute
    if minute < 15:
        dt = dt.replace(minute=0)
    elif minute < 45:
        dt = dt.replace(minute=30)
    else:
        dt = dt.replace(minute=0)
        dt = dt.replace(hour=(dt.hour + 1) % 24)
    return dt.strftime("%I:%M %p")

# 🔄 Actualizar resumen SIN sobrescribir (con control de concurrencia)
def actualizar_cache_resumen_tennis():
    crear_tabla_resumen_si_no_existe()
    conn = get_conn()

    # Cargar data parcial de este scraper
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fecha, hora, venue FROM horarios;")
            rows = cur.fetchall()

            # Agrupar por fecha y hora
            parcial_dict = {}
            for fecha, hora, venue in rows:
                hora_norm = normalizar_hora(hora)
                if fecha not in parcial_dict:
                    parcial_dict[fecha] = {}
                parcial_dict[fecha].setdefault(hora_norm, set()).add(venue)

            # Merge concurrente seguro
            for fecha, horas_nuevas in parcial_dict.items():
                cur.execute("""
                    SELECT resumen FROM disponibilidad_resumen
                    WHERE fecha = %s AND deporte = 'tennis'
                    FOR UPDATE
                """, (fecha,))
                row = cur.fetchone()
                if row:
                    resumen_actual = json.loads(row[0])
                else:
                    resumen_actual = {}

                for hora, nuevos_venues in horas_nuevas.items():
                    existentes = set(resumen_actual.get(hora, []))
                    resumen_actual[hora] = sorted(list(existentes.union(nuevos_venues)))

                cur.execute("""
                    INSERT INTO disponibilidad_resumen (fecha, deporte, resumen)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fecha, deporte)
                    DO UPDATE SET resumen = EXCLUDED.resumen
                """, (fecha, 'tennis', json.dumps(resumen_actual)))

# 🚀 Main
if __name__ == "__main__":
    hoy = datetime.date.today()
    fechas = [(hoy + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(28)]
    start = time.time()
    asyncio.run(scrapear_concurrente(VENUES, fechas, max_concurrent=1))
    actualizar_cache_resumen_tennis()
    end = time.time()
    print(f"\nTiempo total: {end - start:.2f} segundos")
