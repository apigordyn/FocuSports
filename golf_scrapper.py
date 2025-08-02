import json, os, requests
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import random
import time
import warnings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent, FakeUserAgentError
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
print(f"⛳ tee_watcher.py started at {datetime.now().isoformat()}")

# --- Configuración global ---
WEBSHARE_API_KEY = "ep51no531qi922dm4acixkdpz9glvzk6jnln2fw4"
WEBSHARE_PROXY_LIST_URL = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page_size=250"

FALLBACK_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3)...",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0)...",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X)...",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)... Edg/115.0..."
]

REFERERS = [
    "https://www.google.com", "https://www.bing.com",
    "https://www.yahoo.com", "https://www.duckduckgo.com",
    "https://www.ecosia.org/"
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.5", "es-ES,es;q=0.8", "fr-FR,fr;q=0.7,en-US;q=0.3"
]

# --- Funciones auxiliares ---
def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

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

def borrar_registros_viejos():
    conn = get_conn()
    cur = conn.cursor()
    hoy = date.today().strftime("%Y%m%d")
    cur.execute("DELETE FROM golf_horarios WHERE fecha < %s;", (hoy,))
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

def fetch_proxies_from_webshare() -> List[str]:
    headers = {"Authorization": f"Token {WEBSHARE_API_KEY}"}
    try:
        response = requests.get(WEBSHARE_PROXY_LIST_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"❌ Error al consultar Webshare: {e}")
        return []

    proxies = []
    for proxy in data.get("results", []):
        if not proxy.get("valid"):
            continue
        try:
            ip, port = proxy["proxy_address"], proxy["port"]
            user, pwd = proxy["username"], proxy["password"]
            proxies.append(f"http://{user}:{pwd}@{ip}:{port}")
        except KeyError:
            continue
    return proxies

def get_random_proxy(proxy_list: List[str]) -> dict:
    proxy = random.choice(proxy_list)
    return {"http": proxy, "https": proxy}

def get_safe_user_agent():
    try:
        return UserAgent().random
    except FakeUserAgentError:
        return random.choice(FALLBACK_USER_AGENTS)

def get_random_headers():
    return {
        "User-Agent": get_safe_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Referer": random.choice(REFERERS),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

def extract_available_slots(url: str, proxy_list: List[str]) -> List[Tuple[str, int]]:
    if not proxy_list:
        print("❌ No se pudo obtener proxies.")
        return []

    for attempt in range(1, 4):
        proxies = get_random_proxy(proxy_list)
        headers = get_random_headers()
        time.sleep(random.uniform(1, 2))
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
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
        except requests.RequestException as e:
            print(f"⚠️ Error intento {attempt}: {e}")

    print(f"❌ Falló al scrapear {url} tras múltiples intentos.")
    return []

def next_n_days(n: int = 28) -> List[date]:
    today = date.today()
    return [today + timedelta(days=i) for i in range(n)]

def scrape_venue(club, data, fechas, proxy_list):
    domain = data["domain"]
    booking_id = data["bookingResourceId"]
    fee_groups = data["feeGroupIds"]
    venue_results = []

    for dia in fechas:
        date_iso = dia.isoformat()
        fecha_fmt = dia.strftime("%Y%m%d")
        for hoyos_str, fee_id in fee_groups.items():
            url = (
                f"https://{domain}/guests/bookings/ViewPublicTimesheet.msp"
                f"?bookingResourceId={booking_id}&selectedDate={date_iso}&feeGroupId={fee_id}"
            )
            for time_str, free in extract_available_slots(url, proxy_list):
                venue_results.append({
                    "venue": club,
                    "fecha": fecha_fmt,
                    "hora": time_str,
                    "hoyos": int(hoyos_str),
                    "lugares": free,
                    "link": url
                })
    return venue_results

def main():
    course_path = Path(__file__).parent / "venues" / "golf_venues.json"
    try:
        COURSES = json.loads(course_path.read_text())
    except Exception as e:
        raise RuntimeError(f"❌ No se pudo cargar {course_path}: {e}")

    crear_tabla_golf_postgres()
    borrar_registros_viejos()

    fechas = next_n_days(28)
    proxy_list = fetch_proxies_from_webshare()
    if not proxy_list:
        print("🚫 Sin proxies disponibles. Saliendo...")
        return

    results = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(scrape_venue, club, data, fechas, proxy_list)
            for club, data in COURSES.items()
        ]
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"❌ Error al procesar un club: {e}")

    df = pd.DataFrame(results)
    print(df)
    guardar_golf_df_postgres(df)
    return df

if __name__ == "__main__":
    main()
