import requests
import pandas as pd
from datetime import datetime, date, timedelta

base_url = "https://app.squarespacescheduling.com/api/scheduling/v1/availability/times"
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}

generic_link = "https://kikoff.com.au/5-aside-football-pitches/#pitch-harbord"

# IDs por duración (Harbord)
DURATION_IDS = {
    60: "39069226",
    75: "39496231",
    90: "40598384",
    120: "60569034"
}

days_to_scrap = 14

def get_slots(duration, appointment_id, day):
    url = base_url
    params = {
        "owner": "d84901c1",
        "appointmentTypeId": appointment_id,
        "calendarId": "any",
        "startDate": day.isoformat(),
        "maxDays": 1,
        "timezone": "Australia/Sydney"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Error con duración {duration} minutos: {e}")
        return []

    rows = []
    for date_str, slots in data.items():
        for slot in slots:
            dt = datetime.fromisoformat(slot["time"])
            rows.append({
                "venue": "KIKOFF",
                "date": dt.strftime("%d-%m-%Y"),
                "time": dt.strftime("%H:%M"),
                "minutes": duration,
                "link": generic_link
            })
    return rows

# Fechas: hoy + N días
today = date.today()
day_range = [today + timedelta(days=i) for i in range(days_to_scrap)]

# Traer todos los slots
all_rows = []
for duration, app_id in DURATION_IDS.items():
    print(f"\n🔍 Scrapeando duración: {duration} minutos (ID: {app_id})...")
    rows_this_duration = []
    for day in day_range:
        rows_this_duration.extend(get_slots(duration, app_id, day))
    print(f"✅ Total encontrados para {duration} minutos: {len(rows_this_duration)}")
    all_rows.extend(rows_this_duration)

# Crear DataFrame
df = pd.DataFrame(all_rows)

# Mostrar resultado
df





import requests
import pandas as pd
from datetime import datetime, date, timedelta

base_url = "https://app.squarespacescheduling.com/api/scheduling/v1/availability/times"
headers = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}

generic_link = "https://kikoff.com.au/5-aside-football-pitches/#pitch-harbord"

# IDs por duración (Harbord)
DURATION_IDS = {
    60: "39069226",
    75: "39496231",
    90: "40598384",
    120: "60569034"
}

days_to_scrap = 14

def get_slots(duration, appointment_id, day):
    url = base_url
    params = {
        "owner": "d84901c1",
        "appointmentTypeId": appointment_id,
        "calendarId": "any",
        "startDate": day.isoformat(),
        "maxDays": 1,
        "timezone": "Australia/Sydney"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Error con duración {duration} minutos: {e}")
        return []

    rows = []
    for date_str, slots in data.items():
        for slot in slots:
            dt = datetime.fromisoformat(slot["time"])
            rows.append({
                "venue": "KIKOFF",
                "date": dt.strftime("%d-%m-%Y"),
                "time": dt.strftime("%H:%M"),
                "minutes": duration,
                "link": generic_link
            })
    return rows

# Fechas: hoy + N días
today = date.today()
day_range = [today + timedelta(days=i) for i in range(days_to_scrap)]

# Traer todos los slots
all_rows = []
for duration, app_id in DURATION_IDS.items():
    print(f"\n🔍 Scrapeando duración: {duration} minutos (ID: {app_id})...")
    rows_this_duration = []
    for day in day_range:
        rows_this_duration.extend(get_slots(duration, app_id, day))
    print(f"✅ Total encontrados para {duration} minutos: {len(rows_this_duration)}")
    all_rows.extend(rows_this_duration)

# Crear DataFrame
df = pd.DataFrame(all_rows)

# Mostrar resultado
df
