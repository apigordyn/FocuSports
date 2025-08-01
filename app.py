import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json

DATABASE_URL = os.getenv("DATABASE_URL")

DEPORTES = {
    "tennis": {
        "tabla": "horarios",
        "columnas": {
            "venue": "venue",
            "fecha": "fecha",
            "hora": "hora",
            "minutos": "duracion_max_min"
        }
    },
    "golf": {
        "tabla": "golf_horarios",
        "columnas": {
            "venue": "venue",
            "fecha": "fecha",
            "hora": "hora",
            "hoyos": "hoyos",
            "lugares": "lugares"
        }
    },
    "futsal": {
        "tabla": "futsal_horarios",
        "columnas": {
            "venue": "venue",
            "fecha": "fecha",
            "hora": "hora",
            "minutos": "minutos"
        }
    }
}

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def formatear_hora_estandar(hora_str):
    s = hora_str.strip().lower().replace('.', '')
    
    # Asegurar que tenga espacio antes de am/pm
    if s.endswith("am") or s.endswith("pm"):
        s = s[:-2] + " " + s[-2:]

    formatos = ["%I:%M %p", "%I:%M%p", "%I %p", "%H:%M"]

    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%I:%M %p")  # salida tipo "10:00 AM"
        except Exception:
            continue

    print(f"⚠️ Hora inválida encontrada: {hora_str}")
    return None  # retorna None si falla

def redondear_a_media_hora(hora_str):
    hora_fmt = formatear_hora_estandar(hora_str)
    if hora_fmt is None:
        print(f"⚠️ No se pudo formatear la hora: {hora_str}")
        return None
    try:
        dt = datetime.strptime(hora_fmt, "%I:%M %p")
    except:
        print(f"⚠️ Falla en parsear redondeo: {hora_fmt}")
        return None
    minute = dt.minute
    if minute < 15:
        dt = dt.replace(minute=0)
    elif minute < 45:
        dt = dt.replace(minute=30)
    else:
        dt = dt.replace(minute=0) + timedelta(hours=1)
    return dt.strftime("%I:%M %p")

def actualizar_resumen(deporte):
    print(f"🏁 Actualizando resumen para: {deporte}")
    config = DEPORTES[deporte]
    tabla = config["tabla"]
    columnas = config["columnas"]

    conn = get_conn()
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {tabla}")
            rows = cur.fetchall()
            print(f"📥 Seleccionando datos desde {tabla}...")
            print(f"✅ {len(rows)} filas recuperadas para {deporte}")

            data_por_fecha = {}

            for row in rows:
                fecha = row[columnas["fecha"]]
                hora = row[columnas["hora"]]
                venue = row[columnas["venue"]]

                if not isinstance(fecha, str):
                    fecha = str(fecha)
                if len(fecha) != 8:
                    print(f"❌ Fecha inválida encontrada: {fecha}")
                    continue

                hora_fmt = formatear_hora_estandar(hora)
                hora_red = redondear_a_media_hora(hora_fmt)

                if fecha not in data_por_fecha:
                    data_por_fecha[fecha] = {}

                if hora_red not in data_por_fecha[fecha]:
                    data_por_fecha[fecha][hora_red] = {}

                if deporte == "golf":
                    hoyos = row.get(columnas.get("hoyos"))
                    lugares = row.get(columnas.get("lugares"))
                    if hoyos is None or lugares is None:
                        continue
                    hoyos = str(hoyos)
                    if venue not in data_por_fecha[fecha][hora_red]:
                        data_por_fecha[fecha][hora_red][venue] = {}
                    if hoyos not in data_por_fecha[fecha][hora_red][venue]:
                        data_por_fecha[fecha][hora_red][venue][hoyos] = lugares
                    else:
                        data_por_fecha[fecha][hora_red][venue][hoyos] = max(
                            data_por_fecha[fecha][hora_red][venue][hoyos],
                            lugares
                        )
                else:
                    minutos = row.get(columnas.get("minutos"), 0)
                    if venue not in data_por_fecha[fecha][hora_red]:
                        data_por_fecha[fecha][hora_red][venue] = minutos
                    else:
                        data_por_fecha[fecha][hora_red][venue] = max(
                            data_por_fecha[fecha][hora_red][venue],
                            minutos
                        )

            fechas_insertadas = list(data_por_fecha.keys())
            print(f"🧩 Insertando resumen por fecha para {len(fechas_insertadas)} fechas")

            for fecha, horarios in data_por_fecha.items():
                resumen_json = json.dumps(horarios)
                cur.execute("""
                    INSERT INTO disponibilidad_resumen (deporte, fecha, resumen)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (deporte, fecha)
                    DO UPDATE SET resumen = EXCLUDED.resumen
                """, (deporte, fecha, resumen_json))
                print(f"  ✅ {deporte} - {fecha}: {len(horarios)} bloques de hora guardados")

    conn.close()

if __name__ == "__main__":
    for deporte in DEPORTES.keys():
        actualizar_resumen(deporte)
    print("✅ Resúmenes actualizados para todos los deportes.")
