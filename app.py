import os
import json
import datetime
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL)

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

def normalizar_hora(hora_str):
    try:
        hora_str = hora_str.replace('.', '').replace('AM', ' AM').replace('PM', ' PM').strip().upper()
        for fmt in ["%I:%M %p", "%H:%M"]:
            try:
                dt = datetime.datetime.strptime(hora_str, fmt)
                break
            except ValueError:
                continue
        else:
            return None

        minute = dt.minute
        if minute < 15:
            dt = dt.replace(minute=0)
        elif minute < 45:
            dt = dt.replace(minute=30)
        else:
            dt = dt.replace(minute=0)
            dt = dt.replace(hour=(dt.hour + 1) % 24)
        return dt.strftime("%I:%M %p")
    except Exception as e:
        print("Error al normalizar hora:", hora_str, e)
        return None

def recalcular_resumen(deporte):
    crear_tabla_resumen_si_no_existe()
    conn = get_conn()
    tabla = "horarios" if deporte == "tennis" else f"{deporte}_horarios"

    with conn:
        with conn.cursor() as cur:
            print(f"📥 Seleccionando datos desde {tabla}...")
            cur.execute(f"SELECT fecha, hora, venue FROM {tabla};")
            rows = cur.fetchall()
            print(f"✅ {len(rows)} filas recuperadas para {deporte}")

            data_por_fecha = {}
            for fecha, hora, venue in rows:
                hora_norm = normalizar_hora(hora)
                if hora_norm is None:
                    continue
                if fecha not in data_por_fecha:
                    data_por_fecha[fecha] = {}
                data_por_fecha[fecha].setdefault(hora_norm, set()).add(venue)

            print(f"🧩 Insertando resumen por fecha para {len(data_por_fecha)} fechas")

            for fecha, horas_dict in data_por_fecha.items():
                resumen = {hora: sorted(list(venues)) for hora, venues in horas_dict.items()}
                cur.execute("""
                    INSERT INTO disponibilidad_resumen (fecha, deporte, resumen)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fecha, deporte)
                    DO UPDATE SET resumen = EXCLUDED.resumen;
                """, (fecha, deporte, json.dumps(resumen)))
                print(f"  ✅ {deporte} - {fecha}: {len(resumen)} bloques de hora guardados")

if __name__ == "__main__":
    for deporte in ["tennis", "golf", "futsal"]:
        print(f"\n🏁 Actualizando resumen para: {deporte}")
        recalcular_resumen(deporte)
    print("\n✅ Resúmenes actualizados para todos los deportes.")
