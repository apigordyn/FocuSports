import os
import json
import datetime
import psycopg2
from psycopg2.extras import execute_values

# 🔧 Conexión a Postgres
def get_conn():
    DATABASE_URL = os.getenv("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)

# 📄 Crear tabla resumen si no existe
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

# 🕒 Normalizar hora al bloque más cercano
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
    except Exception:
        return None

# 📊 Recalcular resumen para un deporte
def recalcular_resumen(deporte):
    print(f"🏁 Actualizando resumen para: {deporte}")
    crear_tabla_resumen_si_no_existe()
    conn = get_conn()

    with conn:
        with conn.cursor() as cur:
            if deporte == "tennis":
                print("📥 Seleccionando datos desde horarios...")
                cur.execute("SELECT fecha, hora, venue FROM horarios WHERE venue ILIKE %s;", (f"%",))
            else:
                print(f"📥 Seleccionando datos desde {deporte}_horarios...")
                cur.execute(f"SELECT fecha, hora, venue FROM {deporte}_horarios;")
            rows = cur.fetchall()
            print(f"✅ {len(rows)} filas recuperadas para {deporte}")

            data_por_fecha = {}
            for fecha_raw, hora, venue in rows:
                # 🎯 Normalización robusta de fecha para tennis
                if deporte == "tennis":
                    if isinstance(fecha_raw, datetime.date):
                        fecha = fecha_raw.strftime("%Y%m%d")
                    elif isinstance(fecha_raw, str):
                        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"]:
                            try:
                                fecha = datetime.datetime.strptime(fecha_raw, fmt).strftime("%Y%m%d")
                                break
                            except:
                                continue
                        else:
                            print(f"❌ Fecha inválida encontrada: {fecha_raw}")
                            continue
                    else:
                        print(f"❌ Tipo de fecha no soportado: {fecha_raw}")
                        continue
                else:
                    fecha = fecha_raw  # Golf/futsal ya están bien

                hora_norm = normalizar_hora(hora)
                if hora_norm is None:
                    continue

                if fecha not in data_por_fecha:
                    data_por_fecha[fecha] = {}
                data_por_fecha[fecha].setdefault(hora_norm, set()).add(venue)

            print(f"🧩 Insertando resumen por fecha para {len(data_por_fecha)} fechas")

            for fecha, horas_dict in sorted(data_por_fecha.items()):
                resumen = {hora: sorted(list(venues)) for hora, venues in horas_dict.items()}
                cur.execute("""
                    INSERT INTO disponibilidad_resumen (fecha, deporte, resumen)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (fecha, deporte)
                    DO UPDATE SET resumen = EXCLUDED.resumen;
                """, (fecha, deporte, json.dumps(resumen)))
                print(f"  ✅ {deporte} - {fecha}: {len(resumen)} bloques de hora guardados")

# 🚀 Main
if __name__ == "__main__":
    for deporte in ["tennis", "golf", "futsal"]:
        recalcular_resumen(deporte)
    print("✅ Resúmenes actualizados para todos los deportes.")
