import os
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, MetaData, text
from datetime import datetime, timedelta

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
horarios = metadata.tables.get("horarios")
golf_horarios = metadata.tables.get("golf_horarios")
futsal_horarios = metadata.tables.get("futsal_horarios")

app = FastAPI()

def formatear_hora_estandar(hora_str):
    s = hora_str.replace('.', '').replace('AM', ' AM').replace('PM', ' PM').strip().upper()
    formatos = ["%H:%M", "%I:%M %p", "%I:%M%p"]
    dt = None
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if dt is None:
        return hora_str
    return dt.strftime("%I:%M %p")

def redondear_a_media_hora(hora_str):
    # Siempre usa la función formatear_hora_estandar para entrada
    s = hora_str.replace('.', '').replace('AM', ' AM').replace('PM', ' PM').strip().upper()
    formatos = ["%H:%M", "%I:%M %p", "%I:%M%p"]
    dt = None
    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if dt is None:
        return hora_str
    minute = dt.minute
    if minute < 15:
        rounded_minute = 0
    elif minute < 45:
        rounded_minute = 30
    else:
        dt += timedelta(hours=1)
        rounded_minute = 0
    dt = dt.replace(minute=rounded_minute, second=0)
    return dt.strftime("%I:%M %p")

@app.get("/disponibilidad_tennis")
def disponibilidad_tennis(
    fecha: str,
    venue: str = None,
    hora: str = None,
    hora_redondeada: str = None
):
    if horarios is None:
        raise HTTPException(status_code=500, detail="Tabla 'horarios' no existe en la base")
    with engine.connect() as conn:
        query = horarios.select().where(horarios.c.fecha == fecha)
        if venue:
            query = query.where(horarios.c.venue == venue)
        if hora:
            query = query.where(horarios.c.hora == hora)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
        for row in rows:
            row["hora"] = formatear_hora_estandar(row["hora"])
            row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
        if hora_redondeada:
            hora_redondeada_norm = redondear_a_media_hora(hora_redondeada)
            rows = [row for row in rows if row["hora_redondeada"] == hora_redondeada_norm]
    return rows

@app.get("/disponibilidad_golf")
def disponibilidad_golf(
    fecha: str,
    venue: str = None,
    hora: str = None,
    hoyos: int = None,
    hora_redondeada: str = None
):
    if golf_horarios is None:
        raise HTTPException(status_code=500, detail="Tabla 'golf_horarios' no existe en la base")
    with engine.connect() as conn:
        query = golf_horarios.select().where(golf_horarios.c.fecha == fecha)
        if venue:
            query = query.where(golf_horarios.c.venue == venue)
        if hora:
            query = query.where(golf_horarios.c.hora == hora)
        if hoyos:
            query = query.where(golf_horarios.c.hoyos == hoyos)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
        for row in rows:
            row["hora"] = formatear_hora_estandar(row["hora"])
            row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
        if hora_redondeada:
            hora_redondeada_norm = redondear_a_media_hora(hora_redondeada)
            rows = [row for row in rows if row["hora_redondeada"] == hora_redondeada_norm]
    return rows

@app.get("/disponibilidad_futsal")
def disponibilidad_futsal(
    fecha: str,
    venue: str = None,
    hora: str = None,
    hora_redondeada: str = None,
    court: str = None,
):
    if futsal_horarios is None:
        raise HTTPException(status_code=500, detail="Tabla 'futsal_horarios' no existe en la base")
    with engine.connect() as conn:
        query = futsal_horarios.select().where(futsal_horarios.c.fecha == fecha)
        if venue:
            query = query.where(futsal_horarios.c.venue == venue)
        if court:
            query = query.where(futsal_horarios.c.court == court)
        if hora:
            query = query.where(futsal_horarios.c.hora == hora)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
        for row in rows:
            row["hora"] = formatear_hora_estandar(row["hora"])
            row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
        if hora_redondeada:
            hora_redondeada_norm = redondear_a_media_hora(hora_redondeada)
            rows = [row for row in rows if row["hora_redondeada"] == hora_redondeada_norm]
    return rows

@app.get("/disponibilidad_general")
def disponibilidad_general(
    fecha: str,
    hora: str = None,
    hora_redondeada: str = None,
    deporte: str = Query(None, regex="^(tennis|golf|futsal)?$"),
    venue: str = None,
    court: str = None,
):
    hora_redondeada_val = None
    if hora_redondeada:
        hora_redondeada_val = redondear_a_media_hora(hora_redondeada)
    elif hora:
        hora_redondeada_val = redondear_a_media_hora(hora)
    venues_set = set()

    with engine.connect() as conn:
        if (deporte is None) or (deporte == "tennis"):
            tennis_query = horarios.select().where(horarios.c.fecha == fecha)
            if venue:
                tennis_query = tennis_query.where(horarios.c.venue == venue)
            if hora:
                tennis_query = tennis_query.where(horarios.c.hora == hora)
            tennis_rows = [dict(row._mapping) for row in conn.execute(tennis_query)]
            for row in tennis_rows:
                row["hora"] = formatear_hora_estandar(row["hora"])
                row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
                if hora_redondeada_val:
                    if row["hora_redondeada"] == hora_redondeada_val:
                        venues_set.add(row["venue"])
                else:
                    venues_set.add(row["venue"])

        if (deporte is None) or (deporte == "golf"):
            golf_query = golf_horarios.select().where(golf_horarios.c.fecha == fecha)
            if venue:
                golf_query = golf_query.where(golf_horarios.c.venue == venue)
            if hora:
                golf_query = golf_query.where(golf_horarios.c.hora == hora)
            golf_rows = [dict(row._mapping) for row in conn.execute(golf_query)]
            for row in golf_rows:
                row["hora"] = formatear_hora_estandar(row["hora"])
                row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
                if hora_redondeada_val:
                    if row["hora_redondeada"] == hora_redondeada_val:
                        venues_set.add(row["venue"])
                else:
                    venues_set.add(row["venue"])

        if (deporte is None) or (deporte == "futsal"):
            futsal_query = futsal_horarios.select().where(futsal_horarios.c.fecha == fecha)
            if venue:
                futsal_query = futsal_query.where(futsal_horarios.c.venue == venue)
            if court:
                futsal_query = futsal_query.where(futsal_horarios.c.court == court)
            if hora:
                futsal_query = futsal_query.where(futsal_horarios.c.hora == hora)
            futsal_rows = [dict(row._mapping) for row in conn.execute(futsal_query)]
            for row in futsal_rows:
                row["hora"] = formatear_hora_estandar(row["hora"])
                row["hora_redondeada"] = redondear_a_media_hora(row["hora"])
                if hora_redondeada_val:
                    if row["hora_redondeada"] == hora_redondeada_val:
                        venues_set.add(row["venue"])
                else:
                    venues_set.add(row["venue"])

    status = "Available" if venues_set else "NonAvailable"
    return {
        "status": status,
        "venues_count": len(venues_set),
        "venues": sorted(list(venues_set))
    }
