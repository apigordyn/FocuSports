import os
from fastapi import FastAPI
from sqlalchemy import create_engine, MetaData
from sqlalchemy import text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            cancha TEXT,
            hora TEXT,
            link TEXT,
            UNIQUE(venue, fecha, cancha, hora)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS golf_horarios (
            id SERIAL PRIMARY KEY,
            venue TEXT,
            fecha TEXT,
            hora TEXT,
            hoyos INTEGER,
            lugares INTEGER,
            link TEXT,
            UNIQUE(venue, fecha, hora, hoyos)
        );
    """))

metadata = MetaData()
metadata.reflect(bind=engine)
horarios = metadata.tables["horarios"]
golf_horarios = metadata.tables["golf_horarios"]

app = FastAPI()

@app.get("/disponibilidad_tennis")
def disponibilidad_tennis(venue: str, fecha: str, hora: str = None):
    with engine.connect() as conn:
        query = horarios.select().where(
            (horarios.c.venue == venue) & (horarios.c.fecha == fecha)
        )
        if hora:
            query = query.where(horarios.c.hora == hora)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
    return rows

@app.get("/disponibilidad_golf")
def disponibilidad_golf(venue: str, fecha: str, hora: str = None, hoyos: int = None):
    with engine.connect() as conn:
        query = golf_horarios.select().where(
            (golf_horarios.c.venue == venue) & (golf_horarios.c.fecha == fecha)
        )
        if hora:
            query = query.where(golf_horarios.c.hora == hora)
        if hoyos:
            query = query.where(golf_horarios.c.hoyos == hoyos)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
    return rows
