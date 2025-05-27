import os
from fastapi import FastAPI
from sqlalchemy import create_engine, MetaData, Table, select

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

horarios = metadata.tables["horarios"]
golf_horarios = metadata.tables["golf_horarios"]

app = FastAPI()

@app.get("/disponibilidad")
def disponibilidad(venue: str, fecha: str, hora: str = None):
    with engine.connect() as conn:
        query = select(horarios).where(
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
        query = select(golf_horarios).where(
            (golf_horarios.c.venue == venue) & (golf_horarios.c.fecha == fecha)
        )
        if hora:
            query = query.where(golf_horarios.c.hora == hora)
        if hoyos:
            query = query.where(golf_horarios.c.hoyos == hoyos)
        result = conn.execute(query)
        rows = [dict(row._mapping) for row in result]
    return rows
