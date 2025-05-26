import os
from fastapi import FastAPI
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.engine import Result

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
horarios = metadata.tables["horarios"]

app = FastAPI()

@app.get("/disponibilidad")
def disponibilidad(venue: str, fecha: str):
    with engine.connect() as conn:
        query = select(horarios).where(
            (horarios.c.venue == venue) & (horarios.c.fecha == fecha)
        )
        result: Result = conn.execute(query)
        rows = [dict(row) for row in result]
    return rows
