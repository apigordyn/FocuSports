from fastapi import FastAPI
import sqlite3

app = FastAPI()

@app.get("/")
def home():
    return {"ok": True, "message": "API de Disponibilidad"}

@app.get("/disponibilidad")
def disponibilidad(venue: str, fecha: str):
    conn = sqlite3.connect("disponibilidad.db")
    cur = conn.cursor()
    cur.execute("SELECT venue, fecha, cancha, hora, link FROM horarios WHERE venue=? AND fecha=?", (venue, fecha))
    rows = cur.fetchall()
    conn.close()
    result = [
        {"venue": r[0], "fecha": r[1], "cancha": r[2], "hora": r[3], "link": r[4]}
        for r in rows
    ]
    return result