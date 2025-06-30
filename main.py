import os, sys, asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import requests
from sqlalchemy import create_engine, Column, Integer, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# PyZ3950
sys.path.append(os.path.join(os.path.dirname(__file__), "PyZ3950"))
from zoom import Connection, Query

# конфигурация
Z3950_HOST = "94.25.37.35"
Z3950_PORT = 6666 #210
Z3950_DBNAME = "svod"
BACKEND_UPLOAD_URL = "http://backend.example.com/upload"
DUMP_DIR = "irbis_dumps"
os.makedirs(DUMP_DIR, exist_ok=True)

# SQLite (по желанию)
engine = create_engine("sqlite:///irbis_data.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
class Record(Base):
    __tablename__ = "records"
    id = Column(Integer, primary_key=True, index=True)
    raw_data = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
Base.metadata.create_all(bind=engine)

app = FastAPI()
conn: Connection = None
conn_alive: bool = False

async def connection_health_checker():
    """Периодически пингует соединение и при падении — пытаться восстанавливать."""
    global conn, conn_alive
    while True:
        await asyncio.sleep(600)  # каждые 10 минут
        if conn is None:
            # если нет соединения — устанавливаем
            try:
                conn = Connection(Z3950_HOST, Z3950_PORT)
                conn.databaseName = Z3950_DBNAME
                conn_alive = True
            except Exception:
                conn_alive = False
            continue
        # проверяем «аливность» коннекта
        try:
            # простой поиск любых записей, ограничив 1 результат
            _ = conn.search(Query("CCL", "1=1"), size=1)
            conn_alive = True
        except Exception:
            conn_alive = False
            # закрываем старый и пробуем пересоздать
            try:
                conn.close()
            except: pass
            try:
                conn = Connection(Z3950_HOST, Z3950_PORT)
                conn.databaseName = Z3950_DBNAME
                conn_alive = True
            except Exception:
                conn_alive = False

@app.on_event("startup")
async def on_startup():
    global conn, conn_alive
    # Стартуем соединение (занимает 2–3 минуты!)
    conn = Connection(Z3950_HOST, Z3950_PORT)
    conn.databaseName = Z3950_DBNAME
    conn_alive = True
    # Запускаем фоновый health-чек
    asyncio.create_task(connection_health_checker())

@app.on_event("shutdown")
def on_shutdown():
    global conn
    if conn:
        conn.close()

@app.get("/refresh")
def refresh_all():
    """
    При каждом запросе от фронтенда достаём список всех объектов и возвращаем их.
    Сохраняем в SQLite и файлы, а статический файл отсылаем на бэкенд.
    """
    global conn, conn_alive
    if not conn_alive or conn is None:
        raise HTTPException(503, "Нет активного соединения к Irbis — попробуйте позже.")
    # Выполняем «полный» запрос (1=1)
    try:
        results = conn.search(Query("CCL", "1=1"))
        raw = "\n\n".join(str(rec.data) for rec in results)
    except Exception as e:
        conn_alive = False
        raise HTTPException(500, f"Ошибка при запросе к Irbis: {e}")

    # Сохраняем в SQLite
    db = SessionLocal()
    db.add(Record(raw_data=raw))
    db.commit()
    db.close()

    # Файлы
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    fname_ts = f"Данные-{ts}.txt"
    fname_static = "Данные.txt"
    path_ts = os.path.join(DUMP_DIR, fname_ts)
    # irbis_dumps
    with open(path_ts, "w", encoding="utf-8") as f:
        f.write(raw)
    # статичный файл
    with open(fname_static, "w", encoding="utf-8") as f:
        f.write(raw)

    # на бэкенд
    try:
        with open(fname_static, "rb") as f:
            resp = requests.post(
                BACKEND_UPLOAD_URL,
                files={"file": ("Данные.txt", f, "text/plain")}
            )
            resp.raise_for_status()
    except Exception as e:
        raise HTTPException(502, f"Не удалось отправить файл на бэкенд: {e}")

    return {"status": "ok", "saved": fname_ts, "count": len(results)}

@app.get("/data")
def get_data():
    """Возвращаем всегда актуальный Данные.txt"""
    if not os.path.exists("Данные.txt"):
        raise HTTPException(404, "Файл Данные.txt не найден")
    return FileResponse("Данные.txt", media_type="text/plain", filename="Данные.txt")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, workers=1)
