IRBIS FastAPI Connector

1. Установить зависимости:
в bash 
   pip install -r requirements-irbis.txt

2. Запустить сервер (из корня проекта, можно не открывая .py): 
в bash 
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1

3. Доступные эндпоинты:
   GET /refresh — запрос всех записей из Irbis и обновление файлов 
   GET /data — получение актуального Данные.txt 
