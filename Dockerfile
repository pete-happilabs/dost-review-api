FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY vendor/engine.py /engine/engine.py
ENV ENGINE_PATH=/engine

COPY app/ app/
COPY migrations/ migrations/

EXPOSE 8013
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8013"]
