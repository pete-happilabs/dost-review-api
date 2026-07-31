# Q3: Multi-stage build, non-root user, healthcheck
FROM python:3.12-slim AS base

RUN groupadd -r dost && useradd -r -g dost dost

WORKDIR /app

# Install deps first (layer cache)
COPY pyproject.toml .
RUN pip install --no-cache-dir . && rm -rf /root/.cache

# Copy engine
COPY vendor/engine.py /engine/engine.py

# Copy app code
COPY app/ app/
COPY migrations/ migrations/

ENV ENGINE_PATH=/engine
EXPOSE 8013

# Q3: Run as non-root
USER dost

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8013/health')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8013"]
