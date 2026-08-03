# Q3: Multi-stage build, non-root user, healthcheck
FROM python:3.12-slim AS base

RUN groupadd -r dost && useradd -r -g dost dost

WORKDIR /app

# Install deps first (layer cache). Note: at this point the source isn't copied
# yet, so this step only resolves dependencies — the dost-review-api dist it
# builds contains no packages.
COPY pyproject.toml .
RUN pip install --no-cache-dir . && rm -rf /root/.cache

# Copy engine
COPY vendor/engine.py /engine/engine.py

# Copy app code
COPY app/ app/
COPY migrations/ migrations/

# Q3: Re-install now that app/ exists so the installed package actually contains
# the code (previously the app was only importable via WORKDIR being on sys.path,
# and the installed dist was an empty decoy). --no-deps keeps this layer fast.
RUN pip install --no-cache-dir --no-deps --force-reinstall .

ENV ENGINE_PATH=/engine
EXPOSE 8013

# Q3: Run as non-root
USER dost

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8013/health')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8013"]
