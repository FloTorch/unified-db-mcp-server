# ---------- Builder Stage ----------
    FROM python:3.11-slim-bookworm AS builder

    RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        unixodbc-dev \
        && rm -rf /var/lib/apt/lists/*
    
    WORKDIR /build
    
    COPY requirements.txt ./
    COPY README.md ./
    COPY unified_db_mcp/ unified_db_mcp/
    
    RUN pip install --no-cache-dir --upgrade pip && \
        pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt
    
    
    # ---------- Runtime Stage ----------
    FROM python:3.11-slim-bookworm
    
    RUN apt-get update && apt-get install -y --no-install-recommends \
        unixodbc \
        && rm -rf /var/lib/apt/lists/*
    
    RUN useradd -m -u 1000 -s /bin/bash appuser
    
    ENV HOME=/home/appuser \
        PATH=/home/appuser/.local/bin:$PATH \
        PYTHONUNBUFFERED=1 \
        PORT=7860 \
        HOST=0.0.0.0
    
    WORKDIR /app
    
    COPY --from=builder /wheels /wheels
    RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
    
    COPY unified_db_mcp/ unified_db_mcp/
    
    RUN chown -R appuser:appuser /app
    USER appuser
    
    EXPOSE 7860
    
    HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
        CMD python -c "import urllib.request, os; port=int(os.environ.get('PORT','7860')); req=urllib.request.Request(f'http://127.0.0.1:{port}/unified-db/mcp', headers={'Accept': 'text/event-stream'}); urllib.request.urlopen(req, timeout=5)" || exit 1
    
    CMD ["python", "-u", "-m", "unified_db_mcp.server"]