# Unified DB MCP Server
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 -s /bin/bash appuser

ENV HOME=/home/appuser \
    PATH=/home/appuser/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PORT=7861 \
    HOST=0.0.0.0

WORKDIR /app

COPY requirements.txt pyproject.toml ./
COPY unified_db_mcp/ unified_db_mcp/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e .

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 7861

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request, os; port=int(os.environ.get('PORT','7861')); urllib.request.urlopen(f'http://127.0.0.1:{port}/check-headers')" || exit 1

CMD ["python", "-u", "-m", "unified_db_mcp.server"]
