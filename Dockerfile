# ─────────────────────────────────────────────
# Agentic Time Entry Validation — Dockerfile
# Base: Python 3.12 slim (matches your dev env)
# ─────────────────────────────────────────────

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# libpq-dev → required by psycopg2
# gcc       → required to compile some Python packages
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Docker layer caching)
# If requirements.txt hasn't changed → reuse cached layer
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create non-root user for security
# Never run containers as root in production
RUN adduser --disabled-password --gecos "" appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check — ECS uses this to know if container is healthy
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" \
    || exit 1

# Start FastAPI with uvicorn
# --host 0.0.0.0  → accept connections from outside container
# --port 8000     → match EXPOSE above
# --workers 1     → single worker (Fargate 0.5 vCPU)
# --log-level info → structured logs to CloudWatch
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info"]
