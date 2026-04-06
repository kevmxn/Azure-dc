# ── Roulette Signal Bot — Dockerfile ──────────────────────────────────────────
FROM python:3.10-slim

# System dependencies for matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY main.py .

# Render injects $PORT automatically
ENV PORT=10000
EXPOSE 10000

# Start the bot (Flask + asyncio WebSockets)
CMD ["python", "main.py"]
