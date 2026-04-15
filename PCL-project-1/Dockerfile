FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    SCRAP_FINAL_PRIMARY_PROVIDER=scrapling \
    SCRAPLING_MODE=auto \
    SCRAPE_SOURCE_WORKERS=5 \
    SCRAPE_MAX_CANDIDATES=80 \
    SCRAP_FINAL_CRAWL_DEPTH=2 \
    SCRAP_FINAL_MAX_PAGES_PER_SOURCE=30 \
    SCRAP_FINAL_MAX_FOLLOW_PER_PAGE=20 \
    SCRAP_FINAL_SHORTLIST_PER_SOURCE=15 \
    SCRAPLING_HTTP_TIMEOUT=25 \
    SCRAPLING_DYNAMIC_TIMEOUT_MS=18000 \
    SCRAPLING_STEALTH_TIMEOUT_MS=25000 \
    SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY=false \
    SCRAP_FINAL_ENABLE_FIRECRAWL_FETCH=false

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    unzip \
    git \
    build-essential \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libatspi2.0-0 \
    libxshmfence1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libpango-1.0-0 \
    libcairo2 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    libxrender1 \
    libxi6 \
    libxtst6 \
    libxss1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install curl-cffi \
    && python -m pip install -r /app/requirements.txt \
    && python -m playwright install chromium

COPY . /app

RUN mkdir -p /app/run_logs /app/docker \
    && chmod +x /app/docker/entrypoint.sh

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["--help"]
