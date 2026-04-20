# AI-Powered Copper and Aluminum Market Analyst Synopsis Intelligence Dashboard

A production-oriented market-intelligence pipeline for discovering, extracting, evaluating, summarizing, storing, and notifying on copper and aluminum market content.

This repository automates the daily collection of analyst reviews, commodities commentary, exchange-linked signals, and selected market-research inputs. It supports:

- multi-source discovery from seeded sites, listings, RSS, sitemaps, traversal, and optional search-driven expansion
- adaptive scraping with static, dynamic, and stealth-aware retrieval
- article extraction from HTML and PDF sources
- AI-backed structured synopsis generation using **Groq**, **Ollama**, or a rule-based fallback
- **SharePoint** persistence through **Microsoft Graph**
- HTML email digest generation for internal stakeholders
- JSONL audit logs for traceability and troubleshooting

---

## 1. Project Purpose

The project was built to reduce the manual effort involved in scanning multiple market-information sites every day. Instead of reading, copying, summarizing, and emailing market updates by hand, the pipeline:

1. discovers candidate content from configured sources
2. extracts readable content from HTML pages and PDFs
3. checks target-metal relevance and date alignment
4. generates a structured analyst-style summary
5. writes the result to SharePoint
6. optionally emails a grouped HTML digest

The system is designed for **internal market intelligence** and downstream reporting/dashboard readiness.

---

## 2. High-Level Architecture

```text
CLI / Runtime Entry
        |
        v
analyst_brief_generator_v10.py
        |
        +--> analyst_brief_generator_v9.py  (orchestration helpers, Graph, email, backend order)
        |
        +--> scrap_final.py                 (active Scrapling-first scrape engine)
                     |
                     +--> scrap_logic.py    (discovery, extraction, scoring, filtering, PDF logic)
                     +--> firecrawl_client.py (optional external fallback)
        |
        +--> groq_client.py                (structured Groq summary generation)
        |
        +--> Microsoft Graph               (SharePoint + sendMail)
        |
        +--> run_logs/*.jsonl              (audit trail)
```

---

## 3. Repository Structure

```text
.
├── analyst_brief_generator_v10.py
├── analyst_brief_generator_v9.py
├── scrap_final.py
├── scrap_logic.py
├── scrape_core_refactor.py
├── scrape_core_refactor_v5_scrapy_pdf.py
├── groq_client.py
├── firecrawl_client.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .gitignore
├── .env                      # not committed; local runtime secrets/config
└── run_logs/                 # JSONL audit logs
```

### Core files

#### `analyst_brief_generator_v10.py`
Current **active entry point**. It:
- loads environment variables
- parses CLI flags
- builds source configs from the registry
- calls the active scrape engine in `scrap_final.py`
- applies relevance filtering and summary generation via legacy helpers
- writes rows to SharePoint
- controls alerting and run logs

#### `analyst_brief_generator_v9.py`
Still a **major orchestration dependency** even in the v10 pipeline. It contains:
- env helper functions
- Graph authentication and SharePoint helpers
- email digest rendering and sending
- backend ordering (Groq / Ollama / rule)
- source registry and fallback source selection
- optional Ollama source-probe logic
- row construction and deduplication helpers

#### `scrap_final.py`
Active **Scrapling-first scrape engine**. It adds:
- static, dynamic, and stealth-aware fetching
- traversal-based source exploration
- per-source concurrency
- optional Firecrawl fallback hooks
- candidate shortlist and extraction flow

#### `scrap_logic.py`
Deep **scraping intelligence layer**. It provides:
- normalized data models
- discovery logic from RSS, sitemaps, listings, search engines, and source hints
- HTML/PDF extraction helpers
- article scope classification
- topic/date/content acceptance rules
- scoring, verification, and filtering behavior

#### `groq_client.py`
Structured inference adapter for Groq. It performs:
- schema-constrained JSON generation
- retry logic
- response validation
- usage metadata capture

#### `firecrawl_client.py`
Optional helper for Firecrawl API support. It provides:
- site map/discovery via `map`
- extraction via `scrape`
- search-based expansion via `search`

#### `scrape_core_refactor.py`
Legacy **baseline scrape core**.

#### `scrape_core_refactor_v5_scrapy_pdf.py`
Legacy baseline variant with **Scrapy extraction + stronger PDF/report behavior**.

---

## 4. Active Execution Path

The active runtime path is:

1. `analyst_brief_generator_v10.py` starts the run
2. it builds source configs from the source registry in `analyst_brief_generator_v9.py`
3. it calls `scrap_final.scrape_sources(...)`
4. `scrap_final.py` uses `scrap_logic.py` helpers for discovery, extraction, scoring, and verification
5. accepted articles are optionally passed through LLM relevance filtering
6. summaries are generated using the configured backend order
7. rows are deduplicated and written to SharePoint
8. an optional HTML email digest is sent through Graph
9. run metadata is written to `run_logs/*.jsonl`

---

## 5. Supported Sources

The project is built around configurable source specs and supports a mix of:

- AInvest
- Reuters
- Discovery Alert
- Capital.com
- Argus Media
- Fitch Solutions
- Metal.com / SMM
- LME.com
- optional supporting market-reference / market-research sources from the extended registry

The registry design allows URL overrides and environment-driven extensions without changing core code.

---

## 6. Runtime Features

### Discovery strategies
The pipeline can discover content through:
- seed URLs
- listing pages
- RSS feeds
- XML sitemaps
- second-hop section traversal
- optional search-engine discovery
- optional Firecrawl-assisted discovery

### Fetching strategies
Depending on configuration and page quality, the engine can use:
- direct HTTP retrieval
- dynamic rendering
- stealthy fallback retrieval
- requests/httpx fallback
- PDF binary fetch + extraction

### Content filtering
Not every discovered page is accepted. The system checks:
- target-metal relevance
- market context
- article quality and body length
- target date / recency alignment
- source suitability
- content type (HTML article vs PDF/report)

### Summary backends
The summary layer supports:
- **Groq**
- **Ollama**
- **rule-based fallback**

### Enterprise integration
The project includes:
- Microsoft Graph authentication with client credentials
- SharePoint site/list resolution
- SharePoint list item creation
- email delivery through Graph `sendMail`
- alert grouping by metal and section

---

## 7. Requirements

The project uses Python 3.12 in Docker and depends on packages such as:

- `python-dotenv`
- `msal`
- `requests`, `httpx`
- `beautifulsoup4`, `lxml`
- `pydantic`
- `feedparser`
- `trafilatura`
- `readability-lxml`
- `scrapy`, `parsel`
- `playwright`
- `pypdf`
- `scrapling`
- `charset-normalizer`, `chardet`

Install locally:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

---

## 8. Environment Configuration

Create a local `.env` file in the project root.

### Minimal example

```env
TENANT_ID=<tenant-id>
GRAPH_APP_CLIENT_ID=<client-id>
GRAPH_APP_CLIENT_SECRET=<client-secret>
SHAREPOINT_HOSTNAME=<tenant>.sharepoint.com
SHAREPOINT_SITE_PATH=/sites/<site-name>
SHAREPOINT_ANALYST_LIST_NAME=Analyst Brief

ENABLE_EMAIL_ALERTS=true
EMAIL_ALERT_MODE=created_only
ALERT_EMAIL_SENDER_UPN=<sender@company.com>
ALERT_EMAIL_TO=<recipient1@company.com>,<recipient2@company.com>
ALERT_EMAIL_CC=<recipient3@company.com>
ALERT_EMAIL_SAVE_TO_SENT_ITEMS=true

SUMMARY_BACKEND=auto
USE_LLM_RELEVANCE_FILTER=true
ENABLE_TODAY_FALLBACK=true
TODAY_ONLY=true
TODAY_ONLY_USE_SYSTEM_DATE=true

OLLAMA_HOST=http://<host>:11434
OLLAMA_MODEL=phi3:medium
OLLAMA_FALLBACK_MODEL=llama3.1:8b
OLLAMA_SUMMARY_MODEL=llama3:8b
OLLAMA_SUMMARY_FALLBACK_MODEL=phi3:medium
OLLAMA_RELEVANCE_MODEL=phi3:medium
OLLAMA_RELEVANCE_FALLBACK_MODEL=llama3:8b
ENABLE_OLLAMA_SOURCE_PROBE=true

GROQ_API_KEY=<groq-key>
GROQ_MODEL=meta-llama/llama-4-scout-17b-16e-instruct

SCRAP_FINAL_PRIMARY_PROVIDER=scrapling
SCRAPLING_MODE=auto
SCRAPE_SOURCE_WORKERS=5
SCRAPE_MAX_CANDIDATES=80
SCRAP_FINAL_CRAWL_DEPTH=2
SCRAP_FINAL_MAX_PAGES_PER_SOURCE=30
SCRAP_FINAL_MAX_FOLLOW_PER_PAGE=20
SCRAP_FINAL_SHORTLIST_PER_SOURCE=15
SCRAPLING_HTTP_TIMEOUT=25
SCRAPLING_DYNAMIC_TIMEOUT_MS=18000
SCRAPLING_STEALTH_TIMEOUT_MS=25000
SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY=false
SCRAP_FINAL_ENABLE_FIRECRAWL_FETCH=false

AUDIT_LOG_DIR=run_logs
```

### Important variable groups

#### Microsoft Graph / SharePoint
- `TENANT_ID`
- `GRAPH_APP_CLIENT_ID`
- `GRAPH_APP_CLIENT_SECRET`
- `SHAREPOINT_HOSTNAME`
- `SHAREPOINT_SITE_PATH`
- `SHAREPOINT_ANALYST_LIST_NAME`

#### Email alerts
- `ENABLE_EMAIL_ALERTS`
- `EMAIL_ALERT_MODE`
- `ALERT_EMAIL_SENDER_UPN`
- `ALERT_EMAIL_TO`
- `ALERT_EMAIL_CC`
- `ALERT_EMAIL_SAVE_TO_SENT_ITEMS`

#### Summary backends
- `SUMMARY_BACKEND`
- `GROQ_API_KEY`
- `GROQ_MODEL`
- `OLLAMA_HOST`
- `OLLAMA_MODEL`
- `OLLAMA_SUMMARY_MODEL`
- `OLLAMA_RELEVANCE_MODEL`

#### Scraping controls
- `SCRAPLING_MODE`
- `SCRAPE_SOURCE_WORKERS`
- `SCRAPE_MAX_CANDIDATES`
- `SCRAP_FINAL_CRAWL_DEPTH`
- `SCRAP_FINAL_MAX_PAGES_PER_SOURCE`
- `SCRAP_FINAL_MAX_FOLLOW_PER_PAGE`
- `SCRAP_FINAL_SHORTLIST_PER_SOURCE`
- `SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY`
- `SCRAP_FINAL_ENABLE_FIRECRAWL_FETCH`

### Configuration hygiene note
If your local `.env` contains duplicate keys, clean them before production use. Duplicates make runtime behavior ambiguous.

---

## 9. Local Setup

### Step 1: Clone the repository

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

### Step 2: Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows
```

### Step 3: Install dependencies

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
python -m playwright install chromium
```

### Step 4: Create `.env`
Add your sanitized runtime configuration.

### Step 5: Test a dry run

```bash
python analyst_brief_generator_v10.py --metal all --sources all --dry-run
```

---

## 10. How to Run the Project

### Recommended dry-run

```bash
python analyst_brief_generator_v10.py --metal all --sources all --dry-run
```

### Run for copper only

```bash
python analyst_brief_generator_v10.py --metal copper --sources all
```

### Run selected sources

```bash
python analyst_brief_generator_v10.py \
  --metal "Copper, Aluminum" \
  --sources "Reuters,Argus Media,LME.com" \
  --backend auto
```

### Override target date

```bash
python analyst_brief_generator_v10.py --target-date 2026-04-16 --dry-run
```

### Save dry-run output to JSON

```bash
python analyst_brief_generator_v10.py --dry-run --output-json output.json
```

### Send a test email alert

```bash
python analyst_brief_generator_v10.py --test-email-alert
```

---

## 11. Interactive Mode

The active entry point supports interactive mode.

```bash
python analyst_brief_generator_v10.py --interactive
```

What interactive mode does:
- prompts for market scope
- prompts for source selection
- optionally lets the operator customize article-count/runtime settings
- then continues the normal execution flow

### Interactive mode via Docker Compose
The uploaded `docker-compose.yml` already sets interactive mode as the default container command.

```bash
docker compose run --rm analyst-brief
```

---

## 12. Docker Support

### Existing Docker artifacts
This repository includes:
- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `.gitignore`

### Build the image

```bash
docker build -t analyst-brief:v10 .
```

### Run container help

```bash
docker run --rm --env-file .env analyst-brief:v10 --help
```

### Dry-run in Docker

```bash
docker run --rm \
  --env-file .env \
  -v "$(pwd)/run_logs:/app/run_logs" \
  analyst-brief:v10 \
  --metal all --sources all --dry-run
```

### Run with Compose

```bash
docker compose run --rm analyst-brief
```

### Important Docker note
The uploaded `Dockerfile` expects:

```text
docker/entrypoint.sh
```

If that file is missing in the repository, Docker execution will fail. Create it before relying on container startup.

A minimal example:

```bash
#!/usr/bin/env sh
set -e
python analyst_brief_generator_v10.py "$@"
```

Save it as:

```text
docker/entrypoint.sh
```

Then make it executable:

```bash
chmod +x docker/entrypoint.sh
```

---

## 13. Logging and Output

The pipeline writes audit events to:

```text
run_logs/*.jsonl
```

Typical log stages include:
- run start
- backend selection
- source probe result
- today-fallback expansion
- SharePoint create vs duplicate skip
- email sent / skipped / failed
- scrape runtime status

Use these logs to understand whether a failure occurred in:
- scraping
- relevance filtering
- summary generation
- Graph authentication
- SharePoint persistence
- email dispatch

---

## 14. SharePoint and Email Behavior

### SharePoint persistence
The system:
- gets a Graph app token
- resolves SharePoint site ID
- resolves SharePoint list ID
- checks for duplicates via `UniqueKey`
- creates only new items

### Email alerting
The system can build a grouped HTML digest with sections such as:
- Price Trend & Momentum
- Key Market Drivers
- Inventory & Flow Dynamics
- Analyst Outlook

Alert behavior depends on:
- `ENABLE_EMAIL_ALERTS`
- `EMAIL_ALERT_MODE`

---

## 15. Backend Strategy

### Groq
Best when you want hosted, schema-constrained structured output.

### Ollama
Useful for local or internal inference, source-probe behavior, and hosted-on-prem/private runtime setups.

### Rule fallback
Ensures the run can still complete even if AI generation is unavailable.

---

## 16. Known Strengths

- clear layering between entry point, orchestration, scrape engine, and deep scrape logic
- SharePoint + Graph integration for enterprise persistence
- grouped HTML email reporting
- multi-strategy discovery instead of a single brittle scraping path
- PDF-aware extraction
- adaptive Scrapling-first runtime
- fallback-aware backend design
- JSONL audit logging for troubleshooting

---

## 17. Known Risks / Cleanup Items

- `analyst_brief_generator_v10.py` is active, but `analyst_brief_generator_v9.py` still contains much of the business logic
- Docker depends on `docker/entrypoint.sh`; verify it exists
- `.env` duplication should be cleaned up before handover/production
- some packages in `requirements.txt` appear optional or legacy rather than core runtime requirements
- multiple backend/provider combinations can create operational ambiguity if not standardized

---

## 18. Recommended Reading Order for a New Maintainer

1. `analyst_brief_generator_v10.py`
2. `analyst_brief_generator_v9.py`
3. `scrap_final.py`
4. `scrap_logic.py`
5. `groq_client.py`
6. `firecrawl_client.py`
7. `requirements.txt`
8. `docker-compose.yml`
9. `Dockerfile`

---

## 19. Suggested Handover Checklist

- [ ] create a sanitized `.env`
- [ ] verify Graph credentials and SharePoint list access
- [ ] test `--dry-run`
- [ ] test `--test-email-alert`
- [ ] verify Ollama reachability if using local/private inference
- [ ] verify Groq key and model if using hosted inference
- [ ] verify `docker/entrypoint.sh` exists
- [ ] verify `run_logs/` is mounted or persisted
- [ ] clean duplicate config keys
- [ ] standardize one approved runtime mode for future maintenance

---

## 20. Example Commands

### Help

```bash
python analyst_brief_generator_v10.py --help
```

### All sources, dry-run

```bash
python analyst_brief_generator_v10.py --metal all --sources all --dry-run
```

### Copper and aluminum, Groq backend

```bash
python analyst_brief_generator_v10.py \
  --metal "Copper, Aluminum" \
  --sources all \
  --backend groq
```

### Interactive mode

```bash
python analyst_brief_generator_v10.py --interactive
```

### Docker Compose interactive mode

```bash
docker compose run --rm analyst-brief
```

---

## 21. License / Internal Use Note

This repository is intended for **internal project and handover use**. Before broader distribution or production deployment, review:
- source rights and licensing
- internal security policy
- secret handling
- infrastructure ownership
- SharePoint/email service-account controls

---

## 22. Credits

Prepared as part of the Pakistan Cables Limited internship project:

**AI-Powered Copper and Aluminum Market Analyst Synopsis Intelligence Dashboard**

Primary handover focus:
- runtime clarity
- maintainability
- partner transition readiness
- enterprise integration continuity

