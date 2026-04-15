from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import logging
import os
import re
import sys
import uuid
import warnings 
#import FieldInfo as FieldInfoV1
from urllib.parse import quote, urlparse
from html import escape
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, TypeVar, get_origin
import msal
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator
import requests
from requests import Session
try:
    from pydantic_core import from_json as pydantic_from_json
except Exception:  # pragma: no cover
    pydantic_from_json = None
try:
    from groq_client import GroqClientError, GroqHTTPError, groq_generate_structured
    GROQ_CLIENT_IMPORT_OK = True
except Exception:  # pragma: no cover
    GROQ_CLIENT_IMPORT_OK = False
    GroqClientError = RuntimeError

    class GroqHTTPError(RuntimeError):
        status_code = 0
        body_snippet = ""

    def groq_generate_structured(*args: Any, **kwargs: Any) -> tuple[str, dict]:
        raise GroqClientError("groq_client import failed")
# v9 prefers the centralized dynamic scraping core in scrap_logic.py.
# SharePoint, email alerting, Ollama summarization, and source-probe logic stay the same.
# Sample env additions for the Firecrawl + Groq deliverable:
# FIRECRAWL_API_KEY=
# FIRECRAWL_MAP_LIMIT=80
# FIRECRAWL_SCRAPE_TIMEOUT=45
# FIRECRAWL_USE_SEARCH_FALLBACK=false
# SCRAPE_PROVIDER=firecrawl
# GROQ_API_KEY=
# GROQ_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
# GROQ_TIMEOUT=60
# GROQ_TEMPERATURE=0.2
# GROQ_MAX_COMPLETION_TOKENS=900
# SUMMARY_BACKEND=groq
warnings.filterwarnings(
    "ignore",
    message=r"Core Pydantic V1 functionality isn't compatible with Python 3\.14 or greater\.",
    category=UserWarning,
)

PY314_PLUS = sys.version_info >= (3, 14)
FORCE_BASELINE_SCRAPE_CORE = os.getenv("FORCE_BASELINE_SCRAPE_CORE", "").strip().lower() in {"1", "true", "yes", "y", "on"}

def _import_baseline_scrape_core():
    if os.getenv("USE_SCRAPY_PDF_CORE", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        try:
            from scrape_core_refactor_v5_scrapy_pdf import (
                Article as ScrapedArticle,
                SourceConfig as ScrapeSourceConfig,
                scrape_sources,
            )
            return ScrapedArticle, ScrapeSourceConfig, scrape_sources, "scrapy_pdf"
        except Exception:
            pass
    from scrape_core_refactor import (
        Article as ScrapedArticle,
        SourceConfig as ScrapeSourceConfig,
        scrape_sources,
    )
    return ScrapedArticle, ScrapeSourceConfig, scrape_sources, "baseline"

if FORCE_BASELINE_SCRAPE_CORE:
    ScrapedArticle, ScrapeSourceConfig, scrape_sources, ACTIVE_SCRAPE_CORE = _import_baseline_scrape_core()
else:
    try:
        from scrap_logic import (
            Article as ScrapedArticle,
            SourceConfig as ScrapeSourceConfig,
            scrape_sources,
        )
        ACTIVE_SCRAPE_CORE = "dynamic_profiles_v10"
    except Exception:
        ScrapedArticle, ScrapeSourceConfig, scrape_sources, ACTIVE_SCRAPE_CORE = _import_baseline_scrape_core()

LOG = logging.getLogger("analystbrief")
T = TypeVar("T", bound=BaseModel)
# ============================================================
# ENV / HELPERS
# ============================================================
def load_env() -> None:
    explicit = os.getenv("ENV_PATH")
    if explicit:
        load_dotenv(dotenv_path=explicit, override=True)
        return
    env_file = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=str(env_file), override=True)
def must_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return value.strip()
def opt_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else value.strip()
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except Exception:
        return default
def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except Exception:
        return default
def first_nonempty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        candidate = normalize_space(str(value))
        if candidate:
            return candidate
    return ""
def utc_now() -> datetime:
    return datetime.now(UTC)
def utc_iso_now() -> str:
    return utc_now().isoformat()
def today_iso() -> str:
    return date.today().isoformat()
def system_today_iso() -> str:
    return date.today().isoformat()
def normalize_space(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()
def truncate(text: str | None, limit: int) -> str:
    value = text or ""
    return value if len(value) <= limit else value[:limit].rstrip() + "..."
def stable_key(*parts: str) -> str:
    raw = "||".join((p or "").strip().lower() for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
def append_run_log(run_id: str, record: dict) -> None:
    log_dir = Path(opt_env("AUDIT_LOG_DIR", "run_logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{run_id}.jsonl"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
def parse_dt_to_iso(value: str) -> str:
    if not value:
        return ""
    try:
        from dateutil import parser as dateparser
        dt = dateparser.parse(value)
        if dt is None:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    except Exception:
        return ""
def parse_dt_to_date(value: str) -> str:
    iso = parse_dt_to_iso(value)
    return iso[:10] if iso else today_iso()
def optional_imports_status() -> Dict[str, bool]:
    status: Dict[str, bool] = {}
    for name in ("pydantic_core", "msal", "dotenv", "scrapy", "pypdf", "trafilatura", "readability", "playwright"):
        try:
            status[name] = importlib.util.find_spec(name) is not None
        except Exception:
            status[name] = False
    return status
# ============================================================
# SHAREPOINT / GRAPH
# ============================================================
_http_session: Optional[Session] = None
def get_http_session() -> Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        _http_session.headers.update({
            "User-Agent": opt_env(
                "HTTP_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36",
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": opt_env("HTTP_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
        })
    return _http_session
def http_json(
    method: str,
    url: str,
    token: Optional[str] = None,
    json_body: Optional[dict] = None,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: int = 60,
) -> dict:
    session = get_http_session()
    merged_headers: Dict[str, str] = {}
    if token:
        merged_headers["Authorization"] = f"Bearer {token}"
    if json_body is not None:
        merged_headers["Content-Type"] = "application/json"
    if headers:
        merged_headers.update(headers)
    response = session.request(
        method=method,
        url=url,
        headers=merged_headers,
        json=json_body,
        params=params,
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} for {url}: {response.text[:1000]}")
    if response.text.strip():
        return response.json()
    return {}
def token_client_credentials(authority: str, client_id: str, client_secret: str, scopes: List[str]) -> str:
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise RuntimeError(f"Client credentials token error: {result}")
    return result["access_token"]
def get_graph_app_token(authority: str) -> str:
    return token_client_credentials(
        authority=authority,
        client_id=must_env("GRAPH_APP_CLIENT_ID"),
        client_secret=must_env("GRAPH_APP_CLIENT_SECRET"),
        scopes=["https://graph.microsoft.com/.default"],
    )
def graph_get_site_id(graph_token: str, hostname: str, site_path: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    data = http_json("GET", url, token=graph_token)
    site_id = data.get("id")
    if not site_id:
        raise RuntimeError(f"Site not found: {data}")
    return site_id
def graph_get_list_id(graph_token: str, site_id: str, list_name: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    data = http_json("GET", url, token=graph_token)
    for item in data.get("value", []):
        if item.get("displayName") == list_name:
            return item["id"]
    raise RuntimeError(f"List '{list_name}' not found on the specified SharePoint site.")
def graph_create_list_item(graph_token: str, site_id: str, list_id: str, fields: Dict[str, Any]) -> dict:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    return http_json(
        "POST",
        url,
        token=graph_token,
        json_body={"fields": fields},
        headers={"Prefer": "apiversion=2.1"},
    )
def graph_query_existing_items(
    graph_token: str,
    site_id: str,
    list_id: str,
    filter_expr: Optional[str] = None,
    expand_fields: bool = True,
) -> List[dict]:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params: Dict[str, str] = {}
    if expand_fields:
        params["$expand"] = "fields"
    if filter_expr:
        params["$filter"] = filter_expr
    data = http_json("GET", url, token=graph_token, params=params)
    return data.get("value", [])
def parse_email_list(raw: str) -> List[str]:
    return [item.strip() for item in re.split(r"[;,]", raw or "") if item.strip()]

def htmlize_multiline(value: str) -> str:
    return escape(value or "").replace("\n", "<br>")

def split_structured_points(value: str) -> List[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    parts: List[str] = []
    for line in re.split(r"[\r\n]+", raw):
        cleaned = normalize_space((line or "").lstrip("-*•· ").strip())
        if cleaned:
            parts.append(cleaned)
    if parts:
        return dedupe_keep_order(parts)
    return dedupe_keep_order([
        normalize_space(x)
        for x in re.split(r"\s*;\s*", raw)
        if normalize_space(x)
    ])

def validate_email_address(value: str, field_name: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise RuntimeError(f"{field_name} is empty.")
    if "@" not in cleaned:
        raise RuntimeError(f"{field_name} must be a valid email address or UPN. Got: {cleaned}")
    return cleaned

def resolve_alert_rows(
    created_rows: List[Dict[str, Any]],
    skipped_duplicate_rows: List[Dict[str, Any]],
    generated_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], str]:
    mode = opt_env("EMAIL_ALERT_MODE", "created_only").strip().lower()
    if mode == "all_generated":
        return generated_rows, mode
    if mode == "created_or_duplicates":
        rows = created_rows if created_rows else skipped_duplicate_rows
        return rows, mode
    return created_rows, "created_only"

def build_email_alert_subject(created_rows: List[Dict[str, Any]], requested_scope: str, target_date: Optional[str]) -> str:
    count = len(created_rows)
    scope = requested_scope if requested_scope and requested_scope != "All" else "All Markets"
    when = target_date or today_iso()
    noun = "item" if count == 1 else "items"
    return f"Analyst Brief Alert | {count} new {noun} | {scope} | {when}"

def normalize_metal_for_email(value: str) -> str:
    low = normalize_space(value).lower()
    if "copper" in low and ("aluminum" in low or "aluminium" in low):
        return "Copper & Aluminium"
    if "copper" in low:
        return "Copper"
    if "aluminum" in low or "aluminium" in low:
        return "Aluminium"
    if "broad" in low:
        return "Broad Metals"
    if low in {"all", "all markets", "market", "markets"}:
        return "Market"
    return normalize_space(value) or "Market"

PRICE_TERMS_EMAIL = [
    "price", "prices", "pricing", "rally", "momentum", "support", "resistance", "upside", "downside",
    "premium", "premiums", "spread", "backwardation", "contango", "comex", "shfe", "lme", "stockpiling",
]
DRIVER_TERMS_EMAIL = [
    "tariff", "trade", "policy", "policy risk", "demand", "supply", "electrification", "ai", "china",
    "construction", "manufacturing", "macro", "growth", "stimulus", "smelter", "mine", "production", "consumption",
]
INVENTORY_TERMS_EMAIL = [
    "inventory", "inventories", "stocks", "warehouse", "warehousing", "flow", "flows", "arbitrage", "drawdown",
    "cancelled warrants", "surplus", "deficit", "tightness",
]
OUTLOOK_TERMS_EMAIL = [
    "outlook", "forecast", "expects", "expectation", "guidance", "watch", "monitor", "risk", "stance", "bullish",
    "bearish", "neutral", "mixed", "signal", "signals",
]

def detect_email_section(text: str, theme: str = "", stance: str = "") -> str:
    blob = f"{theme} {stance} {text}".lower()
    if any(term in blob for term in INVENTORY_TERMS_EMAIL):
        return "inventory"
    if any(term in blob for term in PRICE_TERMS_EMAIL):
        return "price"
    if any(term in blob for term in OUTLOOK_TERMS_EMAIL):
        return "outlook"
    if any(term in blob for term in DRIVER_TERMS_EMAIL):
        return "drivers"
    return "drivers"

def source_chip_html(source: str, url: str) -> str:
    label = escape(normalize_space(source) or "Source")
    href = escape(url or "")
    chip_style = (
        "display:inline-block;margin-left:8px;padding:2px 8px;border-radius:999px;"
        "background:#f3f4f6;color:#6b7280;font-size:11px;text-decoration:none;"
    )
    if href:
        return f'<a href="{href}" style="{chip_style}">{label}</a>'
    return f'<span style="{chip_style}">{label}</span>'

def bullet_item_html(text: str, source: str, url: str) -> str:
    return (
        "<li style=\"margin:0 0 10px 0;\">"
        f"<span>{escape(normalize_space(text))}</span>{source_chip_html(source, url)}"
        "</li>"
    )

def build_email_digest_groups(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    groups: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    seen_per_group: Dict[str, set] = {}
    for row in rows:
        group_name = normalize_metal_for_email(row.get("Metal", ""))
        group = groups.setdefault(group_name, {
            "price": [],
            "drivers": [],
            "inventory": [],
            "outlook": [],
        })
        seen = seen_per_group.setdefault(group_name, set())
        source = normalize_space(row.get("SourceName", ""))
        url = row.get("SourceUrl", "") or ""
        theme = normalize_space(row.get("Theme", ""))
        stance = normalize_space(row.get("Stance", ""))
        synopsis = normalize_space(row.get("Synopsis", ""))
        key_points = split_structured_points(row.get("KeyPoints", ""))
        watch_items = split_structured_points(row.get("WatchItems", ""))
        risk_flags = split_structured_points(row.get("RiskFlags", ""))

        candidate_points: List[Tuple[str, str]] = []
        if synopsis:
            candidate_points.append((synopsis, detect_email_section(synopsis, theme=theme, stance=stance)))
        for point in key_points:
            candidate_points.append((point, detect_email_section(point, theme=theme, stance=stance)))
        for point in watch_items:
            candidate_points.append((point, "outlook"))
        for point in risk_flags:
            candidate_points.append((point, "outlook"))
        if stance and theme:
            candidate_points.append((f"Analyst tone remains {stance.lower()} around {theme.lower()}.", "outlook"))
        elif stance and synopsis:
            candidate_points.append((f"Analyst tone remains {stance.lower()} based on the latest readable update.", "outlook"))

        if not candidate_points:
            continue

        for text_value, section_name in candidate_points:
            text_value = normalize_space(text_value)
            if len(text_value) < 18:
                continue
            dedupe_key = (section_name, text_value.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            group[section_name].append({
                "text": text_value,
                "source": source,
                "url": url,
            })

    for group_name, sections in groups.items():
        fallback_pool = sections["drivers"] + sections["price"] + sections["inventory"] + sections["outlook"]
        for key in ("price", "drivers", "inventory", "outlook"):
            if not sections[key] and fallback_pool:
                sections[key] = fallback_pool[:2]
        for key in sections:
            sections[key] = sections[key][:4]
    return groups

def render_email_section(title: str, items: List[Dict[str, str]]) -> str:
    if not items:
        return (
            f'<div style="margin:0 0 22px 0;">'
            f'<div style="font-size:18px;font-weight:700;margin:0 0 10px 0;">{escape(title)}</div>'
            '<ul style="margin:0;padding-left:26px;color:#111827;font-size:15px;">'
            '<li style="margin:0 0 10px 0;">No clear signal extracted from today\'s readable sources.</li>'
            '</ul></div>'
        )
    bullets = "".join(bullet_item_html(item.get("text", ""), item.get("source", ""), item.get("url", "")) for item in items)
    return (
        f'<div style="margin:0 0 22px 0;">'
        f'<div style="font-size:18px;font-weight:700;margin:0 0 10px 0;">{escape(title)}</div>'
        f'<ul style="margin:0;padding-left:26px;color:#111827;font-size:15px;">{bullets}</ul>'
        '</div>'
    )

def render_email_group(group_name: str, sections: Dict[str, List[Dict[str, str]]]) -> str:
    order = [
        ("1. Price Trend & Momentum", sections.get("price", [])),
        ("2. Key Market Drivers", sections.get("drivers", [])),
        ("3. Inventory & Flow Dynamics", sections.get("inventory", [])),
        ("4. Analyst Outlook", sections.get("outlook", [])),
    ]
    content = "".join(render_email_section(title, items) for title, items in order)
    return (
        f'<div style="margin:0 0 34px 0;">'
        f'<h2 style="font-size:30px;line-height:1.2;margin:0 0 18px 0;color:#111827;">{escape(group_name)} – Daily Synopsis</h2>'
        f'{content}'
        '</div>'
    )

def build_email_alert_html(
    created_rows: List[Dict[str, Any]],
    requested_scope: str,
    target_date: Optional[str],
    run_id: str,
) -> str:
    groups = build_email_digest_groups(created_rows)
    ordered_group_names = [name for name in ["Copper", "Aluminium", "Copper & Aluminium", "Broad Metals", "Market"] if name in groups]
    ordered_group_names += [name for name in groups.keys() if name not in ordered_group_names]
    rendered_groups = "".join(render_email_group(name, groups[name]) for name in ordered_group_names)
    readable_scope = normalize_metal_for_email(requested_scope if requested_scope and requested_scope != "All" else "Copper & Aluminium")
    when = escape(target_date or today_iso())
    sources_used = dedupe_keep_order([normalize_space(row.get("SourceName", "")) for row in created_rows if normalize_space(row.get("SourceName", ""))])
    source_line = " · ".join(escape(name) for name in sources_used[:8])
    return f"""
    <html>
      <body style=\"font-family:Segoe UI, Arial, sans-serif;line-height:1.55;color:#111827;background:#ffffff;\">
        <div style=\"max-width:920px;margin:auto;padding:28px 24px 36px 24px;\">
          <p style=\"font-size:17px;color:#111827;margin:0 0 18px 0;\">
            Here is <strong>today's LME-focused synopsis</strong> ({escape(readable_scope)}), compiled from the latest available analyst reviews, market news, and updates.
          </p>
          <div style=\"border-top:1px solid #d1d5db;margin:0 0 28px 0;\"></div>
          {rendered_groups}
          <div style=\"border-top:1px solid #e5e7eb;margin:30px 0 0 0;padding-top:14px;font-size:12px;color:#6b7280;\">
            <div><strong>Target date:</strong> {when}</div>
            <div><strong>Run ID:</strong> {escape(run_id)}</div>
            <div><strong>Sources used:</strong> {source_line or 'Available readable sources'}</div>
          </div>
        </div>
      </body>
    </html>
    """

def graph_send_mail(
    graph_token: str,
    sender_user_principal_name: str,
    to_addresses: List[str],
    subject: str,
    html_body: str,
    cc_addresses: Optional[List[str]] = None,
    save_to_sent_items: bool = True,
) -> dict:
    sender = validate_email_address(sender_user_principal_name, "ALERT_EMAIL_SENDER_UPN")
    clean_to = [validate_email_address(addr, "ALERT_EMAIL_TO") for addr in to_addresses if addr.strip()]
    clean_cc = [validate_email_address(addr, "ALERT_EMAIL_CC") for addr in (cc_addresses or []) if addr.strip()]
    if not clean_to:
        raise RuntimeError("Email alerts are enabled but ALERT_EMAIL_TO is empty.")
    def to_recipients(addresses: List[str]) -> List[Dict[str, Dict[str, str]]]:
        return [{"emailAddress": {"address": addr}} for addr in addresses]
    encoded_sender = quote(sender, safe="")
    url = f"https://graph.microsoft.com/v1.0/users/{encoded_sender}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body,
            },
            "toRecipients": to_recipients(clean_to),
            "ccRecipients": to_recipients(clean_cc),
        },
        "saveToSentItems": save_to_sent_items,
    }
    session = get_http_session()
    response = session.post(
        url,
        headers={
            "Authorization": f"Bearer {graph_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=60,
    )
    if response.status_code not in {200, 201, 202}:
        raise RuntimeError(
            f"Email alert send failed | status={response.status_code} | "
            f"sender={sender} | to={clean_to} | response={response.text[:1200]}"
        )
    LOG.info(
        "Graph mail accepted | status=%s | sender=%s | to=%s | cc=%s",
        response.status_code,
        sender,
        clean_to,
        clean_cc,
    )
    if response.text.strip():
        try:
            return response.json()
        except Exception:
            return {"status_code": response.status_code, "text": response.text[:1000]}
    return {"status_code": response.status_code}
def send_email_alert_digest(
    graph_token: str,
    alert_rows: List[Dict[str, Any]],
    requested_scope: str,
    target_date: Optional[str],
    run_id: str,
) -> dict:
    if not alert_rows:
        LOG.info("No alert rows available. Email alert skipped.")
        return {"status": "skipped", "reason": "no_alert_rows"}
    if not env_bool("ENABLE_EMAIL_ALERTS", False):
        LOG.info("Email alerts disabled by configuration.")
        return {"status": "skipped", "reason": "disabled"}
    sender = must_env("ALERT_EMAIL_SENDER_UPN")
    to_addresses = parse_email_list(must_env("ALERT_EMAIL_TO"))
    cc_addresses = parse_email_list(opt_env("ALERT_EMAIL_CC", ""))
    subject = build_email_alert_subject(alert_rows, requested_scope, target_date)
    html_body = build_email_alert_html(alert_rows, requested_scope, target_date, run_id)
    result = graph_send_mail(
        graph_token=graph_token,
        sender_user_principal_name=sender,
        to_addresses=to_addresses,
        cc_addresses=cc_addresses,
        subject=subject,
        html_body=html_body,
        save_to_sent_items=env_bool("ALERT_EMAIL_SAVE_TO_SENT_ITEMS", True),
    )
    return {
        "status": "sent",
        "rows": len(alert_rows),
        "subject": subject,
        "sender": sender,
        "to": to_addresses,
        "cc": cc_addresses,
        "graph_result": result,
    }
# ============================================================
# SOURCE SELECTION / CONFIG
# ============================================================
SCOPE_ALIAS_MAP = {
    "copper": "Copper",
    "aluminum": "Aluminum",
    "aluminium": "Aluminum",
    "broad-metals": "Broad Metals",
    "broad metals": "Broad Metals",
    "broad": "Broad Metals",
    "copper,aluminum": "Copper, Aluminum",
    "copper aluminum": "Copper, Aluminum",
    "both": "Copper, Aluminum",
    "all": "All",
}
DISCOVERY_FALLBACK_SOURCES_DEFAULT = [
    "Reuters",
    "Discovery Alert",
    "Capital.com",
    "Argus Media",
    "Fitch Solutions",
    "Metal.com",
    "LME.com",
    "AInvest",
]
SOURCE_ROLE_MAP = {
    "AInvest": "secondary narrative market commentary",
    "Reuters": "primary narrative market commentary",
    "Discovery Alert": "narrative market news and alerts",
    "Capital.com": "narrative commodities analysis",
    "Argus Media": "industry market commentary",
    "Fitch Solutions": "market research and sector outlook",
    "Metal.com": "physical market news and commentary",
    "Metal.com Copper News": "narrative copper market news",
    "Metal.com Aluminum News": "narrative aluminum market news",
    "LME.com": "official market data and exchange signal",
    "Brecorder Commodities": "narrative commodities market news",
    "Investing Copper": "market data and quote reference",
    "TradingEconomics Copper": "market data and macro indicator reference",
    "TradingEconomics Aluminum": "market data and macro indicator reference",
    "Fortune Business Insights Metals & Minerals": "market research and industry outlook",
    "Mordor Intelligence Aluminum Market": "market research and sector outlook",
    "Mordor Intelligence Market Analysis": "market research and sector outlook",
    "BusinessInsider Aluminum Price": "market data and price reference",
}
def prompt_user_preferences(source_configs: List[ScrapeSourceConfig]) -> Tuple[str, List[ScrapeSourceConfig]]:
    print("\nWhat market coverage do you want?\n")
    print("1. Copper")
    print("2. Aluminum")
    print("3. Broad Metals")
    print("4. Copper + Aluminum")
    print("5. All\n")
    scope_choice = input("Enter choice number (1-5): ").strip()
    scope_map = {
        "1": "Copper",
        "2": "Aluminum",
        "3": "Broad Metals",
        "4": "Copper, Aluminum",
        "5": "All",
    }
    requested_scope = scope_map.get(scope_choice, "All")
    print("\nWhich sources do you want to search?\n")
    for idx, src in enumerate(source_configs, start=1):
        print(f"{idx}. {src.name}")
    print(f"{len(source_configs) + 1}. All sources\n")
    raw = input("Enter one or more numbers separated by commas: ").strip()
    if not raw:
        return requested_scope, source_configs
    choices = [x.strip() for x in raw.split(",") if x.strip()]
    if str(len(source_configs) + 1) in choices:
        return requested_scope, source_configs
    valid_indices = set(range(1, len(source_configs) + 1))
    selected_indices = sorted({int(c) for c in choices if c.isdigit() and int(c) in valid_indices})
    if not selected_indices:
        return requested_scope, source_configs
    return requested_scope, [source_configs[i - 1] for i in selected_indices]
def normalize_requested_scope(raw_scope: str) -> str:
    if not raw_scope:
        return "All"
    return SCOPE_ALIAS_MAP.get(raw_scope.strip().lower(), "All")
def parse_requested_sources(raw_sources: str, source_configs: List[ScrapeSourceConfig]) -> List[ScrapeSourceConfig]:
    if not raw_sources or raw_sources.strip().lower() == "all":
        return source_configs
    requested_names = [x.strip().lower() for x in raw_sources.split(",") if x.strip()]
    selected = [src for src in source_configs if src.name.lower() in requested_names]
    return selected if selected else source_configs
def scope_matches(requested_scope: str, article_scope: str) -> bool:
    if requested_scope == "All":
        return True
    if requested_scope == "Copper":
        return article_scope in {"Copper", "Copper, Aluminum"}
    if requested_scope == "Aluminum":
        return article_scope in {"Aluminum", "Copper, Aluminum"}
    if requested_scope == "Broad Metals":
        return article_scope == "Broad Metals"
    if requested_scope == "Copper, Aluminum":
        return article_scope in {"Copper", "Aluminum", "Copper, Aluminum"}
    return True
def get_fallback_source_names() -> List[str]:
    raw = opt_env(
        "TODAY_FALLBACK_SOURCE_NAMES",
        ",".join(DISCOVERY_FALLBACK_SOURCES_DEFAULT),
    )
    return [x.strip() for x in raw.split(",") if x.strip()]
def get_fallback_sources(
    all_configs: List[ScrapeSourceConfig],
    selected_configs: List[ScrapeSourceConfig],
) -> List[ScrapeSourceConfig]:
    selected_names = {src.name for src in selected_configs}
    fallback_names = set(get_fallback_source_names())
    return [cfg for cfg in all_configs if cfg.name in fallback_names and cfg.name not in selected_names]
def source_min_body_chars(default_value: int, *, js_heavy: bool = False) -> int:
    """
    v6 is intentionally more accepting.
    Lower defaults keep thin but still useful analyst pages in the pipeline so Ollama can
    classify and summarize them instead of dropping them too early.
    """
    baseline = env_int("SOURCE_MIN_BODY_CHARS_DEFAULT", default_value)
    if js_heavy:
        baseline = env_int("SOURCE_MIN_BODY_CHARS_JS_HEAVY", baseline)
    floor_value = env_int("SOURCE_MIN_BODY_CHARS_FLOOR", 35)
    return max(floor_value, baseline)
def dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items or []:
        value = normalize_space(item)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
def infer_allowed_hosts_from_urls(urls: List[str]) -> List[str]:
    hosts: List[str] = []
    for url in urls or []:
        try:
            from urllib.parse import urlparse
            host = (urlparse(url).netloc or "").strip().lower()
            if not host:
                continue
            hosts.append(host)
            if host.startswith("www."):
                hosts.append(host[4:])
            else:
                hosts.append(f"www.{host}")
        except Exception:
            continue
    return dedupe_keep_order(hosts)
def derive_generic_env_name(source_name: str) -> str:
    clean = re.sub(r"[^A-Z0-9]+", "_", (source_name or "").upper()).strip("_")
    return f"{clean}_URL" if clean else "SOURCE_URL"
def derive_default_terms(source_name: str, urls: List[str]) -> Tuple[List[str], List[str]]:
    label = f"{source_name} {' '.join(urls or [])}".lower()
    category_terms = ["copper", "aluminum", "aluminium", "base metals", "industrial metals", "market", "analysis"]
    search_terms = ["copper", "aluminum", "aluminium", "base metals", "market analysis", "analyst review"]
    if "lme" in label:
        category_terms += ["lme", "stocks", "warehousing", "non-ferrous", "market data"]
        search_terms += ["lme copper", "lme aluminium", "warehousing", "non-ferrous"]
    if "reuters" in label:
        search_terms += ["industrial metals analysis", "andy home"]
    if "argus" in label:
        category_terms += ["latest market news"]
        search_terms += ["latest market news", "base metals"]
    if "capital.com" in label or "capital" in label:
        category_terms += ["commodity", "commodities"]
        search_terms += ["commodities", "commodity outlook"]
    if "fitch" in label:
        category_terms += ["commodities", "metals", "mining", "insights"]
        search_terms += ["metals mining", "commodities"]
    if "metal.com" in label or "smm" in label:
        category_terms += ["news", "smm analysis", "physical market"]
        search_terms += ["copper news", "aluminum news", "SMM analysis"]
    if "ainvest" in label:
        category_terms += ["materials"]
        search_terms += ["materials", "market"]
    if "discoveryalert" in label or "discovery alert" in label:
        category_terms += ["volatility", "market commentary"]
        search_terms += ["market commentary", "base metals"]
    if "brecorder" in label:
        category_terms += ["commodities", "markets", "news"]
        search_terms += ["commodities market", "copper news", "aluminum news"]
    if "investing.com" in label or "investing" in label:
        category_terms += ["commodities", "market data", "price", "futures"]
        search_terms += ["copper commodity price", "copper futures", "commodity market data"]
    if "tradingeconomics" in label:
        category_terms += ["commodity", "market data", "forecast", "historical data"]
        search_terms += ["copper commodity forecast", "aluminum commodity forecast", "commodity data"]
    if "businessinsider" in label:
        category_terms += ["commodities", "price", "market data"]
        search_terms += ["aluminum price", "commodity price"]
    if "mordor" in label:
        category_terms += ["industry reports", "market analysis", "outlook"]
        search_terms += ["aluminum market outlook", "market analysis", "industry report"]
    if "fortunebusinessinsights" in label or "fortune business insights" in label:
        category_terms += ["industry reports", "forecast", "market size"]
        search_terms += ["metals and minerals industry report", "market outlook", "industry forecast"]
    return dedupe_keep_order(category_terms), dedupe_keep_order(search_terms)
SOURCE_REGISTRY_V6: List[Dict[str, Any]] = [
    {
        "name": "AInvest",
        "env_var": "AINVEST_URL",
        "seed_urls": [
            "https://www.ainvest.com/news/articles-materials/",
            "https://www.ainvest.com/news/",
        ],
        "include_hints": ["/news/", "/analysis/", "/articles-", "/materials/"],
        "exclude_hints": ["/news/wire", "/news/author", "/etfs/", "/technicals", "/author", "/stocks/"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": ["https://www.ainvest.com/sitemap.xml"],
        "source_role": "narrative_news",
    },
    {
        "name": "Reuters",
        "env_var": "REUTERS_LISTING_URL",
        "seed_urls": ["https://www.reuters.com/authors/andy-home/"],
        "include_hints": ["/markets/", "/business/", "/world/", "/authors/andy-home/"],
        "exclude_hints": ["/video/", "/graphics/"],
        "paywalled_preview_ok": True,
        "js_heavy": False,
        "min_body_chars": 70,
        "sitemap_urls": [
            "https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml",
            "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml",
        ],
        "source_role": "narrative_news",
    },
    {
        "name": "Discovery Alert",
        "env_var": "DISCOVERY_ALERT_URL",
        "seed_urls": ["https://discoveryalert.com.au/articles/"],
        "include_hints": ["/articles/", "/article/", "/news/", "/market/"],
        "exclude_hints": ["/tools", "/calculators", "/listed-options", "/join", "/login", "/author/", "/category/"],
        "category_terms": ["copper", "aluminum", "aluminium", "market", "alert", "commentary"],
        "search_terms": ["copper market alert", "aluminum market alert", "base metals commentary"],
        "paywalled_preview_ok": False,
        "js_heavy": False,
        "min_body_chars": 65,
        "sitemap_urls": ["https://discoveryalert.com.au/sitemap_index.xml"],
        "source_role": "narrative_news",
    },
    {
        "name": "Capital.com",
        "env_var": "CAPITALCOM_URL",
        "seed_urls": ["https://capital.com/en-int/analysis/capital-com-research-team"],
        "include_hints": ["/analysis/", "/en-int/analysis/", "/research/"],
        "exclude_hints": ["/learn", "/academy", "/contact-us", "/careers", "/webinars"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": ["https://capital.com/sitemap.xml"],
        "source_role": "narrative_news",
    },
    {
        "name": "Argus Media",
        "env_var": "ARGUSMEDIA_URL",
        "seed_urls": [
            "https://www.argusmedia.com/en/commodities/base-metals",
            "https://www.argusmedia.com/en/news-and-insights/latest-market-news/",
        ],
        "include_hints": ["/en/news-and-insights/", "/en/commodities/base-metals"],
        "exclude_hints": ["/events", "/webinars", "/podcasts"],
        "paywalled_preview_ok": True,
        "js_heavy": True,
        "min_body_chars": 50,
        "sitemap_urls": ["https://www.argusmedia.com/sitemap.xml"],
        "source_role": "narrative_news",
    },
    {
        "name": "Fitch Solutions",
        "env_var": "FITCHSOLUTIONS_URL",
        "seed_urls": [
            "https://www.fitchsolutions.com/bmi/metals-mining",
            "https://www.fitchsolutions.com/bmi/commodities",
        ],
        "include_hints": ["/bmi/", "/insights/", "/metals-mining", "/commodities"],
        "exclude_hints": ["/agribusiness", "/demo", "/contact"],
        "paywalled_preview_ok": True,
        "js_heavy": False,
        "min_body_chars": 55,
        "sitemap_urls": ["https://www.fitchsolutions.com/sitemap.xml"],
        "source_role": "market_research",
    },
    {
        "name": "Metal.com Copper News",
        "env_var": "METALCOM_COPPER_NEWS_URL",
        "seed_urls": ["https://news.metal.com/copper"],
        "allowed_hosts": ["news.metal.com", "metal.com", "www.metal.com"],
        "include_hints": ["newscontent", "/copper", "/news/", "/article/", "/analysis/"],
        "exclude_hints": ["/price", "/prices", "/future", "/futures", "/quote", "/quotes", "/symbol", "/ticker", "/download", "/about", "/app"],
        "category_terms": ["copper", "market", "news", "analysis", "commentary", "outlook", "update", "alert"],
        "search_terms": ["copper market news", "copper commentary", "copper outlook", "copper update"],
        "report_terms": ["analysis", "commentary", "outlook", "report", "research", "update"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": [],
        "source_role": "narrative_news",
    },
    {
        "name": "Metal.com Aluminum News",
        "env_var": "METALCOM_ALUMINUM_NEWS_URL",
        "seed_urls": ["https://news.metal.com/aluminum"],
        "allowed_hosts": ["news.metal.com", "metal.com", "www.metal.com"],
        "include_hints": ["newscontent", "/aluminum", "/aluminium", "/news/", "/article/", "/analysis/"],
        "exclude_hints": ["/price", "/prices", "/future", "/futures", "/quote", "/quotes", "/symbol", "/ticker", "/download", "/about", "/app"],
        "category_terms": ["aluminum", "aluminium", "market", "news", "analysis", "commentary", "outlook", "update", "alert"],
        "search_terms": ["aluminum market news", "aluminium commentary", "aluminum outlook", "aluminum update"],
        "report_terms": ["analysis", "commentary", "outlook", "report", "research", "update"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": [],
        "source_role": "narrative_news",
    },
    {
        "name": "LME.com",
        "env_var": "LME_URL",
        "seed_urls": [
            "https://www.lme.com/metals/non-ferrous",
            "https://www.lme.com/metals/non-ferrous/lme-copper",
            "https://www.lme.com/metals/non-ferrous/lme-aluminium",
            "https://www.lme.com/news",
        ],
        "include_hints": ["/metals/non-ferrous", "/news", "/market-data/", "/sustainability-and-physical-markets/"],
        "exclude_hints": ["/login", "/register", "/accessing-market-data/historical-data"],
        "paywalled_preview_ok": False,
        "js_heavy": False,
        "min_body_chars": 40,
        "sitemap_urls": [],
        "source_role": "market_data",
    },
    {
        "name": "Brecorder Commodities",
        "env_var": "BRECORDER_COMMODITIES_URL",
        "seed_urls": ["https://www.brecorder.com/markets/commodities"],
        "allowed_hosts": ["www.brecorder.com", "brecorder.com"],
        "include_hints": ["/markets/commodities", "/news/", "/markets/", "/commodities"],
        "exclude_hints": ["/category/", "/tag/", "/tags/", "/author/", "/authors/", "/videos", "/podcasts"],
        "category_terms": ["copper", "aluminum", "aluminium", "commodities", "markets", "news", "update"],
        "search_terms": ["copper commodities news", "aluminum commodities news", "commodity market update"],
        "report_terms": ["analysis", "commentary", "update", "report"],
        "paywalled_preview_ok": False,
        "js_heavy": False,
        "min_body_chars": 65,
        "sitemap_urls": [],
        "source_role": "narrative_news",
    },
    {
        "name": "Investing Copper",
        "env_var": "INVESTING_COPPER_URL",
        "seed_urls": ["https://www.investing.com/commodities/copper"],
        "allowed_hosts": ["www.investing.com", "investing.com"],
        "include_hints": ["/commodities/copper", "/analysis/", "/news/", "/technical/"],
        "exclude_hints": ["/currencies/", "/stocks/", "/etfs/", "/crypto/", "/indices/"],
        "category_terms": ["copper", "market data", "price", "futures", "inventory"],
        "search_terms": ["copper price", "copper futures", "copper market data"],
        "report_terms": ["analysis", "commentary", "outlook", "technical"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": [],
        "source_role": "market_data",
    },
    {
        "name": "TradingEconomics Copper",
        "env_var": "TRADINGECONOMICS_COPPER_URL",
        "seed_urls": ["https://tradingeconomics.com/commodity/copper"],
        "allowed_hosts": ["tradingeconomics.com", "www.tradingeconomics.com"],
        "include_hints": ["/commodity/copper", "/forecast", "/news"],
        "exclude_hints": ["/stocks", "/currencies", "/bonds", "/crypto", "/calendar"],
        "category_terms": ["copper", "commodity", "market data", "forecast", "inventory"],
        "search_terms": ["copper commodity", "copper forecast", "copper commodity data"],
        "report_terms": ["forecast", "analysis", "commentary", "outlook"],
        "paywalled_preview_ok": False,
        "js_heavy": False,
        "min_body_chars": 50,
        "sitemap_urls": [],
        "source_role": "market_data",
    },
    {
        "name": "TradingEconomics Aluminum",
        "env_var": "TRADINGECONOMICS_ALUMINUM_URL",
        "seed_urls": ["https://tradingeconomics.com/commodity/aluminum"],
        "allowed_hosts": ["tradingeconomics.com", "www.tradingeconomics.com"],
        "include_hints": ["/commodity/aluminum", "/commodity/aluminium", "/forecast", "/news"],
        "exclude_hints": ["/stocks", "/currencies", "/bonds", "/crypto", "/calendar"],
        "category_terms": ["aluminum", "aluminium", "commodity", "market data", "forecast"],
        "search_terms": ["aluminum commodity", "aluminium forecast", "aluminum commodity data"],
        "report_terms": ["forecast", "analysis", "commentary", "outlook"],
        "paywalled_preview_ok": False,
        "js_heavy": False,
        "min_body_chars": 50,
        "sitemap_urls": [],
        "source_role": "market_data",
    },
    {
        "name": "Fortune Business Insights Metals & Minerals",
        "env_var": "FORTUNE_BUSINESS_INSIGHTS_METALS_MINERALS_URL",
        "seed_urls": ["https://www.fortunebusinessinsights.com/industry/metals-and-minerals"],
        "allowed_hosts": ["www.fortunebusinessinsights.com", "fortunebusinessinsights.com"],
        "include_hints": ["/industry/", "/industry-reports", "/report", "/market"],
        "exclude_hints": ["/about-us", "/contact-us", "/press-release", "/careers"],
        "category_terms": ["metals", "minerals", "industry", "market", "outlook", "report"],
        "search_terms": ["metals and minerals industry report", "market outlook", "industry report"],
        "report_terms": ["report", "industry", "forecast", "outlook", "market size", "research"],
        "paywalled_preview_ok": True,
        "js_heavy": False,
        "min_body_chars": 80,
        "sitemap_urls": [],
        "source_role": "market_research",
    },
    {
        "name": "Mordor Intelligence Aluminum Market",
        "env_var": "MORDOR_ALUMINUM_MARKET_URL",
        "seed_urls": ["https://www.mordorintelligence.com/industry-reports/aluminum-market"],
        "allowed_hosts": ["www.mordorintelligence.com", "mordorintelligence.com"],
        "include_hints": ["/industry-reports/", "/aluminum-market", "/report", "/market"],
        "exclude_hints": ["/pricing", "/contact-us", "/careers", "/blog"],
        "category_terms": ["aluminum", "aluminium", "market", "industry", "outlook", "report"],
        "search_terms": ["aluminum market report", "aluminium market outlook", "aluminum industry report"],
        "report_terms": ["report", "industry report", "market analysis", "outlook", "forecast", "research"],
        "paywalled_preview_ok": True,
        "js_heavy": False,
        "min_body_chars": 90,
        "sitemap_urls": [],
        "source_role": "market_research",
    },
    {
        "name": "Mordor Intelligence Market Analysis",
        "env_var": "MORDOR_MARKET_ANALYSIS_URL",
        "seed_urls": ["https://www.mordorintelligence.com/market-analysis"],
        "allowed_hosts": ["www.mordorintelligence.com", "mordorintelligence.com"],
        "include_hints": ["/market-analysis", "/industry-reports/", "/report", "/outlook"],
        "exclude_hints": ["/pricing", "/contact-us", "/careers", "/blog"],
        "category_terms": ["market analysis", "industry report", "outlook", "forecast"],
        "search_terms": ["market analysis metals", "aluminum market analysis", "copper market analysis"],
        "report_terms": ["report", "industry report", "market analysis", "outlook", "forecast", "research"],
        "paywalled_preview_ok": True,
        "js_heavy": False,
        "min_body_chars": 90,
        "sitemap_urls": [],
        "source_role": "market_research",
    },
    {
        "name": "BusinessInsider Aluminum Price",
        "env_var": "BUSINESSINSIDER_ALUMINUM_PRICE_URL",
        "seed_urls": ["https://markets.businessinsider.com/commodities/aluminum-price"],
        "allowed_hosts": ["markets.businessinsider.com", "businessinsider.com", "www.businessinsider.com"],
        "include_hints": ["/commodities/aluminum-price", "/news", "/analysis"],
        "exclude_hints": ["/stocks/", "/currencies/", "/crypto/", "/funds/"],
        "category_terms": ["aluminum", "aluminium", "price", "commodities", "market data"],
        "search_terms": ["aluminum price", "aluminium price", "commodity price"],
        "report_terms": ["analysis", "commentary", "outlook"],
        "paywalled_preview_ok": False,
        "js_heavy": True,
        "min_body_chars": 55,
        "sitemap_urls": [],
        "source_role": "market_data",
    },
]
def build_source_config_from_spec(spec: Dict[str, Any]) -> ScrapeSourceConfig:
    name = spec["name"]
    env_var = spec.get("env_var") or derive_generic_env_name(name)
    raw_seed_urls = list(spec.get("seed_urls") or [])
    if raw_seed_urls:
        env_override = opt_env(env_var, raw_seed_urls[0])
        seed_urls = dedupe_keep_order([env_override] + raw_seed_urls[1:])
    else:
        seed_urls = dedupe_keep_order([opt_env(env_var, "")])
    seed_urls = [u for u in seed_urls if u]
    allowed_hosts = dedupe_keep_order(spec.get("allowed_hosts") or infer_allowed_hosts_from_urls(seed_urls))
    category_terms, search_terms = derive_default_terms(name, seed_urls)
    category_terms = dedupe_keep_order((spec.get("category_terms") or []) + category_terms)
    search_terms = dedupe_keep_order((spec.get("search_terms") or []) + search_terms)
    js_heavy = bool(spec.get("js_heavy", False))
    config_kwargs: Dict[str, Any] = {
        "name": name,
        "seed_urls": seed_urls,
        "allowed_hosts": allowed_hosts,
        "include_hints": dedupe_keep_order(spec.get("include_hints") or []),
        "exclude_hints": dedupe_keep_order(spec.get("exclude_hints") or []),
        "category_terms": category_terms,
        "search_terms": search_terms,
        "paywalled_preview_ok": bool(spec.get("paywalled_preview_ok", True)),
        "min_body_chars": source_min_body_chars(int(spec.get("min_body_chars", 55)), js_heavy=js_heavy),
        "js_heavy": js_heavy,
        "sitemap_urls": dedupe_keep_order(spec.get("sitemap_urls") or []),
        "rss_urls": dedupe_keep_order(spec.get("rss_urls") or []),
        "pdf_report_ok": bool(spec.get("pdf_report_ok", True)),
        "report_terms": dedupe_keep_order(spec.get("report_terms") or ["report", "research", "analysis", "outlook", "forecast", "pdf"]),
    }
    if "source_role" in getattr(ScrapeSourceConfig, "__dataclass_fields__", {}):
        config_kwargs["source_role"] = normalize_space(str(spec.get("source_role") or "narrative_news")).lower()
    return ScrapeSourceConfig(**config_kwargs)
def build_additional_source_specs_from_env() -> List[Dict[str, Any]]:
    """
    Add one or more extra sites without changing any other logic.
    Example:
        ADDITIONAL_SOURCE_URLS=https://example.com/news,https://another-site.com/research
    """
    raw = opt_env("ADDITIONAL_SOURCE_URLS", "")
    if not raw:
        return []

    urls = [
        normalize_space(part)
        for part in re.split(r"[\n,;]+", raw)
        if normalize_space(part)
    ]

    specs: List[Dict[str, Any]] = []
    seen_names: set[str] = set()

    for url in urls:
        host = (urlparse(url).netloc or "generic-source").lower().strip()
        name = host.replace("www.", "") or "generic-source"

        if name in seen_names:
            continue
        seen_names.add(name)

        specs.append({
            "name": name,
            "env_var": derive_generic_env_name(name),
            "seed_urls": [url],
            "include_hints": ["/news", "/analysis", "/research", "/report", "/reports", "/insights", "/market"],
            "exclude_hints": ["/login", "/register", "/contact", "/about", "/careers", "/events", "/webinars", "/podcasts"],
            "paywalled_preview_ok": True,
            "js_heavy": True,
            "min_body_chars": 45,
            "sitemap_urls": [],
            "rss_urls": [],
            "source_role": "narrative_news",
        })

    return specs
def build_source_configs() -> List[ScrapeSourceConfig]:
    specs = list(SOURCE_REGISTRY_V6) + build_additional_source_specs_from_env()
    return [build_source_config_from_spec(spec) for spec in specs]
# ============================================================
# Pydantic models for summary / relevance / repair
# ============================================================
class SummaryOutput(BaseModel):
    theme: str = Field(...)
    stance: Literal["Bullish", "Bearish", "Neutral", "Mixed", "Unknown"] = Field(default="Unknown")
    synopsis: str = Field(...)
    key_points: List[str] = Field(default_factory=list)
    risk_flags: List[str] = Field(default_factory=list)
    watch_items: List[str] = Field(default_factory=list)
    @field_validator("theme", "synopsis", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        return normalize_space(str(value or ""))
    @field_validator("key_points", "risk_flags", "watch_items", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            candidate = normalize_space(value)
            if not candidate:
                return []
            if "\n" in value:
                parts = [normalize_space(x) for x in value.splitlines() if normalize_space(x)]
            else:
                parts = [
                    normalize_space(x)
                    for x in re.split(r"[•;\n]|(?:\s+-\s+)", value)
                    if normalize_space(x)
                ]
            return parts[:6]
        if isinstance(value, list):
            return [normalize_space(str(x)) for x in value if normalize_space(str(x))][:6]
        return []
    @field_validator("stance", mode="before")
    @classmethod
    def _normalize_stance(cls, value: Any) -> str:
        v = normalize_space(str(value or "")).lower()
        mapping = {
            "bullish": "Bullish",
            "bearish": "Bearish",
            "neutral": "Neutral",
            "mixed": "Mixed",
            "unknown": "Unknown",
            "mixed outlook": "Mixed",
            "positive": "Bullish",
            "negative": "Bearish",
        }
        return mapping.get(v, "Unknown")
    @field_validator("theme")
    @classmethod
    def _validate_theme(cls, value: str) -> str:
        if len(value.strip()) < 3:
            raise ValueError("theme is empty or too short")
        return value
    @field_validator("synopsis")
    @classmethod
    def _validate_synopsis(cls, value: str) -> str:
        if len(value.strip()) < 40:
            raise ValueError("synopsis is empty or too short")
        return value
    @field_validator("key_points")
    @classmethod
    def _validate_key_points(cls, value: List[str]) -> List[str]:
        cleaned = [x for x in value if normalize_space(x)]
        if len(cleaned) < 1:
            raise ValueError("at least one key point is required")
        return cleaned
class RelevanceOutput(BaseModel):
    is_relevant: bool = False
    should_summarize: bool = False
    target_metals: List[str] = Field(default_factory=list)
    same_day_valid: bool = False
    page_type: str = "unknown"
    market_context: bool = False
    scope: Literal["Copper", "Aluminum", "Copper, Aluminum", "Broad Metals", "Not Relevant"] = "Not Relevant"
    reasoning_short: str = ""
    reason: str = ""
    @field_validator("target_metals", mode="before")
    @classmethod
    def _normalize_target_metals(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raw_items = re.split(r"[,;\n]+", value)
        elif isinstance(value, list):
            raw_items = [str(item) for item in value]
        else:
            raw_items = [str(value)]
        normalized: List[str] = []
        for item in raw_items:
            token = normalize_space(item).lower()
            if token in {"aluminium", "aluminum"}:
                token = "aluminum"
            if token == "cu":
                token = "copper"
            if token == "al":
                token = "aluminum"
            if token in {"copper", "aluminum"} and token not in normalized:
                normalized.append(token)
        return normalized
    @field_validator("page_type", "reasoning_short", "reason", mode="before")
    @classmethod
    def _normalize_relevance_text(cls, value: Any) -> str:
        return normalize_space(str(value or ""))
    @field_validator("reason", mode="before")
    @classmethod
    def _normalize_reason(cls, value: Any) -> str:
        return normalize_space(str(value or ""))


class OllamaSourceProbeItem(BaseModel):
    source_name: str = ""
    source_url: str = ""
    title: str = ""
    published_at: str = ""
    scope: Literal["Copper", "Aluminum", "Copper, Aluminum", "Broad Metals", "Not Relevant"] = "Not Relevant"
    theme: str = ""
    stance: Literal["Bullish", "Bearish", "Neutral", "Mixed", "Unknown"] = "Unknown"
    synopsis: str = ""
    key_points: List[str] = Field(default_factory=list)
    risk_flags: List[str] = Field(default_factory=list)
    watch_items: List[str] = Field(default_factory=list)
    confidence: Literal["Low", "Medium", "High"] = "Low"
    note: str = ""

    @field_validator("source_name", "source_url", "title", "published_at", "theme", "synopsis", "note", mode="before")
    @classmethod
    def _normalize_probe_text(cls, value: Any) -> str:
        return normalize_space(str(value or ""))

    @field_validator("key_points", "risk_flags", "watch_items", mode="before")
    @classmethod
    def _normalize_probe_lists(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [normalize_space(x.lstrip("-*•· ")) for x in re.split(r"[\r\n;]+", value) if normalize_space(x.lstrip("-*•· "))][:6]
        if isinstance(value, list):
            return [normalize_space(str(x)) for x in value if normalize_space(str(x))][:6]
        return []

    @field_validator("stance", mode="before")
    @classmethod
    def _normalize_probe_stance(cls, value: Any) -> str:
        v = normalize_space(str(value or "")).lower()
        mapping = {
            "bullish": "Bullish",
            "bearish": "Bearish",
            "neutral": "Neutral",
            "mixed": "Mixed",
            "unknown": "Unknown",
            "positive": "Bullish",
            "negative": "Bearish",
        }
        return mapping.get(v, "Unknown")

    @field_validator("confidence", mode="before")
    @classmethod
    def _normalize_probe_confidence(cls, value: Any) -> str:
        v = normalize_space(str(value or "")).lower()
        mapping = {"low": "Low", "medium": "Medium", "high": "High"}
        return mapping.get(v, "Low")


class OllamaSourceProbeOutput(BaseModel):
    status: Literal["have_items", "fallback_to_scraping", "error"] = "fallback_to_scraping"
    note: str = ""
    items: List[OllamaSourceProbeItem] = Field(default_factory=list)

    @field_validator("note", mode="before")
    @classmethod
    def _normalize_probe_note(cls, value: Any) -> str:
        return normalize_space(str(value or ""))
# ============================================================
# OLLAMA
# ============================================================
DIRECT_METAL_TERMS = [
    "copper",
    "aluminum",
    "aluminium",
    "primary aluminum",
    "primary aluminium",
    "red metal",
]
BROAD_METAL_TERMS = [
    "base metals",
    "industrial metals",
    "metal markets",
    "metals market",
    "non-ferrous metals",
    "commodity metals",
]
ANALYSIS_TERMS = [
    "news",
    "analysis",
    "market analysis",
    "markets",
    "analyst",
    "commentary",
    "report",
    "research note",
    "outlook",
    "market outlook",
    "price outlook",
    "forecast",
    "weekly outlook",
    "monthly outlook",
    "trend",
    "trend signals",
    "sentiment",
    "bullish",
    "bearish",
    "neutral",
    "mixed outlook",
    "base metals outlook",
    "industrial metals outlook",
    "commodities",
    "insights",
]
SUPPLY_CHAIN_TERMS = [
    "supply",
    "demand",
    "inventory",
    "inventories",
    "stocks",
    "warehouse",
    "premiums",
    "smelter",
    "smelters",
    "refined",
    "concentrate",
    "mine",
    "mining",
    "output",
    "production",
    "consumption",
    "imports",
    "exports",
    "tariff",
    "trade",
    "surplus",
    "deficit",
    "china",
    "lme",
    "comex",
    "shfe",
    "rod",
    "cathode",
]
ALLOWED_SCOPES = {"Copper", "Aluminum", "Copper, Aluminum", "Broad Metals"}
MODEL_TASK_ENV_NAMES = {
    "summary": "SUMMARY",
    "relevance": "RELEVANCE",
    "repair": "REPAIR",
    "probe": "SOURCE_PROBE",
}
_OLLAMA_MODEL_CACHE: Dict[str, List[str]] = {}

def get_local_ollama_host() -> str:
    return opt_env("LOCAL_OLLAMA_HOST", opt_env("OLLAMA_LOCAL_HOST", "http://127.0.0.1:11434")).rstrip("/")

def get_remote_ollama_host() -> str:
    return opt_env("REMOTE_OLLAMA_HOST", opt_env("OLLAMA_HOST", "http://10.4.3.45:11434")).rstrip("/")

def get_probe_ollama_host() -> str:
    return opt_env("OLLAMA_SOURCE_PROBE_HOST", get_remote_ollama_host()).rstrip("/")

def get_ollama_host() -> str:
    return get_remote_ollama_host()

def unique_nonempty(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        item = (item or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out

def _task_env_key(task: str) -> str:
    return MODEL_TASK_ENV_NAMES.get((task or "").strip().lower(), "DEFAULT")

def _canonical_model_name(value: str) -> str:
    return normalize_space(value).lower()

def _match_model_name(available_models: List[str], requested: str) -> str:
    want = _canonical_model_name(requested)
    if not want:
        return ""
    exact = {model.lower(): model for model in available_models}
    if want in exact:
        return exact[want]
    if ":" not in want:
        for model in available_models:
            low = model.lower()
            if low == want or low.startswith(want + ":"):
                return model
    return ""

def ollama_list_models(host: str) -> List[str]:
    session = get_http_session()
    response = session.get(f"{host}/api/tags", timeout=(5, 20))
    response.raise_for_status()
    data = response.json()
    return [m.get("name", "") for m in data.get("models", []) if m.get("name")]

def get_cached_ollama_models(host: str, refresh: bool = False) -> List[str]:
    host = (host or "").rstrip("/")
    if not host:
        return []
    if refresh or host not in _OLLAMA_MODEL_CACHE:
        _OLLAMA_MODEL_CACHE[host] = ollama_list_models(host)
    return list(_OLLAMA_MODEL_CACHE.get(host, []))

def safe_ollama_list_models(host: str, refresh: bool = False) -> List[str]:
    try:
        return get_cached_ollama_models(host, refresh=refresh)
    except Exception:
        return []

def get_remote_task_models(task: str) -> List[str]:
    task = (task or "").strip().lower()
    if task == "summary":
        return unique_nonempty([
            opt_env("OLLAMA_SUMMARY_MODEL", opt_env("OLLAMA_MODEL", "phi3:mini")),
            opt_env("OLLAMA_SUMMARY_FALLBACK_MODEL", opt_env("OLLAMA_FALLBACK_MODEL", "llama3.1:8b")),
        ])
    if task == "relevance":
        return unique_nonempty([
            opt_env("OLLAMA_RELEVANCE_MODEL", opt_env("OLLAMA_RELEVANCE_MODEL_DEFAULT", "phi3:mini")),
            opt_env("OLLAMA_RELEVANCE_FALLBACK_MODEL", opt_env("OLLAMA_FALLBACK_MODEL", "llama3.1:8b")),
        ])
    if task == "repair":
        return unique_nonempty([
            opt_env("OLLAMA_REPAIR_MODEL", opt_env("OLLAMA_REPAIR_MODEL_DEFAULT", "phi3:mini")),
            opt_env("OLLAMA_REPAIR_FALLBACK_MODEL", opt_env("OLLAMA_FALLBACK_MODEL", "llama3.1:8b")),
        ])
    if task == "probe":
        return unique_nonempty([
            opt_env("OLLAMA_SOURCE_PROBE_MODEL", opt_env("OLLAMA_MODEL", "phi3:mini")),
            opt_env("OLLAMA_SOURCE_PROBE_FALLBACK_MODEL", opt_env("OLLAMA_FALLBACK_MODEL", "llama3.1:8b")),
        ])
    return unique_nonempty([
        opt_env("OLLAMA_MODEL", "phi3:mini"),
        opt_env("OLLAMA_FALLBACK_MODEL", "llama3.1:8b"),
    ])

def get_task_models(task: str) -> List[str]:
    return get_remote_task_models(task)

def resolve_preferred_local_model(task: str, available_models: List[str]) -> str:
    available_models = available_models or []
    if not available_models:
        return ""
    task_key = _task_env_key(task)
    candidates = [
        opt_env(f"LOCAL_SELECTED_{task_key}_MODEL", ""),
        opt_env("LOCAL_SELECTED_MODEL", ""),
        opt_env(f"LOCAL_{task_key}_MODEL", ""),
        opt_env("LOCAL_OLLAMA_MODEL", ""),
        opt_env("DEFAULT_LOCAL_MODEL", ""),
        opt_env("GEMMA_MODEL", ""),
        "gemma3:4b",
        "gemma2:2b",
        "gemma2:9b",
        "gemma:2b",
    ]
    for candidate in candidates:
        matched = _match_model_name(available_models, candidate)
        if matched:
            return matched
    for model in available_models:
        if "gemma" in model.lower():
            return model
    return available_models[0]

def build_task_execution_plan(task: str) -> List[Tuple[str, str, List[str]]]:
    task = (task or "").strip().lower()
    plans: List[Tuple[str, str, List[str]]] = []

    if task != "probe":
        local_host = get_local_ollama_host()
        local_available_models = safe_ollama_list_models(local_host)
        preferred_local_model = resolve_preferred_local_model(task, local_available_models)
        if preferred_local_model:
            plans.append(("local", local_host, [preferred_local_model]))

    remote_host = get_probe_ollama_host() if task == "probe" else get_remote_ollama_host()
    remote_models = get_remote_task_models(task)
    if remote_host and remote_models:
        duplicate_remote = any(
            label in {"local", "server"} and host.rstrip("/") == remote_host.rstrip("/") and models == remote_models
            for label, host, models in plans
        )
        if not duplicate_remote:
            plans.append(("server" if task != "probe" else "probe_fallback", remote_host, remote_models))
    return plans
def get_task_temperature(task: str, retry_attempt: int = 1) -> float:
    task = (task or "").strip().lower()
    if task == "summary":
        base = env_float("OLLAMA_SUMMARY_TEMPERATURE", env_float("OLLAMA_TEMPERATURE", 0.0))
    elif task == "relevance":
        base = env_float("OLLAMA_RELEVANCE_TEMPERATURE", env_float("OLLAMA_TEMPERATURE", 0.0))
    elif task == "repair":
        base = env_float("OLLAMA_REPAIR_TEMPERATURE", env_float("OLLAMA_TEMPERATURE", 0.0))
    elif task == "probe":
        base = env_float("OLLAMA_SOURCE_PROBE_TEMPERATURE", env_float("OLLAMA_TEMPERATURE", 0.0))
    else:
        base = env_float("OLLAMA_TEMPERATURE", 0.0)
    return 0.0 if retry_attempt > 1 else base
def get_task_top_p(task: str) -> float:
    task = (task or "").strip().lower()
    if task == "summary":
        return env_float("OLLAMA_SUMMARY_TOP_P", env_float("OLLAMA_TOP_P", 0.8))
    if task == "relevance":
        return env_float("OLLAMA_RELEVANCE_TOP_P", env_float("OLLAMA_TOP_P", 0.8))
    if task == "repair":
        return env_float("OLLAMA_REPAIR_TOP_P", env_float("OLLAMA_TOP_P", 0.8))
    if task == "probe":
        return env_float("OLLAMA_SOURCE_PROBE_TOP_P", env_float("OLLAMA_TOP_P", 0.8))
    return env_float("OLLAMA_TOP_P", 0.8)
def strip_markdown_fences(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()
def extract_json_candidates(text: str) -> List[str]:
    text = strip_markdown_fences(text)
    if not text:
        return []
    candidates: List[str] = []
    if text.startswith("{") and text.endswith("}"):
        candidates.append(text)
    start_positions = [m.start() for m in re.finditer(r"\{", text)]
    for start in start_positions[:10]:
        depth = 0
        in_str = False
        escape = False
        for idx in range(start, len(text)):
            ch = text[idx]
            if escape:
                escape = False
                continue
            if ch == "\\":  # backslash
                escape = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidates.append(text[start:idx + 1])
                    break
    out: List[str] = []
    seen = set()
    for cand in candidates:
        c = cand.strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out
def model_field_defaults(model_cls: Type[T]) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {}
    for name, field in model_cls.model_fields.items():
        origin = get_origin(field.annotation)
        if origin is list or field.annotation == list or str(origin).endswith("list"):
            defaults[name] = []
        elif field.annotation in {bool}:
            defaults[name] = False
        else:
            defaults[name] = ""
    return defaults
def repair_json_text_common(raw_text: str, model_cls: Type[T]) -> str:
    text = strip_markdown_fences(raw_text)
    text = text.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
    defaults = model_field_defaults(model_cls)
    for name, default_value in defaults.items():
        replacement = "[]" if isinstance(default_value, list) else ("false" if isinstance(default_value, bool) else '""')
        text = re.sub(rf'("{re.escape(name)}"\s*:\s*),', rf'\1 {replacement},', text)
        text = re.sub(rf'("{re.escape(name)}"\s*:\s*)(?=[}}\]])', rf'\1 {replacement}', text)
    text = re.sub(r",\s*([}\]])", r"\1", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text)
    return text.strip()
def coerce_partial_data_to_model(data: Dict[str, Any], model_cls: Type[T]) -> T:
    merged = model_field_defaults(model_cls)
    merged.update(data or {})
    return model_cls.model_validate(merged)
def validate_llm_output(raw_text: str, model_cls: Type[T]) -> T:
    last_error: Optional[Exception] = None
    for candidate in [strip_markdown_fences(raw_text)] + extract_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            return model_cls.model_validate_json(candidate)
        except ValidationError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
    repaired = repair_json_text_common(raw_text, model_cls)
    for candidate in [repaired] + extract_json_candidates(repaired):
        if not candidate:
            continue
        try:
            return model_cls.model_validate_json(candidate)
        except ValidationError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
    if pydantic_from_json is not None:
        try:
            partial = pydantic_from_json(strip_markdown_fences(raw_text).encode("utf-8"), allow_partial=True)
            if isinstance(partial, dict):
                return coerce_partial_data_to_model(partial, model_cls)
        except Exception as exc:
            last_error = exc
        try:
            partial = pydantic_from_json(repaired.encode("utf-8"), allow_partial=True)
            if isinstance(partial, dict):
                return coerce_partial_data_to_model(partial, model_cls)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"LLM output could not be validated for {model_cls.__name__}: "
        f"{str(last_error)[:500]} | raw={truncate(strip_markdown_fences(raw_text), 600)}"
    )
def ollama_generate_raw(
    host: str,
    model: str,
    prompt: str,
    schema: dict,
    timeout: int = 90,
    num_predict: int = 350,
    temperature: Optional[float] = None,
    task: str = "summary",
    retry_attempt: int = 1,
) -> Tuple[str, dict]:
    session = get_http_session()
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": schema,
        "keep_alive": opt_env("OLLAMA_KEEP_ALIVE", "10m"),
        "options": {
            "temperature": temperature if temperature is not None else get_task_temperature(task, retry_attempt=retry_attempt),
            "top_p": get_task_top_p(task),
            "num_predict": num_predict,
            "seed": env_int("OLLAMA_SEED", 7),
        },
    }
    response = session.post(f"{host}/api/generate", json=payload, timeout=(10, timeout))
    if response.status_code >= 400:
        raise RuntimeError(
            f"Ollama HTTP {response.status_code} for model '{model}' at {host}/api/generate. "
            f"Response body: {response.text[:1000]}"
        )
    data = response.json()
    text = data.get("response", "")
    if not text:
        raise RuntimeError(f"Ollama returned empty response for model '{model}': {data}")
    return text, data
def build_json_repair_prompt(raw_output: str, schema: dict, model_name: str) -> str:
    return (
        "You are repairing malformed JSON from another model.\n"
        "Return VALID JSON ONLY.\n"
        "Do not add commentary.\n"
        "Do not wrap in markdown fences.\n"
        "If any field is missing, fill strings with \"\" , booleans with false, and arrays with [].\n"
        f"Target schema:\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n\n"
        f"Broken output from model {model_name}:\n{truncate(raw_output, 12000)}"
    )
def is_summary_substantive(summary: SummaryOutput) -> Tuple[bool, str]:
    if len((summary.theme or "").strip()) < 3:
        return False, "Theme too short or empty"
    if len((summary.synopsis or "").strip()) < 40:
        return False, "Synopsis too short or empty"
    key_points = [x.strip() for x in (summary.key_points or []) if x and x.strip()]
    if not key_points:
        return False, "No key points returned"
    return True, "OK"
def run_ollama_structured(
    prompt: str,
    model_cls: Type[T],
    timeout: int,
    num_predict: int,
    task: str = "summary",
) -> Tuple[T, str, dict]:
    attempts_per_model = env_int("OLLAMA_ATTEMPTS_PER_MODEL", 2)
    enable_repair_prompt = env_bool("ENABLE_OLLAMA_REPAIR_PROMPT", True)
    schema = model_cls.model_json_schema()
    errors: List[str] = []
    execution_plan = build_task_execution_plan(task)
    for host_label, host, models in execution_plan:
        for model in models:
            if not model:
                continue
            for attempt in range(1, attempts_per_model + 1):
                raw_text = ""
                meta: dict = {}
                try:
                    LOG.info(
                        "Ollama request started | task=%s | host_label=%s | host=%s | model=%s | attempt=%s",
                        task,
                        host_label,
                        host,
                        model,
                        attempt,
                    )
                    raw_text, meta = ollama_generate_raw(
                        host=host,
                        model=model,
                        prompt=prompt,
                        schema=schema,
                        timeout=timeout,
                        num_predict=num_predict,
                        task=task,
                        retry_attempt=attempt,
                    )
                    parsed = validate_llm_output(raw_text, model_cls)
                    if task == "summary" and isinstance(parsed, SummaryOutput):
                        ok, reason = is_summary_substantive(parsed)
                        if not ok:
                            raise RuntimeError(f"Summary validated structurally but lacked substance: {reason}")
                    merged_meta = dict(meta)
                    merged_meta["host"] = host
                    merged_meta["host_label"] = host_label
                    return parsed, f"{host_label}:{model}", merged_meta
                except Exception as exc:
                    msg = f"{host_label}/{model} attempt {attempt}: {str(exc)}"
                    LOG.warning("Ollama failed | task=%s | %s", task, msg)
                    errors.append(msg)
                    if enable_repair_prompt and raw_text:
                        try:
                            repair_prompt = build_json_repair_prompt(raw_text, schema, model)
                            for repair_host_label, repair_host, repair_models in build_task_execution_plan("repair"):
                                for repair_model in repair_models:
                                    repaired_text, repair_meta = ollama_generate_raw(
                                        host=repair_host,
                                        model=repair_model,
                                        prompt=repair_prompt,
                                        schema=schema,
                                        timeout=max(45, int(timeout * 0.7)),
                                        num_predict=env_int("OLLAMA_REPAIR_NUM_PREDICT", max(120, int(num_predict * 0.7))),
                                        task="repair",
                                        retry_attempt=1,
                                    )
                                    repaired = validate_llm_output(repaired_text, model_cls)
                                    if task == "summary" and isinstance(repaired, SummaryOutput):
                                        ok, reason = is_summary_substantive(repaired)
                                        if not ok:
                                            raise RuntimeError(
                                                f"Repair output validated structurally but lacked substance: {reason}"
                                            )
                                    merged_meta = dict(repair_meta)
                                    merged_meta["repair_used"] = True
                                    merged_meta["initial_model"] = model
                                    merged_meta["initial_host"] = host
                                    merged_meta["initial_host_label"] = host_label
                                    merged_meta["repair_model"] = repair_model
                                    merged_meta["repair_host"] = repair_host
                                    merged_meta["repair_host_label"] = repair_host_label
                                    return repaired, f"{repair_host_label}:{repair_model}:repair", merged_meta
                        except Exception as repair_exc:
                            errors.append(f"{host_label}/{model} repair after attempt {attempt}: {str(repair_exc)}")
    raise RuntimeError(f"Ollama generation failed for task '{task}': " + " | ".join(errors))
# ============================================================
# ANALYSIS / SUMMARY / RELEVANCE
# ============================================================
def sentence_split(text: str) -> List[str]:
    return [normalize_space(p) for p in re.split(r"(?<=[.!?])\s+", text or "") if normalize_space(p)]
def select_evidence_sentences(article: ScrapedArticle, max_sentences: int = 8) -> List[str]:
    sentences = sentence_split(article.body_text)
    scored: List[Tuple[int, str]] = []
    for sentence in sentences:
        low = sentence.lower()
        score = 0
        score += sum(3 for term in DIRECT_METAL_TERMS if term in low)
        score += sum(2 for term in BROAD_METAL_TERMS if term in low)
        score += sum(1 for term in ANALYSIS_TERMS if term in low)
        score += sum(1 for term in SUPPLY_CHAIN_TERMS if term in low)
        if score > 0:
            scored.append((score, sentence))
    scored.sort(key=lambda item: item[0], reverse=True)
    selected: List[str] = []
    seen = set()
    for _, sentence in scored:
        key = sentence.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    return selected
def extract_key_paragraphs(article: ScrapedArticle, max_paragraphs: int = 4) -> List[str]:
    paragraphs = [normalize_space(p) for p in re.split(r"\n+", article.body_text or "") if normalize_space(p)]
    if len(paragraphs) <= max_paragraphs:
        return paragraphs
    selected = paragraphs[:2] + paragraphs[-2:]
    out: List[str] = []
    seen = set()
    for paragraph in selected:
        key = paragraph.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(paragraph)
    return out[:max_paragraphs]
def synthesize_llm_excerpt(article: ScrapedArticle, max_chars: Optional[int] = None) -> str:
    """
    Builds a stable excerpt even when site extraction is thin.
    This is where v5 leans more heavily on the RDP Ollama server.
    """
    limit = max_chars or env_int("LLM_EVIDENCE_MAX_CHARS", 5000)
    parts: List[str] = []
    if article.title:
        parts.append(f"TITLE: {article.title}")
    if article.author:
        parts.append(f"AUTHOR: {article.author}")
    if article.published_at:
        parts.append(f"PUBLISHED_AT: {article.published_at}")
    if article.body_text:
        parts.append(article.body_text)
    else:
        parts.append("No full body text was extracted; use title, metadata, and evidence carefully.")
    combined = "\n\n".join([p for p in parts if p])
    return truncate(combined, limit)
RELEVANCE_EXCLUDED_PAGE_TYPES = {
    "category_page",
    "archive_page",
    "market_data_page",
    "quote_page",
    "ticker_page",
    "futures_table_page",
    "symbol_landing_page",
}
def article_warning_value(article: ScrapedArticle, prefix: str, default: str = "") -> str:
    for warning in article.warnings or []:
        if isinstance(warning, str) and warning.startswith(prefix):
            return normalize_space(warning.split(":", 1)[1])
    return default
def article_warning_values(article: ScrapedArticle, prefix: str) -> List[str]:
    value = article_warning_value(article, prefix, "")
    if not value:
        return []
    return [normalize_space(item) for item in value.split(",") if normalize_space(item)]
def article_warning_bool(article: ScrapedArticle, prefix: str, default: bool = False) -> bool:
    value = article_warning_value(article, prefix, "")
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}
def article_warning_int(article: ScrapedArticle, prefix: str, default: int = 0) -> int:
    value = article_warning_value(article, prefix, "")
    if not value:
        return default
    try:
        return int(float(value))
    except Exception:
        return default
def normalize_target_metals(values: List[str]) -> List[str]:
    normalized: List[str] = []
    for value in values or []:
        token = normalize_space(value).lower()
        if token in {"aluminium", "aluminum", "al"}:
            token = "aluminum"
        if token in {"copper", "cu"}:
            token = "copper"
        if token in {"copper", "aluminum"} and token not in normalized:
            normalized.append(token)
    return normalized
def derive_scope_from_target_metals(metals: List[str], fallback_scope: str = "Not Relevant") -> str:
    metal_set = set(normalize_target_metals(metals))
    if {"copper", "aluminum"}.issubset(metal_set):
        return "Copper, Aluminum"
    if "copper" in metal_set:
        return "Copper"
    if "aluminum" in metal_set:
        return "Aluminum"
    return fallback_scope if fallback_scope in ALLOWED_SCOPES else "Not Relevant"
def page_type_override_allowed_for_article(article: ScrapedArticle, *, same_day_valid: bool, target_metals: List[str], market_context: bool, page_type: str) -> bool:
    if page_type not in RELEVANCE_EXCLUDED_PAGE_TYPES:
        return False
    narrative_chars = article_warning_int(article, "narrative_chars", 0)
    quote_signal = article_warning_value(article, "quote_signal", "low").lower()
    return (
        same_day_valid
        and bool(normalize_target_metals(target_metals))
        and market_context
        and narrative_chars >= env_int("PAGE_TYPE_OVERRIDE_MIN_CHARS", 700)
        and quote_signal != "high"
    )
def build_relevance_packet(article: ScrapedArticle, target_date: Optional[str]) -> dict:
    packet = build_llm_article_packet(article)
    verified_metals = normalize_target_metals(article_warning_values(article, "verified_metals"))
    page_type = article_warning_value(article, "page_type", "unknown") or "unknown"
    narrative_chars = article_warning_int(article, "narrative_chars", 0)
    quote_signal = article_warning_value(article, "quote_signal", "unknown") or "unknown"
    source_role = packet["source_role"]
    verified_same_day = article_warning_bool(article, "verified_same_day_date", False)
    verified_market_context = article_warning_bool(article, "verified_market_context", False)
    return {
        "title": article.title,
        "source_name": article.source_name,
        "source_url": article.source_url,
        "source_role": source_role,
        "target_date": target_date or today_iso(),
        "published_at": article.published_at,
        "scope_hint": article.scope,
        "page_type": page_type,
        "target_metals": verified_metals,
        "same_day_valid": verified_same_day,
        "market_context": verified_market_context,
        "narrative_chars": narrative_chars,
        "quote_signal": quote_signal,
        "date_sources_checked": article_warning_values(article, "date_sources_checked"),
        "acceptance_score": article_warning_int(article, "acceptance_score", int(getattr(article, "score", 0) or 0)),
        "acceptance_reasons": article_warning_values(article, "acceptance_reasons"),
        "reject_reasons": article_warning_values(article, "reject_reasons"),
        "evidence_sentences": packet["evidence_sentences"],
        "key_paragraphs": packet["key_paragraphs"],
        "excerpt_for_model": synthesize_llm_excerpt(article, max_chars=env_int("LLM_RELEVANCE_MAX_CHARS", 2600)),
        "structure_override_possible": page_type_override_allowed_for_article(
            article,
            same_day_valid=verified_same_day,
            target_metals=verified_metals,
            market_context=verified_market_context,
            page_type=page_type,
        ),
    }
def build_llm_article_packet(article: ScrapedArticle) -> dict:
    source_role = first_nonempty(
        *[
            warning.split(":", 1)[1]
            for warning in (article.warnings or [])
            if isinstance(warning, str) and warning.startswith("source_role:")
        ]
    )
    return {
        "source_name": article.source_name,
        "source_role": source_role or SOURCE_ROLE_MAP.get(article.source_name, "market commentary"),
        "source_url": article.source_url,
        "title": article.title,
        "author": article.author,
        "published_at": article.published_at,
        "scope_hint": article.scope,
        "fetch_method": article.fetch_method,
        "extraction_method": article.extraction_method,
        "score": article.score,
        "warnings": article.warnings or [],
        "evidence_sentences": select_evidence_sentences(
            article,
            max_sentences=env_int("SUMMARY_EVIDENCE_SENTENCES", 8),
        ),
        "key_paragraphs": [
            truncate(p, 800)
            for p in extract_key_paragraphs(
                article,
                max_paragraphs=env_int("SUMMARY_KEY_PARAGRAPHS", 4),
            )
        ],
        "excerpt_for_model": synthesize_llm_excerpt(article),
    }
def build_summary_evidence(article: ScrapedArticle) -> dict:
    packet = build_llm_article_packet(article)
    return {
        "title": packet["title"],
        "author": packet["author"],
        "published_at": packet["published_at"],
        "scope": packet["scope_hint"],
        "source_role": packet["source_role"],
        "evidence_sentences": packet["evidence_sentences"],
        "key_paragraphs": packet["key_paragraphs"],
        "source_name": packet["source_name"],
        "source_url": packet["source_url"],
        "fetch_method": packet["fetch_method"],
        "extraction_method": packet["extraction_method"],
        "warnings": packet["warnings"],
        "excerpt_for_model": packet["excerpt_for_model"],
    }
def build_analyst_prompt(article: ScrapedArticle) -> str:
    payload = {
        "source_name": article.source_name,
        "article_url": article.source_url,
        "title": article.title,
        "author": article.author,
        "published_at": article.published_at,
        "metal_or_scope": article.scope,
        "evidence": build_summary_evidence(article),
        "generated_at_utc": utc_iso_now(),
        "instructions": [
            "Extract the analyst view from the page evidence as faithfully as possible.",
            "Prefer evidence sentences and key paragraphs over generic filler text.",
            "If extraction is thin, still produce the best conservative synopsis from the title, excerpt, and visible evidence.",
            "Do not invent LME prices, forecasts, ratings, company actions, or supply-demand claims that are not present.",
            "If the article is mostly news rather than analyst commentary, summarize the market signal cautiously and keep stance Neutral or Mixed unless the evidence is stronger.",
            "If the article is broad base-metals commentary, say so explicitly in the theme/synopsis.",
            "Return valid JSON only and keep key points concrete.",
        ],
    }
    return (
        "You are an executive analyst-brief extraction assistant running behind a scraping pipeline.\n"
        "Your job is to turn imperfectly extracted market articles into structured analyst-review summaries for copper, aluminum, or broader base-metals coverage.\n"
        "Use only the supplied evidence.\n"
        "Do not browse.\n"
        "Do not invent facts.\n"
        "Use the exact response schema you were given.\n"
        "Return VALID JSON ONLY.\n\n"
        f"PAYLOAD:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
def build_relevance_prompt(article: ScrapedArticle, target_date: Optional[str]) -> str:
    payload = build_relevance_packet(article, target_date)
    payload["task"] = (
        "Decide whether this fully opened and verified page should proceed to the final same-day "
        "copper/aluminum analyst-intelligence synopsis stage."
    )
    payload["valid_scopes"] = ["Copper", "Aluminum", "Copper, Aluminum", "Broad Metals", "Not Relevant"]
    payload["rules"] = [
        "Only same-day or target-date-valid pages should reach should_summarize=true.",
        "The page must clearly be about copper, aluminum, or aluminium. Broad metals alone are not enough.",
        "Reject category pages, archive pages, quote screens, ticker pages, futures/price tables, symbol landing pages, and market-data reference pages unless the full-page evidence shows strong same-day narrative prose.",
        "Prefer should_summarize=false when the page is mostly quotes, tables, symbols, or widgets with weak narrative text.",
        "If the page is relevant background but not suitable for the daily narrative synopsis feed, set is_relevant=true and should_summarize=false.",
        "Use the verified page signals and evidence excerpt together. Do not invent facts.",
    ]
    return (
        "You are the final relevance gate for a fast one-shot daily analyst-intelligence pipeline.\n"
        "The page has already been discovered, opened, extracted, and locally verified.\n"
        "Your job is to make a structured go/no-go decision for synopsis generation.\n"
        "Use the exact schema you were given.\n"
        "Return VALID JSON ONLY.\n\n"
        f"PAYLOAD:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
def finalize_relevance_output(article: ScrapedArticle, parsed: RelevanceOutput) -> RelevanceOutput:
    target_metals = normalize_target_metals(parsed.target_metals or article_warning_values(article, "verified_metals"))
    same_day_valid = bool(parsed.same_day_valid or article_warning_bool(article, "verified_same_day_date", False))
    market_context = bool(parsed.market_context or article_warning_bool(article, "verified_market_context", False))
    page_type = normalize_space(parsed.page_type or article_warning_value(article, "page_type", "unknown")) or "unknown"
    excluded_page = page_type in RELEVANCE_EXCLUDED_PAGE_TYPES
    override_allowed = page_type_override_allowed_for_article(
        article,
        same_day_valid=same_day_valid,
        target_metals=target_metals,
        market_context=market_context,
        page_type=page_type,
    )
    should_summarize = bool(parsed.should_summarize)
    if not target_metals or not same_day_valid or not market_context:
        should_summarize = False
    if excluded_page and not override_allowed:
        should_summarize = False
    scope = parsed.scope if parsed.scope in ALLOWED_SCOPES else derive_scope_from_target_metals(target_metals, article.scope)
    is_relevant = bool(parsed.is_relevant or should_summarize)
    if not is_relevant and target_metals and market_context and not excluded_page:
        is_relevant = True
    reasoning_short = parsed.reasoning_short or parsed.reason
    if not reasoning_short:
        reasoning_short = "Structured relevance accepted" if should_summarize else "Structured relevance rejected"
    return RelevanceOutput(
        is_relevant=is_relevant,
        should_summarize=should_summarize,
        target_metals=target_metals,
        same_day_valid=same_day_valid,
        page_type=page_type,
        market_context=market_context,
        scope=scope,
        reasoning_short=reasoning_short,
        reason=reasoning_short,
    )
def log_relevance_backend_failure(article: ScrapedArticle, backend_name: str, model: str, exc: Exception) -> None:
    status_code = getattr(exc, "status_code", "")
    body_snippet = getattr(exc, "body_snippet", "")
    LOG.warning(
        "%s relevance failed | url=%s | model=%s | status=%s | error=%s | body=%s",
        backend_name,
        article.source_url,
        model,
        status_code,
        exc,
        body_snippet,
    )
def classify_relevance_with_groq(article: ScrapedArticle, target_date: Optional[str]) -> Tuple[RelevanceOutput, str, dict]:
    model = opt_env("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
    prompt = build_relevance_prompt(article, target_date)
    json_text, meta = groq_generate_structured(
        prompt=prompt,
        schema=RelevanceOutput.model_json_schema(),
        model=model,
        timeout=env_int("GROQ_RELEVANCE_TIMEOUT", min(45, env_int("GROQ_TIMEOUT", 60))),
        temperature=env_float("GROQ_RELEVANCE_TEMPERATURE", min(0.2, env_float("GROQ_TEMPERATURE", 0.2))),
        max_completion_tokens=env_int("GROQ_RELEVANCE_MAX_COMPLETION_TOKENS", 320),
    )
    parsed = finalize_relevance_output(article, RelevanceOutput.model_validate_json(json_text))
    return parsed, model, meta
def classify_relevance_with_ollama(article: ScrapedArticle, target_date: Optional[str]) -> Tuple[RelevanceOutput, str, dict]:
    prompt = build_relevance_prompt(article, target_date)
    parsed, model, meta = run_ollama_structured(
        prompt=prompt,
        model_cls=RelevanceOutput,
        timeout=env_int("OLLAMA_RELEVANCE_TIMEOUT", 60),
        num_predict=env_int("OLLAMA_RELEVANCE_NUM_PREDICT", 80),
        task="relevance",
    )
    return finalize_relevance_output(article, parsed), model, meta
def classify_relevance_rule(article: ScrapedArticle, target_date: Optional[str]) -> RelevanceOutput:
    packet = build_relevance_packet(article, target_date)
    target_metals = normalize_target_metals(packet.get("target_metals", []))
    same_day_valid = bool(packet.get("same_day_valid"))
    market_context = bool(packet.get("market_context"))
    page_type = normalize_space(str(packet.get("page_type") or "unknown")) or "unknown"
    narrative_chars = int(packet.get("narrative_chars") or 0)
    quote_signal = normalize_space(str(packet.get("quote_signal") or "unknown")).lower()
    structurally_allowed = (
        page_type not in RELEVANCE_EXCLUDED_PAGE_TYPES
        or bool(packet.get("structure_override_possible"))
    )
    should_summarize = (
        bool(target_metals)
        and same_day_valid
        and market_context
        and structurally_allowed
        and narrative_chars >= env_int("NARRATIVE_MIN_CHARS", 450)
        and quote_signal != "high"
    )
    is_relevant = bool(target_metals) and market_context
    scope = derive_scope_from_target_metals(target_metals, article.scope)
    reason_parts: List[str] = []
    if target_metals:
        reason_parts.append("target_metals_verified")
    if same_day_valid:
        reason_parts.append("same_day_verified")
    if market_context:
        reason_parts.append("market_context_verified")
    if not structurally_allowed:
        reason_parts.append(f"page_type_rejected:{page_type}")
    if narrative_chars < env_int("NARRATIVE_MIN_CHARS", 450):
        reason_parts.append("narrative_too_thin")
    if quote_signal == "high":
        reason_parts.append("quote_signal_high")
    reasoning = ", ".join(reason_parts) if reason_parts else "rule_based_relevance"
    return RelevanceOutput(
        is_relevant=is_relevant,
        should_summarize=should_summarize,
        target_metals=target_metals,
        same_day_valid=same_day_valid,
        page_type=page_type,
        market_context=market_context,
        scope=scope if is_relevant else "Not Relevant",
        reasoning_short=reasoning,
        reason=reasoning,
    )
def relevance_backend_order(backend: str) -> List[str]:
    selected_backend = select_backend(backend)
    if selected_backend == "groq":
        return ["groq", "ollama", "rule"]
    if selected_backend == "ollama":
        return ["ollama", "rule"]
    if selected_backend == "rule":
        return ["rule"]
    order: List[str] = []
    if groq_enabled():
        order.append("groq")
    if ollama_configured():
        order.append("ollama")
    if not order:
        order.append("rule")
    elif "rule" not in order:
        order.append("rule")
    return order
def relevance_filter_enabled(backend: str) -> bool:
    raw = os.getenv("USE_LLM_RELEVANCE_FILTER")
    if raw is not None and raw.strip():
        return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    return select_backend(backend) in {"groq", "ollama", "auto"}
def classify_relevance(article: ScrapedArticle, backend: str, target_date: Optional[str]) -> Tuple[RelevanceOutput, str]:
    if select_backend(backend) == "rule":
        decision = classify_relevance_rule(article, target_date)
        LOG.info(
            "Rule relevance | url=%s | is_relevant=%s | should_summarize=%s | page_type=%s | metals=%s | same_day_valid=%s | reason=%s",
            article.source_url,
            decision.is_relevant,
            decision.should_summarize,
            decision.page_type,
            decision.target_metals,
            decision.same_day_valid,
            decision.reasoning_short or decision.reason,
        )
        return decision, "rule"
    attempted_ai = False
    for option in relevance_backend_order(backend):
        if option == "groq":
            attempted_ai = True
            model = opt_env("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
            try:
                decision, used_model, meta = classify_relevance_with_groq(article, target_date)
                usage = meta.get("usage") if isinstance(meta.get("usage"), dict) else {}
                article.warnings.append(
                    "Groq relevance meta: "
                    f"model={used_model}, "
                    f"format={meta.get('response_format_mode')}, "
                    f"finish_reason={meta.get('finish_reason')}, "
                    f"prompt_tokens={usage.get('prompt_tokens')}, "
                    f"completion_tokens={usage.get('completion_tokens')}, "
                    f"total_tokens={usage.get('total_tokens')}"
                )
                LOG.info(
                    "Groq relevance | url=%s | is_relevant=%s | should_summarize=%s | page_type=%s | metals=%s | same_day_valid=%s | reason=%s",
                    article.source_url,
                    decision.is_relevant,
                    decision.should_summarize,
                    decision.page_type,
                    decision.target_metals,
                    decision.same_day_valid,
                    decision.reasoning_short or decision.reason,
                )
                return decision, "groq"
            except Exception as exc:
                log_relevance_backend_failure(article, "Groq", model, exc)
                article.warnings.append(f"Groq relevance failed: {str(exc)[:500]}")
                continue
        if option == "ollama":
            attempted_ai = True
            try:
                decision, used_model, meta = classify_relevance_with_ollama(article, target_date)
                article.warnings.append(
                    "Ollama relevance meta: "
                    f"model={used_model}, "
                    f"eval_count={meta.get('eval_count')}, "
                    f"prompt_eval_count={meta.get('prompt_eval_count')}, "
                    f"total_duration={meta.get('total_duration')}, "
                    f"repair_used={meta.get('repair_used', False)}"
                )
                LOG.info(
                    "Ollama relevance | url=%s | is_relevant=%s | should_summarize=%s | page_type=%s | metals=%s | same_day_valid=%s | reason=%s",
                    article.source_url,
                    decision.is_relevant,
                    decision.should_summarize,
                    decision.page_type,
                    decision.target_metals,
                    decision.same_day_valid,
                    decision.reasoning_short or decision.reason,
                )
                return decision, "ollama"
            except Exception as exc:
                LOG.warning("Ollama relevance failed | url=%s | error=%s", article.source_url, exc)
                article.warnings.append(f"Ollama relevance failed: {str(exc)[:500]}")
                continue
        if option == "rule":
            decision = classify_relevance_rule(article, target_date)
            if attempted_ai:
                LOG.info(
                    "Relevance fallback used | url=%s | fallback=rule | reason=backend_failure_or_unavailable",
                    article.source_url,
                )
            LOG.info(
                "Rule relevance | url=%s | is_relevant=%s | should_summarize=%s | page_type=%s | metals=%s | same_day_valid=%s | reason=%s",
                article.source_url,
                decision.is_relevant,
                decision.should_summarize,
                decision.page_type,
                decision.target_metals,
                decision.same_day_valid,
                decision.reasoning_short or decision.reason,
            )
            return decision, "rule"
    decision = classify_relevance_rule(article, target_date)
    return decision, "rule"
def infer_stance(text: str) -> str:
    low = (text or "").lower()
    bullish = ["bullish", "supportive", "upside", "tight supply", "higher prices", "rebound", "recovery"]
    bearish = ["bearish", "downside", "oversupply", "weaker demand", "lower prices", "pressure", "slowdown"]
    bull_hits = sum(1 for item in bullish if item in low)
    bear_hits = sum(1 for item in bearish if item in low)
    if bull_hits > bear_hits and bull_hits > 0:
        return "Bullish"
    if bear_hits > bull_hits and bear_hits > 0:
        return "Bearish"
    if bull_hits > 0 and bear_hits > 0:
        return "Mixed"
    return "Neutral"
def derive_theme(article: ScrapedArticle) -> str:
    text = f"{article.title} {article.body_text}".lower()
    if "supply" in text or "smelter" in text or "mine" in text or "concentrate" in text:
        return "Supply outlook"
    if "demand" in text or "consumption" in text or "construction" in text:
        return "Demand outlook"
    if "tariff" in text or "trade" in text or "import" in text or "export" in text:
        return "Trade and policy"
    if "premium" in text or "inventory" in text or "stocks" in text or "warehouse" in text:
        return "Market structure"
    if "forecast" in text or "outlook" in text or "analysis" in text or "insights" in text:
        return "Analyst outlook"
    if "base metals" in text or "industrial metals" in text:
        return "Broad metals commentary"
    return "General market commentary"
def build_rule_based_summary(article: ScrapedArticle) -> dict:
    evidence_sentences = select_evidence_sentences(article, max_sentences=5)
    selected = evidence_sentences if evidence_sentences else sentence_split(article.body_text)[:4]
    synopsis = " ".join(selected[:5]).strip()
    key_points = selected[:3] if selected else ["Relevant commentary detected but extraction was thin."]
    risk_flags: List[str] = []
    text_low = article.body_text.lower()
    if "uncertain" in text_low or "uncertainty" in text_low:
        risk_flags.append("Article highlights uncertainty")
    if "tariff" in text_low or "trade" in text_low:
        risk_flags.append("Trade-policy sensitivity")
    if "china" in text_low:
        risk_flags.append("China demand/supply dependence")
    if "inventory" in text_low or "stocks" in text_low:
        risk_flags.append("Inventory trend sensitivity")
    if not risk_flags:
        risk_flags.append("No explicit risk flag extracted")
    watch_items: List[str] = []
    if "supply" in text_low:
        watch_items.append("Monitor supply-side developments")
    if "demand" in text_low:
        watch_items.append("Monitor demand-side commentary")
    if "premium" in text_low or "inventory" in text_low:
        watch_items.append("Monitor inventories and premiums")
    if not watch_items:
        watch_items.append("Monitor follow-up analyst notes")
    return {
        "theme": derive_theme(article),
        "stance": infer_stance(article.title + "\n" + article.body_text),
        "synopsis": synopsis or truncate(article.body_text, 400),
        "key_points": key_points,
        "risk_flags": risk_flags[:3],
        "watch_items": watch_items[:3],
    }
def generate_with_ollama(article: ScrapedArticle) -> Tuple[dict, str]:
    prompt = build_analyst_prompt(article)
    parsed, model, meta = run_ollama_structured(
        prompt=prompt,
        model_cls=SummaryOutput,
        timeout=env_int("OLLAMA_SUMMARY_TIMEOUT", 90),
        num_predict=env_int("OLLAMA_SUMMARY_NUM_PREDICT", 180),
        task="summary",
    )
    ok, reason = is_summary_substantive(parsed)
    if not ok:
        raise RuntimeError(f"Summary was structurally valid but not substantive from model {model}: {reason}")
    article.warnings.append(
        "Ollama meta: "
        f"model={model}, "
        f"host_label={meta.get('host_label')}, "
        f"host={meta.get('host')}, "
        f"eval_count={meta.get('eval_count')}, "
        f"prompt_eval_count={meta.get('prompt_eval_count')}, "
        f"total_duration={meta.get('total_duration')}, "
        f"repair_used={meta.get('repair_used', False)}"
    )
    return parsed.model_dump(), "ollama_structured"


def groq_enabled() -> bool:
    return bool(opt_env("GROQ_API_KEY", ""))


def ollama_configured() -> bool:
    return bool(
        opt_env("LOCAL_OLLAMA_HOST", "")
        or opt_env("REMOTE_OLLAMA_HOST", "")
        or opt_env("OLLAMA_HOST", "")
    )


def log_summary_backend_failure(article: ScrapedArticle, backend_name: str, model: str, exc: Exception) -> None:
    status_code = getattr(exc, "status_code", "")
    body_snippet = getattr(exc, "body_snippet", "")
    LOG.warning(
        "%s summary failed | url=%s | model=%s | status=%s | error=%s | body=%s",
        backend_name,
        article.source_url,
        model,
        status_code,
        exc,
        body_snippet,
    )


def generate_with_groq(article: ScrapedArticle) -> Tuple[dict, str]:
    prompt = build_analyst_prompt(article)
    model = opt_env("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
    try:
        json_text, meta = groq_generate_structured(
            prompt=prompt,
            schema=SummaryOutput.model_json_schema(),
            model=model,
            timeout=env_int("GROQ_TIMEOUT", 60),
            temperature=env_float("GROQ_TEMPERATURE", 0.2),
            max_completion_tokens=env_int("GROQ_MAX_COMPLETION_TOKENS", 900),
        )
    except Exception as exc:
        log_summary_backend_failure(article, "Groq", model, exc)
        raise
    parsed = SummaryOutput.model_validate_json(json_text)
    ok, reason = is_summary_substantive(parsed)
    if not ok:
        raise RuntimeError(f"Groq summary was structurally valid but not substantive: {reason}")
    usage = meta.get("usage") if isinstance(meta.get("usage"), dict) else {}
    article.warnings.append(
        "Groq meta: "
        f"model={meta.get('model')}, "
        f"format={meta.get('response_format_mode')}, "
        f"finish_reason={meta.get('finish_reason')}, "
        f"prompt_tokens={usage.get('prompt_tokens')}, "
        f"completion_tokens={usage.get('completion_tokens')}, "
        f"total_tokens={usage.get('total_tokens')}"
    )
    return parsed.model_dump(), "groq_structured"


def summary_backend_order(backend: str) -> List[str]:
    backend = select_backend(backend)
    if backend == "groq":
        return ["groq", "ollama", "rule"]
    if backend == "ollama":
        return ["ollama", "rule"]
    if backend == "rule":
        return ["rule"]
    order: List[str] = []
    if groq_enabled():
        order.append("groq")
    if ollama_configured():
        order.append("ollama")
    if not order:
        order.append("rule")
    elif "rule" not in order:
        order.append("rule")
    return order

def select_backend(backend: str) -> str:
    value = (backend or "ollama").strip().lower()
    if value not in {"groq", "ollama", "auto", "rule"}:
        raise RuntimeError(f"Invalid backend '{backend}'. Allowed: ['auto', 'groq', 'ollama', 'rule']")
    return value
def generate_summary(article: ScrapedArticle, backend: str) -> Tuple[dict, str]:
    selected_backend = select_backend(backend)
    if selected_backend == "rule":
        return build_rule_based_summary(article), "rule_based"
    attempted_ai = False
    for option in summary_backend_order(selected_backend):
        if option == "groq":
            attempted_ai = True
            try:
                return generate_with_groq(article)
            except Exception as exc:
                article.warnings.append(f"Groq failed: {str(exc)[:500]}")
                continue
        if option == "ollama":
            attempted_ai = True
            try:
                return generate_with_ollama(article)
            except Exception as exc:
                LOG.warning("Ollama summary failed | url=%s | error=%s", article.source_url, exc)
                article.warnings.append(f"Ollama failed: {str(exc)[:500]}")
                continue
        if option == "rule":
            fallback_mode = "rule_based_fallback" if attempted_ai else "rule_based"
            LOG.info("Summary fallback used | url=%s | fallback_mode=%s | reason=backend_failure_or_unavailable", article.source_url, fallback_mode)
            return build_rule_based_summary(article), fallback_mode
    return build_rule_based_summary(article), "rule_based_fallback" if attempted_ai else "rule_based"
def maybe_filter_with_llm_relevance(
    articles: List[ScrapedArticle],
    backend: str,
    target_date: Optional[str],
) -> List[ScrapedArticle]:
    if not relevance_filter_enabled(backend):
        return articles
    filtered: List[ScrapedArticle] = []
    for article in articles:
        try:
            decision, used_backend = classify_relevance(article, backend, target_date)
            reason = decision.reasoning_short or decision.reason
            article.warnings.append(f"relevance_backend:{used_backend}")
            article.warnings.append(f"relevance_reason:{reason}")
            article.warnings.append(f"relevance_should_summarize:{decision.should_summarize}")
            if decision.target_metals:
                article.warnings.append("relevance_target_metals:" + ",".join(decision.target_metals))
            if decision.page_type:
                article.warnings.append(f"relevance_page_type:{decision.page_type}")
            if decision.scope in ALLOWED_SCOPES:
                article.scope = decision.scope
            if not decision.is_relevant or not decision.should_summarize or article.scope not in ALLOWED_SCOPES:
                LOG.info(
                    "Relevance rejected | url=%s | backend=%s | page_type=%s | metals=%s | same_day_valid=%s | reason=%s",
                    article.source_url,
                    used_backend,
                    decision.page_type,
                    decision.target_metals,
                    decision.same_day_valid,
                    reason,
                )
                continue
            filtered.append(article)
        except Exception as exc:
            article.warnings.append(f"LLM relevance failed: {str(exc)[:250]}")
            filtered.append(article)
    return filtered
# ============================================================
# OLLAMA RDP LAST-STEP FALLBACK SOURCE PROBE
# ============================================================
def build_ollama_source_probe_prompt(
    source_configs: List[ScrapeSourceConfig],
    requested_scope: str,
    target_date: Optional[str],
) -> str:
    source_payload = [
        {
            "name": cfg.name,
            "seed_urls": cfg.seed_urls[:3],
            "role": normalize_space(str(getattr(cfg, "source_role", ""))) or SOURCE_ROLE_MAP.get(cfg.name, "market commentary"),
            "role_detail": SOURCE_ROLE_MAP.get(cfg.name, "market commentary"),
            "include_hints": cfg.include_hints[:8],
        }
        for cfg in source_configs
    ]
    payload = {
        "task": "Normal scraping, relevance filtering, and synopsis generation did not produce enough usable rows. As a last-step fallback, check whether the server-side model already has usable analyst-review style knowledge or resource candidates from these requested sites for the requested market scope and date.",
        "requested_scope": requested_scope,
        "target_date": target_date or today_iso(),
        "sources": source_payload,
        "rules": [
            "Return items only if you can provide a conservative, source-named, structured item that plausibly comes from one of the requested sites.",
            "If freshness is uncertain, prefer returning no items and status fallback_to_scraping.",
            "Do not invent exact URLs if you do not know them; leave source_url empty instead.",
            "Do not invent prices, forecasts, analyst names, or publication dates.",
            "Only use scopes Copper, Aluminum, Copper, Aluminum, Broad Metals, or Not Relevant.",
            "If you do not have confident usable items already available from model-side knowledge, return status fallback_to_scraping with an empty items array.",
        ],
        "required_item_fields": [
            "source_name", "title", "scope", "theme", "stance", "synopsis", "key_points", "risk_flags", "watch_items", "confidence", "note"
        ],
    }
    return (
        "You are the last-step fallback resource probe for an analyst-brief pipeline running against an Ollama server on the RDP machine.\n"
        "Your job is NOT to browse.\n"
        "Your job is to say whether the model side already has any usable structured analyst-review items from the requested sources after the normal scrape-and-summarize pipeline failed to produce enough usable rows.\n"
        "If not, explicitly return status=fallback_to_scraping and items=[].\n"
        "Return VALID JSON ONLY.\n\n"
        f"PAYLOAD:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def build_sharepoint_fields_from_probe_item(item: OllamaSourceProbeItem, run_id: str) -> Dict[str, Any]:
    published_at = item.published_at or today_iso()
    return {
        "Title": truncate(item.title or f"{item.source_name} Analyst Brief", 250),
        "BriefDate": parse_dt_to_date(published_at) if published_at else today_iso(),
        "SourceName": item.source_name or "Ollama RDP Probe",
        "SourceUrl": item.source_url or "",
        "PublishedAt": published_at or "",
        "ArticleAuthor": "",
        "Metal": item.scope if item.scope in ALLOWED_SCOPES else "Broad Metals",
        "Theme": truncate(item.theme or "Ollama RDP source probe", 255),
        "Stance": truncate(item.stance or "Unknown", 50),
        "Synopsis": truncate(item.synopsis, 6000),
        "KeyPoints": truncate("\n".join(f"- {x}" for x in (item.key_points or []) if x), 6000),
        "RiskFlags": truncate("\n".join(f"- {x}" for x in (item.risk_flags or []) if x), 4000),
        "WatchItems": truncate("\n".join(f"- {x}" for x in (item.watch_items or []) if x), 4000),
        "GeneratedAt": utc_iso_now(),
        "GenerationMode": "ollama_rdp_last_fallback_probe",
        "Warnings": truncate(
            "\n".join([
                "Last-step Ollama RDP source probe fallback used after the local-first scrape and summary pipeline.",
                f"Probe confidence: {item.confidence}",
                item.note or "",
                "If freshness is critical, verify against source URL or allow scraping fallback.",
            ]),
            4000,
        ),
        "RawExcerpt": truncate(item.synopsis or "", 12000),
        "RunId": run_id,
        "UniqueKey": stable_key(item.source_url or "", item.title, published_at, item.source_name, "ollama_rdp_last_fallback_probe"),
    }


def try_ollama_source_probe(
    source_configs: List[ScrapeSourceConfig],
    requested_scope: str,
    target_date: Optional[str],
    run_id: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not env_bool("ENABLE_OLLAMA_SOURCE_PROBE", True):
        return [], {"status": "disabled", "note": "ENABLE_OLLAMA_SOURCE_PROBE is false"}
    prompt = build_ollama_source_probe_prompt(source_configs, requested_scope, target_date)
    parsed, model, meta = run_ollama_structured(
        prompt=prompt,
        model_cls=OllamaSourceProbeOutput,
        timeout=env_int("OLLAMA_SOURCE_PROBE_TIMEOUT", 45),
        num_predict=env_int("OLLAMA_SOURCE_PROBE_NUM_PREDICT", 260),
        task="probe",
    )
    usable_items: List[OllamaSourceProbeItem] = []
    for item in parsed.items:
        if item.scope not in ALLOWED_SCOPES:
            continue
        if requested_scope and requested_scope != "All" and not scope_matches(requested_scope, item.scope):
            continue
        if len((item.synopsis or "").strip()) < 40:
            continue
        if not item.key_points:
            continue
        usable_items.append(item)
    usable_items = usable_items[: env_int("OLLAMA_SOURCE_PROBE_MAX_ITEMS", 6)]
    rows = [build_sharepoint_fields_from_probe_item(item, run_id) for item in usable_items]
    info = {
        "status": parsed.status,
        "note": parsed.note,
        "model": model,
        "meta": meta,
        "items_returned": len(parsed.items),
        "items_usable": len(rows),
    }
    return rows, info

# ============================================================
# OUTPUT MAPPING
# ============================================================
def build_sharepoint_fields(article: ScrapedArticle, summary: dict, generation_mode: str, run_id: str) -> Dict[str, Any]:
    synopsis = normalize_space(str(summary.get("synopsis", "")))
    key_points = summary.get("key_points", [])
    risk_flags = summary.get("risk_flags", [])
    theme = normalize_space(str(summary.get("theme", "")))
    stance = normalize_space(str(summary.get("stance", "")))
    if stance not in {"Bullish", "Bearish", "Neutral", "Mixed", "Unknown"}:
        stance = "Neutral"
    watch_items = summary.get("watch_items", [])
    return {
        "Title": truncate(article.title or f"{article.source_name} Analyst Brief", 250),
        "BriefDate": parse_dt_to_date(article.published_at) if article.published_at else today_iso(),
        "SourceName": article.source_name,
        "SourceUrl": article.source_url,
        "PublishedAt": article.published_at or "",
        "ArticleAuthor": truncate(article.author, 255),
        "Metal": article.scope,
        "Theme": truncate(theme, 255),
        "Stance": truncate(stance, 50),
        "Synopsis": truncate(synopsis, 6000),
        "KeyPoints": truncate("\n".join(f"- {x}" for x in key_points if x), 6000),
        "RiskFlags": truncate("\n".join(f"- {x}" for x in risk_flags if x), 4000),
        "WatchItems": truncate("\n".join(f"- {x}" for x in watch_items if x), 4000),
        "GeneratedAt": utc_iso_now(),
        "GenerationMode": generation_mode,
        "Warnings": truncate("\n".join(article.warnings or []), 4000),
        "RawExcerpt": truncate(article.body_text, 12000),
        "RunId": run_id,
        "UniqueKey": stable_key(article.source_url, article.title, article.published_at),
    }
# ============================================================
# RUNTIME SETTINGS
# ============================================================
def ask_user_value(prompt: str, default: str) -> str:
    raw = input(f"{prompt} [Default: {default}]: ").strip()
    return raw if raw else default
def ask_user_int(prompt: str, default: int, min_value: Optional[int] = None) -> str:
    while True:
        raw = input(f"{prompt} [Default: {default}]: ").strip()
        if not raw:
            return str(default)
        try:
            value = int(raw)
            if min_value is not None and value < min_value:
                print(f"Please enter a number >= {min_value}")
                continue
            return str(value)
        except ValueError:
            print("Please enter a whole number.")
def ask_user_float(prompt: str, default: float, min_value: Optional[float] = None, max_value: Optional[float] = None) -> str:
    while True:
        raw = input(f"{prompt} [Default: {default}]: ").strip()
        if not raw:
            return str(default)
        try:
            value = float(raw)
            if min_value is not None and value < min_value:
                print(f"Please enter a number >= {min_value}")
                continue
            if max_value is not None and value > max_value:
                print(f"Please enter a number <= {max_value}")
                continue
            return str(value)
        except ValueError:
            print("Please enter a valid number.")
def prompt_local_model_choice(local_models: List[str], default_model: str) -> str:
    if not local_models:
        return ""
    default_model = default_model if default_model in local_models else local_models[0]
    default_index = local_models.index(default_model) + 1
    print("\nAvailable local Ollama models on this PC:\n")
    for idx, model in enumerate(local_models, start=1):
        marker = " (default)" if model == default_model else ""
        print(f"{idx}. {model}{marker}")
    while True:
        raw = input(f"\nChoose the local model number to use for relevance and synopsis [Default: {default_index}]: ").strip()
        if not raw:
            return default_model
        try:
            selected = int(raw)
            if 1 <= selected <= len(local_models):
                return local_models[selected - 1]
        except ValueError:
            pass
        print(f"Please choose a number between 1 and {len(local_models)}.")
def configure_runtime_llm_preferences(
    *,
    interactive: bool,
    choose_local_model: bool,
    requested_local_model: str = "",
) -> Dict[str, Any]:
    local_host = get_local_ollama_host()
    remote_host = get_remote_ollama_host()
    local_models = safe_ollama_list_models(local_host, refresh=True)
    remote_models = safe_ollama_list_models(remote_host, refresh=True)

    chosen_local_model = _match_model_name(local_models, requested_local_model) if requested_local_model else ""
    if not chosen_local_model:
        chosen_local_model = resolve_preferred_local_model("summary", local_models)
    if local_models and (choose_local_model or interactive):
        chosen_local_model = prompt_local_model_choice(local_models, chosen_local_model or local_models[0])

    if chosen_local_model:
        os.environ["LOCAL_SELECTED_MODEL"] = chosen_local_model

    return {
        "local_host": local_host,
        "remote_host": remote_host,
        "local_models": local_models,
        "remote_models": remote_models,
        "selected_local_model": chosen_local_model,
    }
def prompt_runtime_settings() -> None:
    print("\nNow choose the run settings.")
    print("You can press Enter to keep the default value shown.\n")
    os.environ["MAX_ARTICLES_TOTAL"] = ask_user_int("How many final articles do you want in total?", env_int("MAX_ARTICLES_TOTAL", 10), min_value=1)
    os.environ["PER_SOURCE_LIMIT"] = ask_user_int("How many final articles do you want from each source?", env_int("PER_SOURCE_LIMIT", 8), min_value=1)
    os.environ["SCRAPE_HEADROOM_MULTIPLIER"] = ask_user_int("How much discovery headroom should be used before filtering?", env_int("SCRAPE_HEADROOM_MULTIPLIER", 2), min_value=1)
    os.environ["OLLAMA_KEEP_ALIVE"] = ask_user_value("How long should the AI model stay loaded after use? (example: 10m, 30m, 1h)", opt_env("OLLAMA_KEEP_ALIVE", "10m"))
    os.environ["OLLAMA_TEMPERATURE"] = ask_user_float("How creative should the AI be? (Lower = safer/more factual)", env_float("OLLAMA_TEMPERATURE", 0), min_value=0.0, max_value=2.0)
    os.environ["OLLAMA_TOP_P"] = ask_user_float("How focused should the AI response be?", env_float("OLLAMA_TOP_P",0.85), min_value=0.0, max_value=1.0)
    os.environ["OLLAMA_RELEVANCE_TIMEOUT"] = ask_user_int("Maximum seconds allowed to decide relevance", env_int("OLLAMA_RELEVANCE_TIMEOUT", 5), min_value=5)
    os.environ["OLLAMA_SUMMARY_TIMEOUT"] = ask_user_int("Maximum seconds allowed to generate one summary", env_int("OLLAMA_SUMMARY_TIMEOUT", 90), min_value=5)
    os.environ["OLLAMA_RELEVANCE_NUM_PREDICT"] = ask_user_int("How detailed should the relevance check be?", env_int("OLLAMA_RELEVANCE_NUM_PREDICT", 60), min_value=20)
    os.environ["OLLAMA_SUMMARY_NUM_PREDICT"] = ask_user_int("How detailed should the summary be?", env_int("OLLAMA_SUMMARY_NUM_PREDICT", 185), min_value=50)
    print("\nYour chosen settings for this run have been loaded.\n")
# ============================================================
# SCRAPE ORCHESTRATION
# ============================================================
def resolve_target_date(explicit_target_date: str = "") -> Optional[str]:
    if explicit_target_date:
        return explicit_target_date.strip()
    if env_bool("TODAY_ONLY", True):
        return system_today_iso() if env_bool("TODAY_ONLY_USE_SYSTEM_DATE", True) else today_iso()
    return None
def fetch_articles_with_fallback(
    all_source_configs: List[ScrapeSourceConfig],
    selected_source_configs: List[ScrapeSourceConfig],
    requested_scope: str,
    target_date: Optional[str],
    max_articles_total: int,
    per_source_limit: int,
    backend: str,
    run_id: str,
) -> List[ScrapedArticle]:
    scrape_provider = opt_env("SCRAPE_PROVIDER", "auto").strip().lower() or "auto"
    default_headroom = 1 if scrape_provider == "firecrawl" else 2
    headroom_multiplier = max(1, env_int("SCRAPE_HEADROOM_MULTIPLIER", default_headroom))
    fetch_total = max_articles_total * headroom_multiplier
    fetch_per_source = max(per_source_limit, per_source_limit * headroom_multiplier)
    LOG.info(
        "Fetch sizing | provider=%s | headroom_multiplier=%s | fetch_total=%s | fetch_per_source=%s",
        scrape_provider,
        headroom_multiplier,
        fetch_total,
        fetch_per_source,
    )
    def _run(source_configs: List[ScrapeSourceConfig]) -> List[ScrapedArticle]:
        articles = scrape_sources(
            sources=source_configs,
            target_date=target_date,
            max_articles_total=fetch_total,
            per_source_limit=fetch_per_source,
            user_agent=opt_env(
                "HTTP_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            ),
        )
        articles = maybe_filter_with_llm_relevance(articles, backend=backend, target_date=target_date)
        scoped = [article for article in articles if scope_matches(requested_scope, article.scope)]
        return scoped[:max_articles_total]
    articles = _run(selected_source_configs)
    LOG.info(
        "Selected-source pass finished | provider=%s | selected_sources=%s | accepted_articles=%s",
        scrape_provider,
        [cfg.name for cfg in selected_source_configs],
        len(articles),
    )
    if articles:
        return articles
    if env_bool("ENABLE_TODAY_FALLBACK", True):
        fallback_sources = get_fallback_sources(all_source_configs, selected_source_configs)
        if fallback_sources:
            LOG.info(
                "No qualifying articles found from selected sources for date %s. Expanding to fallback sources: %s",
                target_date,
                [s.name for s in fallback_sources],
            )
            append_run_log(run_id, {
                "stage": "today_fallback_started",
                "target_date": target_date,
                "fallback_sources": [s.name for s in fallback_sources],
                "ts": utc_iso_now(),
            })
            return _run(fallback_sources)
    return []
# ============================================================
# MAIN
# ============================================================
def main() -> int:
    load_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parser = argparse.ArgumentParser(description="Analyst brief generator v9 orchestrator (local-model-first relevance and synopsis + server fallback + last-step probe fallback + SharePoint + email alerts).")
    parser.add_argument("--interactive", action="store_true", help="Prompt the user to choose market scope and sources")
    parser.add_argument("--metal", default="all", help="copper | aluminum | broad-metals | copper,aluminum | all")
    parser.add_argument("--sources", default="all", help="Comma-separated source names, e.g. Reuters,Argus Media,LME.com")
    parser.add_argument("--target-date", default="", help="Override article publish date filter in YYYY-MM-DD format")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to SharePoint")
    parser.add_argument("--backend", default=opt_env("SUMMARY_BACKEND", "ollama"), help="groq | ollama | auto | rule")
    parser.add_argument("--groq-model", default="", help="Override Groq model for structured synopsis generation")
    parser.add_argument("--scrape-provider", default=opt_env("SCRAPE_PROVIDER", ""), help="firecrawl | local | auto")
    parser.add_argument("--max-articles-total", type=int, default=env_int("MAX_ARTICLES_TOTAL", 20))
    parser.add_argument("--per-source-limit", type=int, default=env_int("PER_SOURCE_LIMIT", 8))
    parser.add_argument("--output-json", default="", help="Optional file path for dry-run JSON output")
    parser.add_argument("--test-email-alert", action="store_true", help="Send a test email alert and exit after Graph auth")
    parser.add_argument("--local-model", default="", help="Preferred local Ollama model for relevance and synopsis tasks")
    parser.add_argument("--choose-local-model", action="store_true", help="Show available local Ollama models and choose one at run start")
    parser.add_argument("--local-host", default="", help="Override local Ollama host (default: LOCAL_OLLAMA_HOST or http://127.0.0.1:11434)")
    parser.add_argument("--server-host", default="", help="Override fallback/server Ollama host (default: REMOTE_OLLAMA_HOST or OLLAMA_HOST)")
    parser.add_argument(
        "--email-alert-mode",
        default=opt_env("EMAIL_ALERT_MODE", "created_only"),
        help="created_only | created_or_duplicates | all_generated",
    )
    args = parser.parse_args()
    os.environ["EMAIL_ALERT_MODE"] = (args.email_alert_mode or opt_env("EMAIL_ALERT_MODE", "created_only")).strip()
    if args.local_host:
        os.environ["LOCAL_OLLAMA_HOST"] = args.local_host.strip().rstrip("/")
    if args.server_host:
        os.environ["REMOTE_OLLAMA_HOST"] = args.server_host.strip().rstrip("/")
        os.environ["OLLAMA_HOST"] = args.server_host.strip().rstrip("/")
    if args.local_model:
        os.environ["LOCAL_SELECTED_MODEL"] = args.local_model.strip()
    if args.groq_model:
        os.environ["GROQ_MODEL"] = args.groq_model.strip()
    if args.scrape_provider:
        os.environ["SCRAPE_PROVIDER"] = args.scrape_provider.strip().lower()
    source_configs = build_source_configs()
    requested_scope = "All"
    selected_source_configs = source_configs
    if args.interactive:
        requested_scope, selected_source_configs = prompt_user_preferences(source_configs)
        customize = input("\nDo you also want to customize run settings like article count and AI detail? (y/n): ").strip().lower()
        if customize in {"y", "yes"}:
            prompt_runtime_settings()
            args.max_articles_total = env_int("MAX_ARTICLES_TOTAL", args.max_articles_total)
            args.per_source_limit = env_int("PER_SOURCE_LIMIT", args.per_source_limit)
    else:
        requested_scope = normalize_requested_scope(args.metal)
        selected_source_configs = parse_requested_sources(args.sources, source_configs)
    if not selected_source_configs:
        raise RuntimeError("No source URLs configured.")
    run_id = str(uuid.uuid4())
    os.environ["SCRAPE_RUN_ID"] = run_id
    target_date = resolve_target_date(args.target_date)
    backend_order = summary_backend_order(args.backend)
    relevance_order = relevance_backend_order(args.backend)
    selected_backend = backend_order[0]
    groq_key_present = groq_enabled()
    groq_model = opt_env("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
    llm_features_needed = (
        selected_backend in {"groq", "ollama"}
        or env_bool("USE_LLM_RELEVANCE_FILTER", False)
        or env_bool("ENABLE_OLLAMA_SOURCE_PROBE", True)
    )
    ollama_runtime_needed = (
        selected_backend == "ollama"
        or (relevance_filter_enabled(args.backend) and bool(relevance_order) and relevance_order[0] == "ollama")
    )
    llm_runtime_info: Dict[str, Any] = {}
    if ollama_runtime_needed:
        llm_runtime_info = configure_runtime_llm_preferences(
            interactive=args.interactive,
            choose_local_model=args.choose_local_model,
            requested_local_model=args.local_model,
        )
    LOG.info("Run ID: %s", run_id)
    LOG.info("Active script: %s", Path(__file__).name)
    LOG.info("Requested scope: %s", requested_scope)
    LOG.info("Selected sources: %s", [s.name for s in selected_source_configs])
    LOG.info("Target date: %s", target_date)
    LOG.info(
        "Summary backend requested=%s selected=%s groq_key_present=%s groq_client_initialized=%s model=%s",
        args.backend,
        selected_backend,
        groq_key_present,
        GROQ_CLIENT_IMPORT_OK,
        groq_model,
    )
    LOG.info("Summary backend order: %s", backend_order)
    LOG.info("Relevance backend order: %s", relevance_order)
    LOG.info("Scrape provider: %s", opt_env("SCRAPE_PROVIDER", "auto") or "auto")
    LOG.info("Optional stack availability: %s", optional_imports_status())
    LOG.info("Active scrape core: %s", ACTIVE_SCRAPE_CORE)
    LOG.info(
        "Scrape settings | dynamic_discovery=%s | dynamic_article_render=%s | debug_enabled=%s | debug_sources=%s",
        env_bool("ENABLE_DYNAMIC_DISCOVERY", True),
        env_bool("ENABLE_DYNAMIC_ARTICLE_RENDER", True),
        env_bool("SCRAPE_DEBUG_ENABLED", False),
        opt_env("SCRAPE_DEBUG_SOURCES", ""),
    )
    append_run_log(run_id, {
        "stage": "run_started",
        "run_id": run_id,
        "backend": args.backend,
        "backend_order": backend_order,
        "relevance_backend_order": relevance_order,
        "selected_backend": selected_backend,
        "groq_key_present": groq_key_present,
        "groq_client_initialized": GROQ_CLIENT_IMPORT_OK,
        "active_script": Path(__file__).name,
        "scrape_provider": opt_env("SCRAPE_PROVIDER", "auto") or "auto",
        "sources": [s.name for s in selected_source_configs],
        "requested_scope": requested_scope,
        "target_date": target_date,
        "max_articles_total": args.max_articles_total,
        "per_source_limit": args.per_source_limit,
        "optional_imports": optional_imports_status(),
        "ts": utc_iso_now(),
    })
    if ollama_runtime_needed:
        LOG.info(
            "Local Ollama | host=%s | selected_model=%s | models=%s",
            llm_runtime_info.get("local_host", ""),
            llm_runtime_info.get("selected_local_model", ""),
            llm_runtime_info.get("local_models", []),
        )
        LOG.info(
            "Server Ollama | host=%s | models=%s",
            llm_runtime_info.get("remote_host", ""),
            llm_runtime_info.get("remote_models", []),
        )
        append_run_log(run_id, {
            "stage": "llm_preflight",
            "local_host": llm_runtime_info.get("local_host", ""),
            "remote_host": llm_runtime_info.get("remote_host", ""),
            "selected_local_model": llm_runtime_info.get("selected_local_model", ""),
            "local_models": llm_runtime_info.get("local_models", []),
            "remote_models": llm_runtime_info.get("remote_models", []),
            "ts": utc_iso_now(),
        })

    generated_rows: List[Dict[str, Any]] = []
    probe_info: Dict[str, Any] = {}

    if not generated_rows:
        articles = fetch_articles_with_fallback(
            all_source_configs=source_configs,
            selected_source_configs=selected_source_configs,
            requested_scope=requested_scope,
            target_date=target_date,
            max_articles_total=args.max_articles_total,
            per_source_limit=args.per_source_limit,
            backend=args.backend,
            run_id=run_id,
        )
        if not articles:
            LOG.warning("No qualifying articles found for target date %s after scraping and relevance filtering.", target_date)
            append_run_log(run_id, {"stage": "scrape_finished_no_articles", "target_date": target_date, "ts": utc_iso_now()})
            if env_bool("ENABLE_OLLAMA_SOURCE_PROBE", True):
                try:
                    probe_rows, probe_info = try_ollama_source_probe(
                        source_configs=selected_source_configs,
                        requested_scope=requested_scope,
                        target_date=target_date,
                        run_id=run_id,
                    )
                    append_run_log(run_id, {
                        "stage": "ollama_source_probe_finished",
                        "requested_scope": requested_scope,
                        "target_date": target_date,
                        "sources": [s.name for s in selected_source_configs],
                        "probe_info": probe_info,
                        "ts": utc_iso_now(),
                    })
                    if probe_rows:
                        generated_rows = probe_rows
                        LOG.info(
                            "Ollama source probe returned %d usable rows via %s as the last-step fallback.",
                            len(generated_rows),
                            probe_info.get("model", "unknown-model"),
                        )
                        append_run_log(run_id, {
                            "stage": "ollama_source_probe_last_fallback_used",
                            "rows": len(generated_rows),
                            "probe_info": probe_info,
                            "ts": utc_iso_now(),
                        })
                    else:
                        LOG.info(
                            "Ollama source probe returned no usable rows at the last-step fallback (status=%s).",
                            probe_info.get("status", "unknown"),
                        )
                except Exception as exc:
                    probe_info = {"status": "error", "error": str(exc)}
                    LOG.warning("Ollama source probe failed at the last-step fallback. Error=%s", exc)
                    append_run_log(run_id, {
                        "stage": "ollama_source_probe_failed",
                        "requested_scope": requested_scope,
                        "target_date": target_date,
                        "sources": [s.name for s in selected_source_configs],
                        "error": str(exc),
                        "ts": utc_iso_now(),
                    })
            if not generated_rows:
                append_run_log(run_id, {"stage": "run_finished_no_articles", "target_date": target_date, "ts": utc_iso_now()})
                return 0
        if articles:
            LOG.info("Relevant articles found: %d", len(articles))
            for idx, article in enumerate(articles, start=1):
                LOG.info("Generating summary %d/%d | %s", idx, len(articles), article.title)
                try:
                    summary, generation_mode = generate_summary(article, args.backend)
                    generated_rows.append(build_sharepoint_fields(article, summary, generation_mode, run_id))
                    append_run_log(run_id, {
                        "stage": "summary_generated",
                        "source_name": article.source_name,
                        "url": article.source_url,
                        "title": article.title,
                        "scope": article.scope,
                        "generation_mode": generation_mode,
                        "warnings": article.warnings,
                        "ts": utc_iso_now(),
                    })
                except Exception as exc:
                    LOG.exception("Generation failed for article: %s", article.source_url)
                    article.warnings.append(f"Generation failed: {str(exc)[:300]}")
                    summary = build_rule_based_summary(article)
                    generated_rows.append(build_sharepoint_fields(article, summary, "rule_based_after_error", run_id))
                    append_run_log(run_id, {
                        "stage": "summary_failed_fallback_used",
                        "source_name": article.source_name,
                        "url": article.source_url,
                        "title": article.title,
                        "error": str(exc),
                        "ts": utc_iso_now(),
                    })
    if args.dry_run:
        output = json.dumps(generated_rows, indent=2, ensure_ascii=False)
        if args.output_json:
            Path(args.output_json).write_text(output, encoding="utf-8")
            print(f"Dry-run output written to: {args.output_json}")
        else:
            print(output)
        append_run_log(run_id, {"stage": "dry_run_output_ready", "rows": len(generated_rows), "ts": utc_iso_now()})
        return 0
    tenant_id = must_env("TENANT_ID")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    graph_token = get_graph_app_token(authority)
    if args.test_email_alert:
        test_row = {
            "Title": "Analyst Brief Email Alert Test",
            "BriefDate": today_iso(),
            "SourceName": "System Test",
            "SourceUrl": "https://example.com/alert-test",
            "PublishedAt": utc_iso_now(),
            "Metal": requested_scope if requested_scope != "All" else "All Markets",
            "Stance": "Neutral",
            "Theme": "Email pipeline connectivity test",
            "Synopsis": "This is a direct test of the analyst brief email alert pipeline.",
            "KeyPoints": "- Graph token acquired\n- HTML digest built\n- Mail request submitted",
            "WatchItems": "- Confirm inbox receipt\n- Confirm sender mailbox\n- Confirm subject line",
        }
        result = send_email_alert_digest(
            graph_token=graph_token,
            alert_rows=[test_row],
            requested_scope=requested_scope,
            target_date=target_date,
            run_id=run_id,
        )
        LOG.info("Test email alert result: %s", result)
        append_run_log(run_id, {"stage": "test_email_alert_sent", "result": result, "ts": utc_iso_now()})
        return 0
    hostname = must_env("SHAREPOINT_HOSTNAME")
    site_path = must_env("SHAREPOINT_SITE_PATH")
    list_name = must_env("SHAREPOINT_ANALYST_LIST_NAME")
    LOG.info("Resolving SharePoint site and list...")
    site_id = graph_get_site_id(graph_token, hostname, site_path)
    list_id = graph_get_list_id(graph_token, site_id, list_name)
    created_count = 0
    skipped_count = 0
    created_rows_for_alert: List[Dict[str, Any]] = []
    skipped_duplicate_rows_for_alert: List[Dict[str, Any]] = []
    for fields in generated_rows:
        try:
            unique_key = fields.get("UniqueKey", "")
            existing = graph_query_existing_items(
                graph_token=graph_token,
                site_id=site_id,
                list_id=list_id,
                filter_expr=f"fields/UniqueKey eq '{unique_key}'" if unique_key else None,
                expand_fields=True,
            )
            if existing:
                skipped_count += 1
                skipped_duplicate_rows_for_alert.append(fields)
                LOG.info("Skipped duplicate: %s", fields.get("Title"))
                append_run_log(run_id, {
                    "stage": "sharepoint_skipped_duplicate",
                    "title": fields.get("Title"),
                    "unique_key": unique_key,
                    "ts": utc_iso_now(),
                })
                continue
            created = graph_create_list_item(graph_token, site_id, list_id, fields)
            created_count += 1
            created_rows_for_alert.append(fields)
            LOG.info("Created SharePoint item: %s", fields.get("Title"))
            append_run_log(run_id, {
                "stage": "sharepoint_created",
                "title": fields.get("Title"),
                "unique_key": unique_key,
                "response_id": created.get("id"),
                "ts": utc_iso_now(),
            })
        except Exception as exc:
            LOG.exception("Failed to create SharePoint item for '%s': %s", fields.get("Title"), exc)
            append_run_log(run_id, {
                "stage": "sharepoint_create_error",
                "title": fields.get("Title"),
                "error": str(exc),
                "ts": utc_iso_now(),
            })
    alert_rows, alert_mode = resolve_alert_rows(
        created_rows=created_rows_for_alert,
        skipped_duplicate_rows=skipped_duplicate_rows_for_alert,
        generated_rows=generated_rows,
    )
    LOG.info(
        "Email alert gate | mode=%s | created=%d | duplicates=%d | chosen_rows=%d | enabled=%s",
        alert_mode,
        len(created_rows_for_alert),
        len(skipped_duplicate_rows_for_alert),
        len(alert_rows),
        env_bool("ENABLE_EMAIL_ALERTS", False),
    )
    if env_bool("ENABLE_EMAIL_ALERTS", False):
        if alert_rows:
            try:
                email_result = send_email_alert_digest(
                    graph_token=graph_token,
                    alert_rows=alert_rows,
                    requested_scope=requested_scope,
                    target_date=target_date,
                    run_id=run_id,
                )
                append_run_log(run_id, {
                    "stage": "email_alert_sent",
                    "alert_mode": alert_mode,
                    "alert_rows_count": len(alert_rows),
                    "recipients": parse_email_list(opt_env("ALERT_EMAIL_TO", "")),
                    "result": email_result,
                    "ts": utc_iso_now(),
                })
            except Exception as exc:
                LOG.exception("Email alert failed: %s", exc)
                append_run_log(run_id, {
                    "stage": "email_alert_failed",
                    "alert_mode": alert_mode,
                    "error": str(exc),
                    "alert_rows_count": len(alert_rows),
                    "ts": utc_iso_now(),
                })
        else:
            LOG.info("Email alert skipped because there are no rows for mode '%s'.", alert_mode)
            append_run_log(run_id, {
                "stage": "email_alert_skipped",
                "reason": "no_rows_for_alert_mode",
                "alert_mode": alert_mode,
                "created_rows_count": len(created_rows_for_alert),
                "duplicate_rows_count": len(skipped_duplicate_rows_for_alert),
                "rows_total": len(generated_rows),
                "ts": utc_iso_now(),
            })
    else:
        append_run_log(run_id, {
            "stage": "email_alert_skipped",
            "reason": "disabled",
            "alert_mode": alert_mode,
            "created_rows_count": len(created_rows_for_alert),
            "duplicate_rows_count": len(skipped_duplicate_rows_for_alert),
            "rows_total": len(generated_rows),
            "ts": utc_iso_now(),
        })
    LOG.info("Done. Created=%d | Skipped duplicates=%d", created_count, skipped_count)
    append_run_log(run_id, {
        "stage": "run_finished",
        "created_count": created_count,
        "skipped_count": skipped_count,
        "rows_total": len(generated_rows),
        "email_alert_rows": len(alert_rows),
        "email_alert_mode": alert_mode,
        "ts": utc_iso_now(),
    })
    return 0
if __name__ == "__main__":
    raise SystemExit(main())


