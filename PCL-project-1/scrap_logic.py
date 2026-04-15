
from __future__ import annotations

"""
Scraping core refactor for analyst/news websites.

This module is intentionally focused on discovery, fetching, extraction, and
recency/topic filtering. It is designed to be integrated into your existing
SharePoint + Ollama pipeline, replacing the brittle scraping logic.

Key changes versus the original script:
- Separate discovery, transport, extraction, and filtering.
- Respect robots.txt and keep per-host backoff/circuit-breaker state.
- Never fail the whole source because a sitemap returns 403/429.
- Use staged scraping: discover -> metadata check -> full extraction only for shortlisted URLs.
- Reuse Playwright browser/context instead of launching a new browser per URL.
- Avoid Playwright's discouraged "networkidle" wait mode.
- Prefer RSS/sitemaps/listing pages before search-engine scraping.
- Centralize explicit fallbacks instead of chaining ternaries with `or`.

Safe to use with public pages and official feeds/sitemaps.
This module does not use proxies or bypass mechanisms.
"""

import gzip
import json
from io import BytesIO
import logging
import re
import time
import urllib.robotparser
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock, current_thread, get_ident, local
from typing import Any, Callable, Iterable, Iterator, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

try:
    import httpx
except Exception:  # pragma: no cover - optional
    httpx = None

try:
    import feedparser
except Exception:  # pragma: no cover - optional
    feedparser = None

try:
    import trafilatura
except Exception:  # pragma: no cover - optional
    trafilatura = None

try:
    from readability import Document as ReadabilityDocument
except Exception:  # pragma: no cover - optional
    ReadabilityDocument = None

try:
    from parsel import Selector
except Exception:  # pragma: no cover - optional
    Selector = None

try:
    import scrapy
    from scrapy.http import HtmlResponse
    from scrapy.linkextractors import LinkExtractor as ScrapyLinkExtractor
    from scrapy.selector import Selector as ScrapySelector
except Exception:  # pragma: no cover - optional
    scrapy = None
    HtmlResponse = None
    ScrapyLinkExtractor = None
    ScrapySelector = None

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional
    PdfReader = None

try:
    from htmldate import find_date
except Exception:  # pragma: no cover - optional
    find_date = None

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional
    sync_playwright = None

try:
    from firecrawl_client import FirecrawlClient, FirecrawlClientError
except Exception:  # pragma: no cover - optional
    FirecrawlClient = None

    class FirecrawlClientError(RuntimeError):
        pass


LOG = logging.getLogger("analyst.scrape")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_space(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def first_nonempty(*values: Optional[str]) -> str:
    for value in values:
        cleaned = normalize_space(value)
        if cleaned:
            return cleaned
    return ""


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="", query="")
    return urlunparse(parsed).rstrip("/")


def host_of(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


PREFERRED_SOURCE_LISTING_URLS: dict[str, list[str]] = {
    "argus media": [
        "https://www.argusmedia.com/en/news-and-insights/latest-market-news?filter_language=en-gb",
    ],
    "argus": [
        "https://www.argusmedia.com/en/news-and-insights/latest-market-news?filter_language=en-gb",
    ],
    "ainvest": [
        "https://www.ainvest.com/news/articles-latest/",
    ],
    "ai invest": [
        "https://www.ainvest.com/news/articles-latest/",
    ],
    "discovery alert": [
        "https://discoveryalert.com.au/articles/",
        "https://discoveryalert.com.au/author/muflih-hidayat/",
    ],
    "capital.com": [
        "https://capital.com/en-int/analysis/commodities-news",
    ],
    "capital": [
        "https://capital.com/en-int/analysis/commodities-news",
    ],
}


PREFERRED_SOURCE_KEYWORDS: dict[str, list[str]] = {
    "argus media": [
        "latest market news",
        "market news",
        "metals",
        "base metals",
        "non-ferrous",
        "copper",
        "aluminium",
        "aluminum",
        "market insight",
        "supply",
        "demand",
    ],
    "argus": [
        "latest market news",
        "market news",
        "metals",
        "base metals",
        "non-ferrous",
        "copper",
        "aluminium",
        "aluminum",
    ],
    "ainvest": [
        "articles latest",
        "latest news",
        "commodities",
        "metals",
        "copper",
        "aluminium",
        "aluminum",
        "analysis",
        "market news",
    ],
    "ai invest": [
        "articles latest",
        "latest news",
        "commodities",
        "metals",
        "copper",
        "aluminium",
        "aluminum",
        "analysis",
    ],
    "discovery alert": [
        "articles",
        "metals",
        "mining",
        "copper",
        "aluminium",
        "aluminum",
        "market",
        "analysis",
        "report",
    ],
    "capital.com": [
        "commodities news",
        "commodities",
        "analysis",
        "metals",
        "copper",
        "aluminium",
        "aluminum",
        "market outlook",
    ],
    "capital": [
        "commodities news",
        "commodities",
        "analysis",
        "metals",
        "copper",
        "aluminium",
        "aluminum",
    ],
}


def _lookup_source_defaults(mapping: dict[str, list[str]], source_name: str) -> list[str]:
    if not source_name:
        return []
    normalized_name = normalize_space(source_name).lower()
    matches: list[str] = []
    for key, values in mapping.items():
        normalized_key = normalize_space(key).lower()
        if normalized_name == normalized_key or normalized_key in normalized_name or normalized_name in normalized_key:
            matches.extend(values)
    return matches


def _dedupe_terms(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = normalize_space(value).lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def prioritized_seed_urls(source: "SourceConfig") -> list[str]:
    preferred = list(_lookup_source_defaults(PREFERRED_SOURCE_LISTING_URLS, getattr(source, "name", "")))
    ordered = preferred + list(getattr(source, "seed_urls", []) or [])
    out: list[str] = []
    seen: set[str] = set()
    for url in ordered:
        normalized = normalize_space(url)
        if not normalized:
            continue
        key = normalize_url(normalized)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def source_discovery_keywords(source: "SourceConfig", profile: Optional["DynamicProfile"] = None) -> list[str]:
    terms: list[str] = []
    terms.extend(DIRECT_TERMS)
    terms.extend(ANALYSIS_TERMS)
    terms.extend(getattr(source, "search_terms", []) or [])
    terms.extend(getattr(source, "category_terms", []) or [])
    terms.extend(getattr(source, "include_hints", []) or [])
    terms.extend(getattr(source, "report_terms", []) or [])
    terms.extend(_lookup_source_defaults(PREFERRED_SOURCE_KEYWORDS, getattr(source, "name", "")))
    if profile is not None:
        terms.extend(getattr(profile, "section_link_hints", []) or [])
    return _dedupe_terms(terms)


def keyword_relevance_bonus(title: str, url: str, keywords: Iterable[str]) -> int:
    text = f"{title} {url}".lower()
    score = 0
    for keyword in keywords:
        low_keyword = normalize_space(keyword).lower()
        if not low_keyword:
            continue
        if low_keyword in text:
            score += 2 if " " in low_keyword else 1
    return score


def parse_isoish_date(value: str | None) -> str:
    if not value:
        return ""
    text = normalize_space(value)
    if not text:
        return ""
    # lightweight parser only; keep your existing dateutil parser if preferred
    try:
        from dateutil import parser as dateparser  # local import for optional dependency
        dt = dateparser.parse(text)
        if dt is None:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    except Exception:
        return ""


def date_from_url(url: str) -> str:
    m = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/", url)
    if not m:
        return ""
    y, mth, d = map(int, m.groups())
    try:
        return datetime(y, mth, d, tzinfo=UTC).isoformat()
    except Exception:
        return ""


def looks_like_article(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    path = parsed.path.lower().rstrip("/")
    if path in {"", "/"}:
        return False
    bad = (
        "/tag/", "/tags/", "/author/", "/authors/", "/category/", "/categories/",
        "/login", "/signin", "/signup", "/register", "/privacy", "/terms",
        "/contact", "/about", "/careers", "/events", "/podcast", "/podcasts",
        "/video", "/videos"
    )
    return not any(x in path for x in bad)


def looks_like_pdf(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def looks_like_report_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(
        token in path
        for token in (
            ".pdf", "/report", "/reports", "/research", "/whitepaper", "/outlook",
            "/forecast", "/insight", "/insights", "/analysis", "/market-report",
        )
    )


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def meta_content(soup: BeautifulSoup, *names: str) -> str:
    for name in names:
        tag = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return normalize_space(tag["content"])
    return ""


def title_from_jsonld(soup: BeautifulSoup) -> dict[str, str]:
    best = {"title": "", "author": "", "published": "", "url": "", "body": ""}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        stack: list[Any] = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            if "@graph" in item:
                stack.append(item["@graph"])

            raw_type = item.get("@type")
            kinds: list[str] = []
            if isinstance(raw_type, list):
                kinds = [str(x).lower() for x in raw_type]
            elif isinstance(raw_type, str):
                kinds = [raw_type.lower()]

            if not any(k in {"article", "newsarticle", "blogposting", "report", "webpage"} for k in kinds):
                if not any(k in item for k in ("headline", "datePublished", "articleBody", "name")):
                    continue

            author = item.get("author", "")
            if isinstance(author, list):
                author = author[0] if author else ""
            if isinstance(author, dict):
                author = author.get("name", "")
            author = normalize_space(str(author))

            main = item.get("mainEntityOfPage")
            url = ""
            if isinstance(main, dict):
                url = main.get("@id") or main.get("url") or ""
            elif isinstance(main, str):
                url = main
            url = normalize_space(url or item.get("url") or "")

            best["title"] = first_nonempty(best["title"], item.get("headline"), item.get("name"))
            best["author"] = first_nonempty(best["author"], author)
            best["published"] = first_nonempty(best["published"], item.get("datePublished"), item.get("dateCreated"))
            best["url"] = first_nonempty(best["url"], url)

            body = normalize_space(str(item.get("articleBody") or ""))
            if len(body) > len(best["body"]):
                best["body"] = body
    return best


def find_publish_date(html: str, url: str) -> str:
    if find_date is not None:
        try:
            value = find_date(html, url=url)
            return parse_isoish_date(value)
        except Exception:
            pass
    return parse_isoish_date(date_from_url(url))


def collect_text(nodes: Iterable[Tag], min_len: int = 30) -> str:
    seen: set[str] = set()
    parts: list[str] = []
    boilerplate = (
        "advertisement", "newsletter", "sign up", "related articles", "read more",
        "follow us", "purchase licensing rights", "request a demo", "subscribe"
    )
    for node in nodes:
        text = normalize_space(node.get_text(" ", strip=True))
        if len(text) < min_len:
            continue
        low = text.lower()
        if any(x in low for x in boilerplate):
            continue
        if low in seen:
            continue
        seen.add(low)
        parts.append(text)
    return "\n".join(parts).strip()


def merge_text_blocks(*parts: Optional[str], max_chars: int = 6000) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = normalize_space(part)
        if not cleaned:
            continue
        low = cleaned.lower()
        if low in seen:
            continue
        if any(low in existing or existing in low for existing in seen):
            continue
        seen.add(low)
        merged.append(cleaned)
    text = "\n\n".join(merged).strip()
    if len(text) > max_chars:
        return text[:max_chars].rsplit(" ", 1)[0].strip()
    return text


def truncate_text(text: str, max_chars: int = 320) -> str:
    cleaned = normalize_space(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars].rsplit(" ", 1)[0].strip() + "..."


def postprocess_pdf_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"(?<=\w)-\s*\n\s*(?=\w)", "", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return normalize_space(text).replace(" .", ".")


def extract_pdf_text(pdf_bytes: bytes, max_pages: int = 20) -> tuple[str, str]:
    if PdfReader is None:
        return "", "pdf_reader_missing"
    try:
        reader = PdfReader(BytesIO(pdf_bytes), strict=False)
        parts: list[str] = []
        for idx, page in enumerate(reader.pages[:max_pages]):
            page_text = page.extract_text() or ""
            page_text = normalize_space(page_text)
            if page_text:
                parts.append(page_text)
        return postprocess_pdf_text("\n\n".join(parts)), "pypdf"
    except Exception:
        return "", "pdf_extract_failed"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class SourceConfig:
    name: str
    seed_urls: list[str]
    allowed_hosts: list[str]
    sitemap_urls: list[str] = field(default_factory=list)
    rss_urls: list[str] = field(default_factory=list)
    include_hints: list[str] = field(default_factory=list)
    exclude_hints: list[str] = field(default_factory=list)
    category_terms: list[str] = field(default_factory=list)
    search_terms: list[str] = field(default_factory=list)
    js_heavy: bool = False
    paywalled_preview_ok: bool = False
    min_body_chars: int = 180
    pdf_report_ok: bool = True
    source_role: str = "narrative_news"
    report_terms: list[str] = field(default_factory=lambda: ["report", "research", "analysis", "outlook", "forecast", "pdf"])


@dataclass(slots=True)
class Candidate:
    source_name: str
    url: str
    listing_url: str
    title_hint: str = ""
    published_hint: str = ""
    snippet_hint: str = ""
    method: str = ""
    score: int = 0


@dataclass(slots=True)
class FetchResult:
    requested_url: str
    final_url: str
    status_code: int
    text: str
    content_type: str
    method: str
    blocked: bool = False
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Article:
    source_name: str
    source_url: str
    title: str
    author: str
    published_at: str
    body_text: str
    scope: str
    fetch_method: str
    extraction_method: str
    discovery_published_hint: str = ""
    discovery_snippet: str = ""
    warnings: list[str] = field(default_factory=list)
    score: int = 0


@dataclass(slots=True)
class HostState:
    failures: int = 0
    blocked_until: Optional[datetime] = None
    sitemap_disabled: bool = False
    browser_only: bool = False
    last_status: int = 0

    def is_paused(self) -> bool:
        return self.blocked_until is not None and utc_now() < self.blocked_until


# ---------------------------------------------------------------------------
# Robots + rate limiting + transport
# ---------------------------------------------------------------------------

class RobotsCache:
    def __init__(self, user_agent: str) -> None:
        self.user_agent = user_agent
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}
        self._lock = Lock()

    def can_fetch(self, url: str) -> bool:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        with self._lock:
            parser = self._cache.get(robots_url)
            if parser is None:
                parser = urllib.robotparser.RobotFileParser()
                parser.set_url(robots_url)
                try:
                    parser.read()
                except Exception:
                    # Fail open, but only because many sites intermittently break robots fetches.
                    pass
                self._cache[robots_url] = parser
        try:
            return parser.can_fetch(self.user_agent, url)
        except Exception:
            return True


class HostRateLimiter:
    def __init__(self, default_delay: float = 1.5) -> None:
        self.default_delay = default_delay
        self._last_request: dict[str, float] = defaultdict(float)
        self._lock = Lock()

    def wait(self, host: str) -> None:
        with self._lock:
            last = self._last_request[host]
            now = time.monotonic()
            remaining = self.default_delay - (now - last)
            if remaining > 0:
                time.sleep(remaining)
            self._last_request[host] = time.monotonic()


class PlaywrightPool:
    def __init__(self, user_agent: str, enabled: bool = True) -> None:
        self.user_agent = user_agent
        self.enabled = enabled and sync_playwright is not None
        self._pw = None
        self._browser = None
        self._context = None

    def start(self) -> None:
        if not self.enabled or self._browser is not None:
            return
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(user_agent=self.user_agent)

    def close(self) -> None:
        try:
            if self._context is not None:
                self._context.close()
        finally:
            self._context = None
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            self._browser = None
        try:
            if self._pw is not None:
                self._pw.stop()
        finally:
            self._pw = None

    def fetch_html(self, url: str, timeout_ms: int = 25000) -> Optional[str]:
        if not self.enabled:
            return None
        self.start()
        page = self._context.new_page()
        try:
            # Avoid "networkidle"; it is unreliable for many production pages.
            page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in {"image", "media", "font"}
                else route.continue_(),
            )
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # A short, deterministic wait is usually better than "networkidle".
            page.wait_for_timeout(1200)
            return page.content()
        except Exception:
            return None
        finally:
            try:
                page.close()
            except Exception:
                pass


class HttpFetcher:
    def __init__(
        self,
        user_agent: str,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        per_host_delay: float = 1.5,
        enable_http2: bool = True,
        enable_browser_fallback: bool = True,
    ) -> None:
        self.user_agent = user_agent
        self.timeout = (connect_timeout, read_timeout)
        self.robots = RobotsCache(user_agent=user_agent)
        self.rate = HostRateLimiter(default_delay=per_host_delay)
        self.hosts: dict[str, HostState] = defaultdict(HostState)
        self.browser = PlaywrightPool(user_agent=user_agent, enabled=enable_browser_fallback)

        retry = Retry(
            total=3,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.6,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
        self.session = Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        })
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.httpx_client = None
        if enable_http2 and httpx is not None:
            try:
                self.httpx_client = httpx.Client(
                    http2=True,
                    timeout=httpx.Timeout(connect=connect_timeout, read=read_timeout, write=20.0, pool=20.0),
                    limits=httpx.Limits(max_connections=30, max_keepalive_connections=10),
                    headers=dict(self.session.headers),
                    follow_redirects=True,
                )
            except Exception:
                self.httpx_client = None

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        try:
            if self.httpx_client is not None:
                self.httpx_client.close()
        except Exception:
            pass
        self.browser.close()

    def _mark_failure(self, host: str, status_code: int) -> None:
        state = self.hosts[host]
        state.failures += 1
        state.last_status = status_code
        if status_code in {401, 403, 429}:
            state.blocked_until = utc_now() + timedelta(minutes=min(20, 2 * state.failures))
            if status_code == 403:
                state.sitemap_disabled = True

    def _mark_success(self, host: str) -> None:
        state = self.hosts[host]
        state.failures = 0
        state.blocked_until = None

    def fetch(self, url: str, allow_browser_fallback: bool = True, respect_robots: bool = True) -> FetchResult:
        host = host_of(url)
        state = self.hosts[host]

        if state.is_paused():
            return FetchResult(url, url, 0, "", "", "paused", blocked=True, warnings=["host paused"])

        if respect_robots and not self.robots.can_fetch(url):
            return FetchResult(url, url, 0, "", "", "robots_blocked", blocked=True, warnings=["robots disallow"])

        self.rate.wait(host)

        warnings: list[str] = []

        # First try requests
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            if response.status_code >= 400:
                self._mark_failure(host, response.status_code)
                warnings.append(f"requests_http_{response.status_code}")
            else:
                self._mark_success(host)
                ctype = response.headers.get("content-type", "")
                return FetchResult(url, str(response.url), response.status_code, response.text, ctype, "requests")
        except Exception as exc:
            warnings.append(f"requests_error:{type(exc).__name__}")

        # Then try HTTP/2 if available
        if self.httpx_client is not None:
            try:
                response = self.httpx_client.get(url)
                if response.status_code < 400:
                    self._mark_success(host)
                    ctype = response.headers.get("content-type", "")
                    return FetchResult(url, str(response.url), response.status_code, response.text, ctype, "httpx_http2", warnings=warnings)
                self._mark_failure(host, response.status_code)
                warnings.append(f"httpx_http_{response.status_code}")
            except Exception as exc:
                warnings.append(f"httpx_error:{type(exc).__name__}")

        # Browser fallback only for HTML pages, not XML feeds/sitemaps
        if allow_browser_fallback:
            html = self.browser.fetch_html(url)
            if html:
                self._mark_success(host)
                return FetchResult(url, url, 200, html, "text/html", "playwright", warnings=warnings)

        return FetchResult(url, url, state.last_status, "", "", "failed", blocked=True, warnings=warnings)

    def fetch_bytes(self, url: str, respect_robots: bool = True) -> tuple[bytes, FetchResult]:
        host = host_of(url)
        state = self.hosts[host]

        if state.is_paused():
            return b"", FetchResult(url, url, 0, "", "", "paused", blocked=True, warnings=["host paused"])

        if respect_robots and not self.robots.can_fetch(url):
            return b"", FetchResult(url, url, 0, "", "", "robots_blocked", blocked=True, warnings=["robots disallow"])

        self.rate.wait(host)
        warnings: list[str] = []

        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            if response.status_code >= 400:
                self._mark_failure(host, response.status_code)
                warnings.append(f"requests_http_{response.status_code}")
            else:
                self._mark_success(host)
                ctype = response.headers.get("content-type", "")
                text = ""
                if "text" in ctype or "html" in ctype or "xml" in ctype or "json" in ctype:
                    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
                    text = response.text
                return response.content, FetchResult(
                    url, str(response.url), response.status_code, text, ctype, "requests", warnings=warnings
                )
        except Exception as exc:
            warnings.append(f"requests_error:{type(exc).__name__}")

        if self.httpx_client is not None:
            try:
                response = self.httpx_client.get(url)
                if response.status_code < 400:
                    self._mark_success(host)
                    ctype = response.headers.get("content-type", "")
                    text = response.text if any(x in ctype for x in ("text", "html", "xml", "json")) else ""
                    return response.content, FetchResult(
                        url, str(response.url), response.status_code, text, ctype, "httpx_http2", warnings=warnings
                    )
                self._mark_failure(host, response.status_code)
                warnings.append(f"httpx_http_{response.status_code}")
            except Exception as exc:
                warnings.append(f"httpx_error:{type(exc).__name__}")

        return b"", FetchResult(url, url, state.last_status, "", "", "failed", blocked=True, warnings=warnings)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

PRIMARY_METALS_TERMS = (
    "copper", "aluminum", "aluminium",
)

BROAD_METALS_TERMS = (
    "base metals", "industrial metals", "metals", "metal", "non-ferrous", "nonferrous",
    "mining", "smelter", "smelters", "refined metal", "concentrate", "cathode",
    "wire rod", "scrap", "ore", "warehouse", "inventories", "supply", "demand", "lme",
)

DIRECT_TERMS = PRIMARY_METALS_TERMS + BROAD_METALS_TERMS

ANALYSIS_TERMS = (
    "analysis", "outlook", "commentary", "review", "insight", "insights",
    "forecast", "market", "market news", "pricing", "premiums", "treatment charges",
)

SHORTLIST_PRIORITY_TERMS = (
    "copper", "aluminum", "aluminium", "market", "news", "analysis",
    "outlook", "commentary", "update", "alert",
)

MARKET_INTELLIGENCE_TERMS = (
    "news", "market news", "market update", "update", "alert", "analysis",
    "analyst analysis", "commentary", "insight", "outlook", "report",
    "research", "review", "trend", "market signal", "exchange update",
    "inventory update", "supply update", "demand update", "price move",
    "price trend",
)

MARKET_STRUCTURE_TERMS = (
    "lme", "comex", "shfe", "inventory", "stocks", "warehouse", "spread",
    "premium", "deficit", "surplus", "demand", "supply", "smelter", "mine",
    "tariff", "arbitrage", "cathode", "concentrate", "scrap", "futures",
)

IRRELEVANT_COMPANY_TERMS = (
    "earnings", "quarter results", "stock price", "shares rose", "shares fell",
    "dividend", "ceo", "acquisition", "merger", "lawsuit",
)

NEGATIVE_TERMS = (
    "crypto", "bitcoin", "ethereum", "etf", "s&p 500", "nasdaq"
)


SYMBOL_CONTEXT_TERMS = MARKET_INTELLIGENCE_TERMS + MARKET_STRUCTURE_TERMS + (
    "commodity", "commodities", "price", "prices", "cash", "3m", "three-month",
    "exchange", "metal", "metals", "grade", "benchmark",
)


def contains_target_metal(text: str | None) -> bool:
    low = normalize_space(text).lower()
    if not low:
        return False
    return has_required_target_metal(low)


def _has_safe_symbol_context(text: str, symbol: str) -> bool:
    normalized_text = normalize_space(text)
    if not normalized_text:
        return False
    pattern = re.compile(rf"(?<![A-Za-z]){re.escape(symbol)}(?![A-Za-z])", re.IGNORECASE)
    if not pattern.search(normalized_text):
        return False
    lines = re.split(r"[\n\r|;]+", normalized_text)
    for line in lines:
        line_low = line.lower()
        if not pattern.search(line):
            continue
        if any(term in line_low for term in SYMBOL_CONTEXT_TERMS):
            return True
        if re.search(rf"(?i)\b(?:{re.escape(symbol)})\b\s*[:/\-]?\s*(?:cash|3m|price|prices|inventory|stocks|premium|spread|warehouse|supply|demand|market|futures)\b", line):
            return True
        if re.search(rf"(?i)\b(?:cash|3m|price|prices|inventory|stocks|premium|spread|warehouse|supply|demand|market|futures)\b\s*[:/\-]?\s*\b{re.escape(symbol)}\b", line):
            return True
    return any(term in normalized_text.lower() for term in SYMBOL_CONTEXT_TERMS)


def normalize_metal_mentions(text: str | None) -> list[str]:
    normalized_text = normalize_space(text)
    if not normalized_text:
        return []
    low = normalized_text.lower()
    mentions: list[str] = []
    if re.search(r"\bcopper\b", low) or _has_safe_symbol_context(normalized_text, "Cu"):
        mentions.append("copper")
    if re.search(r"\baluminum\b", low) or re.search(r"\baluminium\b", low) or _has_safe_symbol_context(normalized_text, "Al"):
        mentions.append("aluminum")
    return list(dict.fromkeys(mentions))


def has_primary_target_metal(text: str | None) -> bool:
    return bool(normalize_metal_mentions(text))


def has_required_target_metal(text: str | None) -> bool:
    return has_primary_target_metal(text)


def has_market_context(text: str | None) -> bool:
    low = normalize_space(text).lower()
    if not low:
        return False
    return any(term in low for term in MARKET_INTELLIGENCE_TERMS) or any(term in low for term in MARKET_STRUCTURE_TERMS)


def _same_day_url_match(url: str, target_date: Optional[str]) -> bool:
    if not target_date:
        return False
    dated = parse_isoish_date(date_from_url(url))
    return bool(dated and dated[:10] == target_date)


def _is_obviously_old_archive_url(url: str, target_date: Optional[str]) -> bool:
    if not target_date:
        return False
    dated = parse_isoish_date(date_from_url(url))
    if not dated:
        return False
    if dated[:10] == target_date:
        return False
    low = normalize_url(url).lower()
    if "/archive/" in low or "reuters.com" in host_of(low):
        return True
    return dated[:10] < target_date


def _looks_like_section_page(url: str) -> bool:
    path = urlparse(url).path.lower().strip("/")
    if not path:
        return True
    parts = [part for part in path.split("/") if part]
    generic = {
        "news", "analysis", "markets", "market", "insights", "insight",
        "commentary", "outlook", "reports", "report", "articles", "latest",
        "metals", "commodities", "updates", "alerts",
    }
    return len(parts) <= 2 and parts[-1] in generic


def _strong_market_signal(text: str | None) -> bool:
    low = normalize_space(text).lower()
    if not low or not has_required_target_metal(low):
        return False
    return has_market_context(low)


def candidate_score(title: str, url: str, keywords: Optional[Iterable[str]] = None, context_text: str = "") -> int:
    text = f"{title} {context_text} {url}".lower()
    score = 0
    score += 6 if has_primary_target_metal(text) else 0
    score += sum(2 for t in ANALYSIS_TERMS if t in text)
    score += 2 if has_market_context(text) else 0
    score -= sum(5 for t in NEGATIVE_TERMS if t in text)
    if not contains_target_metal(text):
        score -= env_int("DISCOVERY_NON_TARGET_PENALTY", 4)
    if keywords:
        score += keyword_relevance_bonus(f"{title} {context_text}", url, keywords)
    if len(normalize_space(context_text)) >= 80:
        score += 1
    if looks_like_report_url(url):
        score += 2
    if looks_like_pdf(url):
        score -= env_int("DISCOVERY_PDF_SCORE_PENALTY", 2)
    else:
        score += env_int("DISCOVERY_HTML_SCORE_BONUS", 1)
    if re.search(r"/20\d{2}/\d{1,2}/\d{1,2}/", url):
        score += 2
    if _looks_like_section_page(url):
        score -= 6
    return score


def source_accepts_url(source: SourceConfig, url: str) -> bool:
    normalized = normalize_url(url)
    host = host_of(normalized)
    keywords = source_discovery_keywords(source)
    if source.allowed_hosts and not any(host == h or host.endswith("." + h) for h in source.allowed_hosts):
        return False

    low = normalized.lower()
    is_pdf = looks_like_pdf(normalized)
    if is_pdf and not source.pdf_report_ok:
        return False

    if not is_pdf and not looks_like_article(normalized):
        return False

    if source.exclude_hints and any(h in low for h in source.exclude_hints):
        return False

    if is_pdf:
        return looks_like_report_url(normalized) or any(term in low for term in source.report_terms)

    if source.include_hints and not any(h in low for h in source.include_hints):
        # soft allow if URL still looks content-rich
        if candidate_score("", normalized, keywords) < 2 and not looks_like_report_url(normalized):
            return False
    return True


def discover_from_rss(source: SourceConfig, fetcher: HttpFetcher, per_feed: int = 25) -> list[Candidate]:
    if not source.rss_urls or feedparser is None:
        return []

    found: dict[str, Candidate] = {}
    for rss_url in source.rss_urls:
        result = fetcher.fetch(rss_url, allow_browser_fallback=False, respect_robots=False)
        if result.status_code >= 400 or not result.text:
            continue
        try:
            feed = feedparser.parse(result.text)
        except Exception:
            continue
        for entry in list(feed.entries or [])[:per_feed]:
            link = normalize_url(getattr(entry, "link", "") or "")
            if not link or not source_accepts_url(source, link):
                continue
            title = normalize_space(getattr(entry, "title", "") or "")
            published = normalize_space(getattr(entry, "published", "") or getattr(entry, "updated", "") or "")
            snippet = truncate_text(
                merge_text_blocks(
                    getattr(entry, "summary", "") or "",
                    getattr(entry, "description", "") or "",
                ),
                max_chars=360,
            )
            cand = Candidate(
                source.name,
                link,
                rss_url,
                title_hint=title,
                published_hint=published,
                snippet_hint=snippet,
                method="rss",
            )
            cand.score = candidate_score(title, link, source_discovery_keywords(source), context_text=snippet) + 4
            prev = found.get(link)
            if prev is None or cand.score > prev.score:
                found[link] = cand
    return sorted(found.values(), key=lambda x: x.score, reverse=True)


def iter_sitemap_urls(xml_bytes: bytes) -> Iterator[str]:
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()
    if "sitemapindex" in tag:
        for loc in root.iter():
            if loc.tag.lower().endswith("loc") and loc.text:
                yield normalize_space(loc.text)
    elif "urlset" in tag:
        for url in root:
            if not url.tag.lower().endswith("url"):
                continue
            for child in url:
                if child.tag.lower().endswith("loc") and child.text:
                    yield normalize_space(child.text)


def discover_from_sitemaps(source: SourceConfig, fetcher: HttpFetcher, max_sitemaps: int = 12, max_urls: int = 600) -> list[Candidate]:
    queue = deque(dict.fromkeys(source.sitemap_urls))
    keywords = source_discovery_keywords(source)
    for seed in prioritized_seed_urls(source):
        parsed = urlparse(seed)
        if parsed.scheme and parsed.netloc:
            base = f"{parsed.scheme}://{parsed.netloc}"
            queue.extend([
                f"{base}/sitemap.xml",
                f"{base}/sitemap_index.xml",
                f"{base}/news-sitemap.xml",
                f"{base}/sitemap-news.xml",
            ])

    seen_sitemaps: set[str] = set()
    found: dict[str, Candidate] = {}

    while queue and len(seen_sitemaps) < max_sitemaps and len(found) < max_urls:
        sitemap_url = normalize_url(queue.popleft())
        if not sitemap_url or sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)

        host_state = fetcher.hosts[host_of(sitemap_url)]
        if host_state.sitemap_disabled:
            continue

        raw, result = fetcher.fetch_bytes(sitemap_url, respect_robots=False)
        if result.status_code in {401, 403, 429}:
            fetcher.hosts[host_of(sitemap_url)].sitemap_disabled = True
            continue
        if not raw:
            continue

        if sitemap_url.endswith(".gz"):
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass

        try:
            root = ET.fromstring(raw)
        except Exception:
            continue

        tag = root.tag.lower()
        if "sitemapindex" in tag:
            for loc in iter_sitemap_urls(raw):
                if loc.endswith(".xml") or loc.endswith(".xml.gz"):
                    queue.append(loc)
            continue

        if "urlset" in tag:
            for elem in root:
                if len(found) >= max_urls:
                    break
                if not elem.tag.lower().endswith("url"):
                    continue

                loc_text = ""
                lastmod = ""
                for child in elem:
                    if child.tag.lower().endswith("loc") and child.text:
                        loc_text = normalize_space(child.text)
                    elif child.tag.lower().endswith("lastmod") and child.text:
                        lastmod = normalize_space(child.text)

                if not loc_text:
                    continue
                loc_text = normalize_url(loc_text)
                if not source_accepts_url(source, loc_text):
                    continue

                cand = Candidate(
                    source_name=source.name,
                    url=loc_text,
                    listing_url=sitemap_url,
                    title_hint="",
                    published_hint=lastmod,
                    method="sitemap",
                )
                cand.score = candidate_score("", loc_text, keywords) + 2
                prev = found.get(loc_text)
                if prev is None or cand.score > prev.score:
                    found[loc_text] = cand

    return sorted(found.values(), key=lambda x: x.score, reverse=True)


def extract_candidates_with_scrapy(source: SourceConfig, page_url: str, html: str) -> list[Candidate]:
    if HtmlResponse is None or ScrapyLinkExtractor is None:
        return []
    try:
        response = HtmlResponse(url=page_url, body=html.encode("utf-8"), encoding="utf-8")
        allow = tuple(re.escape(h) for h in (source.include_hints or []) if h)
        deny = tuple(re.escape(h) for h in (source.exclude_hints or []) if h)
        extractor = ScrapyLinkExtractor(
            allow=allow or (),
            deny=deny or (),
            allow_domains=source.allowed_hosts or (),
            unique=True,
        )
        candidates: list[Candidate] = []
        for link in extractor.extract_links(response):
            href = normalize_url(link.url)
            if not source_accepts_url(source, href):
                continue
            title = normalize_space(link.text or "")
            snippet = truncate_text(normalize_space(getattr(link, "fragment", "") or ""), max_chars=240)
            cand = Candidate(source.name, href, page_url, title_hint=title, snippet_hint=snippet, method="scrapy_linkextractor")
            cand.score = candidate_score(title, href, source_discovery_keywords(source), context_text=snippet) + 2
            candidates.append(cand)
        return candidates
    except Exception:
        return []


def discover_from_listing_pages(source: SourceConfig, fetcher: HttpFetcher, second_hop_limit: int = 3) -> list[Candidate]:
    found: dict[str, Candidate] = {}
    keywords = source_discovery_keywords(source)

    def extract(page_url: str) -> list[Candidate]:
        result = fetcher.fetch(page_url, allow_browser_fallback=True)
        if result.status_code >= 400 or not result.text:
            return []
        soup = soupify(result.text)
        local: dict[str, Candidate] = {}

        for cand in extract_candidates_with_scrapy(source, page_url, result.text):
            prev = local.get(cand.url)
            if prev is None or cand.score > prev.score:
                local[cand.url] = cand

        for a in soup.find_all("a", href=True):
            href = normalize_url(urljoin(page_url, a.get("href")))
            if not source_accepts_url(source, href):
                continue
            title = normalize_space(a.get_text(" ", strip=True))
            snippet = ""
            if len(title) < 8:
                parent = a.find_parent(["article", "li", "div", "section"])
                if parent:
                    heading = parent.find(["h1", "h2", "h3", "h4"])
                    title = first_nonempty(title, heading.get_text(" ", strip=True) if heading else "")
            published = ""
            parent = a.find_parent(["article", "li", "div", "section"])
            if parent:
                snippet = _candidate_snippet_from_context(a, parent)
                time_tag = parent.find("time")
                if time_tag:
                    published = normalize_space(time_tag.get("datetime") or time_tag.get_text(" ", strip=True))
            cand = Candidate(
                source.name,
                href,
                page_url,
                title_hint=title,
                published_hint=published,
                snippet_hint=snippet,
                method="listing",
            )
            if looks_like_pdf(href):
                cand.score = candidate_score(title, href, keywords, context_text=snippet) + 3
            else:
                cand.score = candidate_score(title, href, keywords, context_text=snippet)
            prev = local.get(href)
            if prev is None or cand.score > prev.score:
                local[href] = cand
        return list(local.values())

    section_pages: list[str] = []
    for seed in prioritized_seed_urls(source):
        for cand in extract(seed):
            prev = found.get(cand.url)
            if prev is None or cand.score > prev.score:
                found[cand.url] = cand

        # discover section pages by soft topical links
        seed_result = fetcher.fetch(seed, allow_browser_fallback=True)
        if seed_result.text:
            soup = soupify(seed_result.text)
            for a in soup.find_all("a", href=True):
                href = normalize_url(urljoin(seed, a.get("href")))
                text = normalize_space(a.get_text(" ", strip=True)).lower()
                if host_of(href) != host_of(seed):
                    continue
                if source_accepts_url(source, href):
                    continue
                if any(t in text or t in href.lower() for t in keywords):
                    section_pages.append(href)

    seen_sections: set[str] = set()
    for section in section_pages[:second_hop_limit]:
        if section in seen_sections:
            continue
        seen_sections.add(section)
        for cand in extract(section):
            cand.method = "second_hop"
            cand.score += 1
            prev = found.get(cand.url)
            if prev is None or cand.score > prev.score:
                found[cand.url] = cand

    return sorted(found.values(), key=lambda x: x.score, reverse=True)


def _search_provider_alias(provider: str) -> str:
    normalized = normalize_space(provider).lower()
    aliases = {
        "duck": "duckduckgo",
        "duckduck": "duckduckgo",
        "duckducky": "duckduckgo",
        "ddg": "duckduckgo",
        "edge": "bing",
    }
    return aliases.get(normalized, normalized)


def _search_provider_request(provider: str, query: str) -> tuple[str, str]:
    actual_provider = _search_provider_alias(provider)
    if actual_provider == "google":
        return actual_provider, f"https://www.google.com/search?{urlencode({'q': query, 'hl': 'en', 'num': 10})}"
    if actual_provider == "duckduckgo":
        return actual_provider, f"https://html.duckduckgo.com/html/?{urlencode({'q': query})}"
    if actual_provider == "brave":
        return actual_provider, f"https://search.brave.com/search?{urlencode({'q': query, 'source': 'web'})}"
    return actual_provider, f"https://www.bing.com/search?{urlencode({'q': query, 'setlang': 'en-US', 'count': 10})}"


def _search_result_url(provider: str, href: str) -> str:
    if not href:
        return ""
    normalized_provider = _search_provider_alias(provider)
    raw = href.strip()
    if normalized_provider == "google" and raw.startswith("/url?"):
        parsed = urlparse(raw)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        raw = query.get("q", "") or query.get("url", "") or ""
    elif raw.startswith("/l/?"):
        parsed = urlparse(raw)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        raw = query.get("uddg", "") or query.get("u", "") or ""
    if not raw or raw.startswith("/search") or raw.startswith("#"):
        return ""
    if raw.startswith("/"):
        return ""
    return normalize_url(raw)


def _target_date_query_tokens(target_date: str) -> list[str]:
    try:
        target = date.fromisoformat(target_date)
    except Exception:
        return [target_date]
    return [
        target_date,
        f"{target.strftime('%B')} {target.day}, {target.year}",
        f"{target.strftime('%b')} {target.day}, {target.year}",
        "today",
    ]


def discover_from_search_engines(source: SourceConfig, fetcher: HttpFetcher, target_date: str, per_query: int = 6) -> list[Candidate]:
    if not env_bool("ENABLE_SEARCH_ENGINE_DISCOVERY", True):
        return []

    providers = _deep_list(opt_env("SEARCH_DISCOVERY_PROVIDERS", "google,edge,brave,duckduckgo"))
    if not providers:
        providers = ["google", "edge", "brave", "duckduckgo"]

    hosts = list(dict.fromkeys(source.allowed_hosts or []))
    if not hosts:
        return []

    date_tokens = _target_date_query_tokens(target_date)
    found: dict[str, Candidate] = {}
    keywords = source_discovery_keywords(source)

    for provider in providers:
        for host in hosts[: max(1, env_int("SEARCH_DISCOVERY_MAX_HOSTS", 2))]:
            for metal in ("copper", "aluminum"):
                query = f"site:{host} {metal} ({' OR '.join(f'\"{token}\"' if ' ' in token or '-' in token else token for token in date_tokens)})"
                provider_name, search_url = _search_provider_request(provider, query)
                result = fetcher.fetch(search_url, allow_browser_fallback=False, respect_robots=False)
                if result.status_code >= 400 or not result.text:
                    continue

                soup = soupify(result.text)
                added = 0
                for anchor in soup.find_all("a", href=True):
                    href = _search_result_url(provider_name, anchor.get("href") or "")
                    if not href or not _has_allowed_host(source, href):
                        continue
                    if not _soft_accept_candidate_url(source, href):
                        continue
                    container = anchor.find_parent(["article", "div", "li", "section"])
                    title = _candidate_title_from_context(anchor, container)
                    snippet = _candidate_snippet_from_context(anchor, container)
                    published = _published_from_node(container)
                    topical_text = f"{title}\n{snippet}\n{href}"
                    if not contains_target_metal(topical_text):
                        continue

                    candidate = Candidate(
                        source.name,
                        href,
                        search_url,
                        title_hint=title,
                        published_hint=published,
                        snippet_hint=snippet,
                        method=f"search_{normalize_space(provider).lower()}",
                    )
                    candidate.score = candidate_score(title, href, keywords, context_text=snippet) + 5
                    if contains_target_metal(title):
                        candidate.score += 2
                    if _date_hint_matches_target(published, target_date):
                        candidate.score += 4
                    _merge_candidate(found, candidate)
                    added += 1
                    if added >= per_query:
                        break

    return sorted(found.values(), key=lambda x: x.score, reverse=True)


def _discover_candidates_base(source: SourceConfig, fetcher: HttpFetcher, target_date: Optional[str] = None) -> list[Candidate]:
    merged: dict[str, Candidate] = {}
    for strategy in (
        discover_from_rss,
        discover_from_sitemaps,
        discover_from_listing_pages,
    ):
        try:
            if strategy is discover_from_sitemaps:
                candidates = strategy(source, fetcher, max_sitemaps=12, max_urls=600)
            else:
                candidates = strategy(source, fetcher)
        except Exception as exc:
            LOG.warning("Discovery strategy failed | source=%s | strategy=%s | error=%s", source.name, strategy.__name__, exc)
            continue
        for cand in candidates:
            prev = merged.get(cand.url)
            if prev is None or cand.score > prev.score:
                merged[cand.url] = cand
    if target_date:
        try:
            candidates = discover_from_search_engines(source, fetcher, target_date)
        except Exception as exc:
            LOG.warning("Discovery strategy failed | source=%s | strategy=%s | error=%s", source.name, "discover_from_search_engines", exc)
        else:
            for cand in candidates:
                _merge_candidate(merged, cand)
    return sorted(merged.values(), key=lambda x: x.score, reverse=True)


# ---------------------------------------------------------------------------
# Extraction + filtering
# ---------------------------------------------------------------------------

def classify_scope(title: str, body: str) -> str:
    text = f"{title}\n{body}".lower()
    copper = "copper" in text
    aluminum = "aluminum" in text or "aluminium" in text
    broad = any(term in text for term in BROAD_METALS_TERMS)
    if copper and aluminum:
        return "Copper, Aluminum"
    if copper:
        return "Copper"
    if aluminum:
        return "Aluminum"
    if broad:
        return "Broad Metals"
    return "Unknown"


def fast_relevance_score(title: str, body: str) -> int:
    title_text = (title or "").lower()
    body_text = (body or "").lower()
    text = f"{title_text}\n{body_text}"
    score = 0
    score += sum(5 for t in PRIMARY_METALS_TERMS if t in title_text)
    score += sum(3 for t in PRIMARY_METALS_TERMS if t in body_text)
    score += sum(3 for t in BROAD_METALS_TERMS if t in title_text)
    score += sum(2 for t in BROAD_METALS_TERMS if t in body_text)
    score += sum(1 for t in ANALYSIS_TERMS if t in text)
    score -= sum(5 for t in NEGATIVE_TERMS if t in text)
    return score


def extract_main_text(html: str, url: str, source: SourceConfig) -> tuple[str, str]:
    if trafilatura is not None:
        try:
            text = trafilatura.extract(
                html,
                url=url,
                include_comments=False,
                include_tables=False,
                include_links=False,
                deduplicate=True,
                favor_precision=True,
                output_format="txt",
                with_metadata=False,
            )
            text = normalize_space(text)
            if len(text) >= source.min_body_chars:
                return text, "trafilatura.extract"
        except Exception:
            pass

    if ReadabilityDocument is not None:
        try:
            doc = ReadabilityDocument(html)
            summary_html = doc.summary()
            soup = soupify(summary_html)
            text = collect_text(soup.find_all(["p", "li"]))
            if len(text) >= source.min_body_chars:
                return text, "readability"
        except Exception:
            pass

    soup = soupify(html)
    for sel in (
        "article", "main", "[role=main]", ".article-body", ".article-content",
        ".entry-content", ".post-content", ".story-content", ".content", ".rich-text"
    ):
        try:
            nodes = soup.select(f"{sel} p, {sel} li")
            text = collect_text(nodes)
            if len(text) >= source.min_body_chars:
                return text, f"css:{sel}"
        except Exception:
            continue

    if Selector is not None:
        try:
            sel = Selector(text=html)
            text = collect_text([BeautifulSoup(f"<p>{p}</p>", "lxml").p for p in sel.css("article p::text, main p::text").getall()])
            if len(text) >= source.min_body_chars:
                return text, "parsel"
        except Exception:
            pass

    soup = soupify(html)
    fallback = collect_text(soup.find_all("p"), min_len=40)
    if fallback:
        return fallback, "paragraphs"

    return "", "empty"


def extract_pdf_article(source: SourceConfig, candidate: Candidate, fetcher: HttpFetcher) -> Optional[Article]:
    raw, result = fetcher.fetch_bytes(candidate.url, respect_robots=True)
    if result.status_code >= 400 or not raw:
        return None
    body_text, pdf_method = extract_pdf_text(raw, max_pages=12)
    if not body_text:
        return None
    title = candidate.title_hint or Path(urlparse(candidate.url).path).name.replace(".pdf", "").replace("-", " ").replace("_", " ")
    published_iso = parse_isoish_date(candidate.published_hint) or parse_isoish_date(date_from_url(candidate.url))
    scope = classify_scope(title, body_text)
    warnings = list(result.warnings)
    warnings.append("pdf_extracted")
    return Article(
        source_name=source.name,
        source_url=normalize_url(candidate.url),
        title=normalize_space(title),
        author="",
        published_at=published_iso,
        body_text=body_text,
        scope=scope,
        fetch_method=result.method,
        extraction_method=f"pdf:{pdf_method}",
        discovery_published_hint=candidate.published_hint,
        discovery_snippet=candidate.snippet_hint,
        warnings=warnings,
        score=fast_relevance_score(title, body_text) + 2,
    )


def extract_article(source: SourceConfig, candidate: Candidate, fetcher: HttpFetcher) -> Optional[Article]:
    if looks_like_pdf(candidate.url):
        return extract_pdf_article(source, candidate, fetcher)

    result = fetcher.fetch(candidate.url, allow_browser_fallback=True)
    if "pdf" in (result.content_type or "").lower():
        return extract_pdf_article(source, candidate, fetcher)
    if result.status_code >= 400 or not result.text:
        return None

    soup = soupify(result.text)
    jsonld = title_from_jsonld(soup)

    title = first_nonempty(
        jsonld.get("title"),
        meta_content(soup, "og:title", "twitter:title"),
        soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "",
        candidate.title_hint,
        soup.title.get_text(" ", strip=True) if soup.title else "",
    )
    author = first_nonempty(
        jsonld.get("author"),
        meta_content(soup, "author", "article:author", "parsely-author", "twitter:creator"),
    )
    published = first_nonempty(
        jsonld.get("published"),
        meta_content(soup, "article:published_time", "og:published_time", "publish-date", "parsely-pub-date"),
        candidate.published_hint,
        find_publish_date(result.text, result.final_url),
        date_from_url(result.final_url),
    )
    body_text, extraction_method = extract_main_text(result.text, result.final_url, source)

    # metadata-only fallback for preview/paywalled pages
    if len(body_text) < max(80, source.min_body_chars // 2):
        meta_desc = meta_content(soup, "description", "og:description", "twitter:description")
        if source.paywalled_preview_ok and (title or meta_desc):
            body_text = first_nonempty(meta_desc, jsonld.get("body"), title)
            extraction_method += "+metadata_only"

    if not title:
        return None

    published_iso = parse_isoish_date(published)
    scope = classify_scope(title, body_text)

    article = Article(
        source_name=source.name,
        source_url=normalize_url(jsonld.get("url") or result.final_url),
        title=title,
        author=author,
        published_at=published_iso,
        body_text=body_text,
        scope=scope,
        fetch_method=result.method,
        extraction_method=extraction_method,
        warnings=list(result.warnings),
    )
    article.score = fast_relevance_score(article.title, article.body_text)
    return article


def is_recent_enough(article: Article, target_date: Optional[str] = None, allow_missing_date: bool = False) -> bool:
    if not target_date:
        return True
    if not article.published_at:
        return allow_missing_date
    return article.published_at[:10] == target_date


def _article_same_day_signal(article: Article, target_date: Optional[str]) -> bool:
    effective_target_date = _effective_target_date(target_date)
    if article.published_at and article.published_at[:10] == effective_target_date:
        return True
    return _date_hint_matches_target(article.discovery_published_hint, effective_target_date)


def topic_matches(article: Article) -> bool:
    title_text = normalize_space(article.title)
    snippet_text = normalize_space(article.discovery_snippet)
    body_text = normalize_space(article.body_text)
    combined_text = f"{title_text}\n{snippet_text}\n{body_text}"
    if not has_primary_target_metal(combined_text):
        return False
    title_snippet = f"{title_text}\n{snippet_text}"
    title_snippet_score = fast_relevance_score(title_text, snippet_text)
    combined_score = fast_relevance_score(title_text, f"{snippet_text}\n{body_text}")
    if any(term in combined_text.lower() for term in IRRELEVANT_COMPANY_TERMS) and not (_strong_market_signal(title_snippet) or has_market_context(combined_text)):
        return False
    if _strong_market_signal(title_snippet) and any(
        token in normalize_space(article.discovery_published_hint).lower()
        for token in ("today", "hour ago", "hours ago", "minute ago", "minutes ago", "just now")
    ):
        return True
    if (has_market_context(title_snippet) or _strong_market_signal(title_snippet)) and title_snippet_score >= env_int("DISCOVERY_SNIPPET_TOPIC_SCORE_THRESHOLD", 4):
        return True
    if article.scope in {"Copper", "Aluminum", "Copper, Aluminum"} and combined_score >= 3:
        return True
    if (has_market_context(combined_text) or _strong_market_signal(combined_text)) and combined_score >= 3:
        return True
    if has_primary_target_metal(title_snippet) and any(term in title_snippet.lower() for term in SHORTLIST_PRIORITY_TERMS):
        return title_snippet_score >= env_int("DISCOVERY_SNIPPET_TOPIC_SCORE_THRESHOLD", 4)
    return False


def content_type_matches(article: Article) -> bool:
    text = f"{article.title}\n{article.body_text}\n{article.discovery_snippet}".lower()
    if not has_primary_target_metal(text):
        return False
    if has_market_context(text):
        return True
    if article.extraction_method.startswith("pdf:"):
        return True
    return len(article.body_text or "") >= env_int("CONTENT_TYPE_LONGFORM_RESCUE_BODY_CHARS", 700) and fast_relevance_score(article.title, article.body_text) >= env_int("CONTENT_TYPE_LONGFORM_RESCUE_SCORE", 5)


def scrape_source(
    source: SourceConfig,
    fetcher: HttpFetcher,
    max_candidates: int = 80,
    max_articles: int = 8,
    target_date: Optional[str] = None,
) -> list[Article]:
    candidates = discover_candidates(source, fetcher)[:max_candidates]
    LOG.info("Discovered %s candidates for %s", len(candidates), source.name)

    # Stage 1: metadata-first shortlist
    shortlisted: list[Candidate] = []
    for cand in candidates:
        score = cand.score
        if target_date and cand.published_hint:
            published = parse_isoish_date(cand.published_hint)
            if published and published[:10] == target_date:
                score += 4
        if score >= 2:
            shortlisted.append(cand)
        if len(shortlisted) >= max_candidates // 2:
            break

    articles: list[Article] = []
    seen: set[str] = set()
    for cand in shortlisted:
        if len(articles) >= max_articles:
            break
        if cand.url in seen:
            continue
        seen.add(cand.url)

        article = extract_article(source, cand, fetcher)
        if article is None:
            LOG.warning("Discarded article | stage=extracted | url=%s | reject_reasons=%s | score=%s | title=%s", cand.url, ["extract_none"], 0, cand.title_hint)
            continue
        if not topic_matches(article):
            LOG.warning("Discarded article | stage=topic_check | url=%s | reject_reasons=%s | score=%s | title=%s", cand.url, ["weak_topic"], article.score, article.title)
            continue
        if not content_type_matches(article):
            LOG.warning("Discarded article | stage=content_check | url=%s | reject_reasons=%s | score=%s | title=%s", cand.url, ["weak_content_type"], article.score, article.title)
            continue
        if not is_recent_enough(article, target_date=target_date, allow_missing_date=False):
            LOG.warning("Discarded article | stage=date_check | url=%s | reject_reasons=%s | score=%s | title=%s", cand.url, ["date_mismatch"], article.score, article.title)
            continue
        articles.append(article)

    return articles


def scrape_sources(
    sources: list[SourceConfig],
    target_date: Optional[str],
    max_articles_total: int = 20,
    per_source_limit: int = 8,
    user_agent: str = "Mozilla/5.0 (compatible; AnalystBriefBot/1.0; +https://example.com/bot)",
) -> list[Article]:
    fetcher = HttpFetcher(
        user_agent=user_agent,
        connect_timeout=10.0,
        read_timeout=20.0,
        per_host_delay=1.5,
        enable_http2=True,
        enable_browser_fallback=True,
    )
    try:
        all_articles: list[Article] = []
        for source in sources:
            try:
                articles = scrape_source(
                    source=source,
                    fetcher=fetcher,
                    max_candidates=80,
                    max_articles=per_source_limit,
                    target_date=target_date,
                )
                all_articles.extend(articles)
                if len(all_articles) >= max_articles_total:
                    break
            except Exception as exc:
                LOG.exception("Source failed | source=%s | error=%s", source.name, exc)
                continue
        deduped: dict[str, Article] = {}
        for article in all_articles:
            key = normalize_url(article.source_url)
            prev = deduped.get(key)
            if prev is None or article.score > prev.score:
                deduped[key] = article
        return sorted(deduped.values(), key=lambda a: (a.published_at, a.score), reverse=True)[:max_articles_total]
    finally:
        fetcher.close()


# ---------------------------------------------------------------------------
# Integration notes
# ---------------------------------------------------------------------------

def to_sharepoint_fields(article: Article) -> dict[str, Any]:
    """
    Minimal mapping; keep your existing LLM summary + SharePoint creation logic.
    """
    return {
        "Title": article.title[:250],
        "SourceName": article.source_name,
        "SourceUrl": article.source_url,
        "PublishedAt": article.published_at,
        "ArticleAuthor": article.author[:255],
        "Metal": article.scope,
        "RawExcerpt": article.body_text[:12000],
        "Warnings": "\n".join(article.warnings)[:4000],
        "FetchMethod": article.fetch_method,
        "ExtractionMethod": article.extraction_method,
    }


def load_sources_from_json(path: str | Path) -> list[SourceConfig]:
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    return [SourceConfig(**row) for row in rows]

# ---------------------------------------------------------------------------
# Dynamic extensions merged from scrap_logic.py
# ---------------------------------------------------------------------------

import json
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from bs4.element import Tag
LOG = logging.getLogger("analyst.scrape.v10")
BaseHttpFetcher = HttpFetcher
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


def opt_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else value.strip()


def _scrape_provider() -> str:
    value = opt_env("SCRAPE_PROVIDER", "auto").strip().lower()
    return value if value in {"auto", "local", "firecrawl"} else "auto"


def _firecrawl_search_fallback_enabled() -> bool:
    return env_bool("FIRECRAWL_USE_SEARCH_FALLBACK", False)


def _build_firecrawl_client() -> FirecrawlClient | None:
    if FirecrawlClient is None:
        return None
    api_key = opt_env("FIRECRAWL_API_KEY", "")
    if not api_key:
        return None
    try:
        return FirecrawlClient(
            api_key=api_key,
            timeout=env_int("FIRECRAWL_SCRAPE_TIMEOUT", 45),
            max_retries=env_int("FIRECRAWL_MAX_RETRIES", 3),
        )
    except Exception as exc:
        LOG.warning("Could not initialize Firecrawl client: %s", exc)
        return None


def _parse_name_list(raw: str) -> set[str]:
    return {x.strip().lower() for x in re.split(r"[\n,;]+", raw or "") if x.strip()}


def _deep_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [normalize_space(str(x)) for x in value if normalize_space(str(x))]
    if isinstance(value, str):
        return [normalize_space(x) for x in re.split(r"[\n,;]+", value) if normalize_space(x)]
    return []


def _debug_enabled_for_source(source_name: str) -> bool:
    if not env_bool("SCRAPE_DEBUG_ENABLED", False):
        return False
    allowed = _parse_name_list(opt_env("SCRAPE_DEBUG_SOURCES", ""))
    return not allowed or source_name.strip().lower() in allowed


def _debug_log_path() -> str:
    debug_dir = opt_env("SCRAPE_DEBUG_DIR", "")
    if not debug_dir:
        return ""
    os.makedirs(debug_dir, exist_ok=True)
    run_id = opt_env("SCRAPE_RUN_ID", "")
    suffix = run_id or date.today().isoformat()
    return os.path.join(debug_dir, f"scrape_{suffix}.jsonl")


def _debug_event(stage: str, source: SourceConfig | None = None, **payload: Any) -> None:
    source_name = source.name if source is not None else normalize_space(str(payload.get("source") or ""))
    if not source_name or not _debug_enabled_for_source(source_name):
        return

    record = {
        "ts": utc_now().isoformat(),
        "stage": stage,
        "source": source_name,
    }
    for key, value in payload.items():
        record[key] = value

    path = _debug_log_path()
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:  # pragma: no cover
        LOG.warning("Could not write scrape debug event | stage=%s | error=%s", stage, exc)


def _has_allowed_host(source: SourceConfig, url: str) -> bool:
    host = host_of(url)
    if not host:
        return False
    if not source.allowed_hosts:
        return True
    return any(host == allowed or host.endswith("." + allowed) for allowed in source.allowed_hosts)


def _normalize_listing_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return ""
    keep_keys = {"page", "p", "pg", "offset", "start", "skip", "cursor", "articlepage"}
    kept_query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() in keep_keys]
    parsed = parsed._replace(fragment="", query=urlencode(kept_query))
    return urlunparse(parsed).rstrip("/")


def _soft_accept_candidate_url(source: SourceConfig, url: str) -> bool:
    normalized = normalize_url(url)
    keywords = source_discovery_keywords(source)
    if not normalized or not _has_allowed_host(source, normalized):
        return False
    if any(hint in normalized.lower() for hint in source.exclude_hints):
        return False
    if source_accepts_url(source, normalized):
        return True
    low = normalized.lower()
    if looks_like_pdf(normalized):
        if not getattr(source, "pdf_report_ok", True):
            return False
        report_terms = list(getattr(source, "report_terms", []) or [])
        return looks_like_report_url(normalized) or any(term in low for term in report_terms)
    soft_terms = _dedupe_terms(
        list(source.include_hints or [])
        + list(source.category_terms or [])
        + list(getattr(source, "report_terms", []) or [])
        + list(keywords)
    )
    if any(term in low for term in soft_terms):
        return True
    return candidate_score("", normalized, keywords) >= env_int("DISCOVERY_SOFT_URL_SCORE_THRESHOLD", 1)


def _candidate_title_from_context(anchor: Tag, container: Tag | None) -> str:
    heading = container.find(["h1", "h2", "h3", "h4", "h5"]) if container is not None else None
    return first_nonempty(
        anchor.get("data-title"),
        anchor.get("title"),
        anchor.get("aria-label"),
        heading.get("aria-label") if heading else "",
        heading.get_text(" ", strip=True) if heading else "",
        anchor.get_text(" ", strip=True),
    )


def _candidate_snippet_from_context(anchor: Tag, container: Tag | None) -> str:
    parts: list[str] = []
    for value in (
        anchor.get("data-description"),
        anchor.get("data-summary"),
        anchor.get("aria-description"),
    ):
        if normalize_space(value):
            parts.append(str(value))

    if container is not None:
        for selector in (
            ".summary",
            ".excerpt",
            ".description",
            ".dek",
            ".subhead",
            ".lead",
            ".standfirst",
            "[data-testid*='summary']",
            "[data-testid*='excerpt']",
            "[data-testid*='description']",
        ):
            try:
                tag = container.select_one(selector)
            except Exception:
                tag = None
            if tag is not None:
                text = normalize_space(tag.get_text(" ", strip=True))
                if text:
                    parts.append(text)

        paragraph_text = collect_text(container.find_all(["p", "li"]), min_len=18)
        if paragraph_text:
            parts.append(paragraph_text)

        if not parts:
            container_text = truncate_text(container.get_text(" ", strip=True), max_chars=360)
            if container_text:
                parts.append(container_text)

    return truncate_text(merge_text_blocks(*parts), max_chars=360)


def _published_from_node(node: Tag | None) -> str:
    if node is None:
        return ""
    for time_tag in node.find_all("time"):
        published = first_nonempty(
            time_tag.get("datetime"),
            time_tag.get("content"),
            time_tag.get("title"),
            time_tag.get_text(" ", strip=True),
        )
        if published:
            return published
    for tag in node.find_all(attrs={"datetime": True}):
        published = first_nonempty(tag.get("datetime"), tag.get("content"), tag.get_text(" ", strip=True))
        if published:
            return published
    for selector in (
        "meta[property='article:published_time']",
        "meta[name='article:published_time']",
        "meta[property='og:published_time']",
        "meta[name='publish-date']",
        "meta[name='date']",
        "meta[name='dc.date']",
        "meta[itemprop='dateModified']",
        "meta[itemprop='datePublished']",
        "relative-time",
        "abbr[data-utime]",
        "[data-testid*='date']",
        "[class*='publish']",
        "[class*='updated']",
        ".date",
        ".published",
        ".timestamp",
    ):
        try:
            tag = node.select_one(selector)
        except Exception:
            tag = None
        if tag is None:
            continue
        published = first_nonempty(
            tag.get("content"),
            tag.get("datetime"),
            tag.get("data-date"),
            tag.get("data-datetime"),
            tag.get("title"),
            tag.get("aria-label"),
            tag.get_text(" ", strip=True),
        )
        if published:
            return published
    return ""


def _merge_candidate(found: dict[str, Candidate], candidate: Candidate) -> None:
    previous = found.get(candidate.url)
    if previous is None:
        found[candidate.url] = candidate
        return

    merged = previous
    if candidate.score > previous.score:
        merged = candidate
    merged.title_hint = previous.title_hint if len(previous.title_hint) >= len(candidate.title_hint) else candidate.title_hint
    merged.published_hint = previous.published_hint if len(previous.published_hint) >= len(candidate.published_hint) else candidate.published_hint
    merged.snippet_hint = previous.snippet_hint if len(previous.snippet_hint) >= len(candidate.snippet_hint) else candidate.snippet_hint
    found[candidate.url] = merged


def _selectors_with_text(selectors: list[str]) -> list[str]:
    return [selector for selector in selectors if normalize_space(selector)]


@dataclass(slots=True)
class DynamicProfile:
    render_listing_pages: bool = False
    render_article_pages: bool = False
    listing_wait_selectors: list[str] = field(default_factory=list)
    article_wait_selectors: list[str] = field(default_factory=list)
    dismiss_selectors: list[str] = field(default_factory=list)
    load_more_selectors: list[str] = field(default_factory=list)
    listing_card_selectors: list[str] = field(default_factory=list)
    article_body_selectors: list[str] = field(default_factory=list)
    section_link_hints: list[str] = field(default_factory=list)
    next_page_selectors: list[str] = field(default_factory=list)
    extra_wait_ms: int = 1200
    scroll_passes: int = 2
    max_listing_pages: int = 4
    second_hop_limit: int = 4
    max_pagination_pages: int = 2
    max_load_more_clicks: int = 2
    render_timeout_ms: int = 25000


def _load_dynamic_profile_overrides() -> dict[str, dict[str, Any]]:
    raw = opt_env("DYNAMIC_SOURCE_PROFILES_JSON", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        LOG.warning("Could not parse DYNAMIC_SOURCE_PROFILES_JSON")
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(key).strip().lower(): value for key, value in data.items() if isinstance(value, dict)}

def _profile_defaults_for_source(source: SourceConfig) -> DynamicProfile:
    name = (source.name or "").lower()
    profile = DynamicProfile(
        render_listing_pages=bool(source.js_heavy),
        render_article_pages=bool(source.js_heavy),
        listing_wait_selectors=["main", "article", "[role=main]", "a[href]"],
        article_wait_selectors=["article", "main", "[role=main]", "h1"],
        dismiss_selectors=[
            "button:has-text('Accept')",
            "button:has-text('I agree')",
            "button:has-text('Agree')",
            "button:has-text('Allow all')",
            "[aria-label*='Accept']",
            "[id*='accept']",
            "[class*='accept']",
        ],
        load_more_selectors=[
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "button:has-text('More')",
            "a:has-text('Load more')",
            "a:has-text('More news')",
        ],
        listing_card_selectors=[
            "article",
            "[data-testid*='article']",
            "[data-testid*='story']",
            ".article-card",
            ".story-card",
            ".news-card",
            ".listing-item",
            ".card",
            "li",
        ],
        article_body_selectors=[
            "article",
            "main",
            "[role=main]",
            ".article-body",
            ".article-content",
            ".story-content",
            ".news-content",
            ".entry-content",
            ".rich-text",
        ],
        section_link_hints=[
            "news",
            "analysis",
            "insights",
            "commentary",
            "markets",
            "commodities",
            "metals",
            "base-metals",
            "non-ferrous",
            "copper",
            "aluminium",
            "aluminum",
            "research",
            "reports",
        ],
        next_page_selectors=[
            "a[rel='next']",
            "a[aria-label*='Next']",
            "a:has-text('Next')",
            "a:has-text('Older')",
            "a:has-text('More')",
        ],
        extra_wait_ms=env_int("DYNAMIC_RENDER_EXTRA_WAIT_MS", 2200 if source.js_heavy else 1400),
        scroll_passes=env_int("DYNAMIC_RENDER_SCROLL_PASSES", 4 if source.js_heavy else 3),
        max_listing_pages=env_int("DYNAMIC_MAX_LISTING_PAGES", 4),
        second_hop_limit=env_int("DYNAMIC_SECOND_HOP_LIMIT", 4),
        max_pagination_pages=env_int("DYNAMIC_MAX_PAGINATION_PAGES", 2),
        max_load_more_clicks=env_int("DYNAMIC_MAX_LOAD_MORE_CLICKS", 2),
        render_timeout_ms=env_int("DYNAMIC_RENDER_TIMEOUT_MS", 35000),
    )

    if "reuters" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/markets/']", "a[href*='/world/']", "article a"]
        profile.listing_card_selectors = ["article", "[data-testid*='story']", "[data-testid*='MediaStoryCard']", ".story-card", "li"]
        profile.article_wait_selectors = ["main article", "article", "main", "h1"]
        profile.article_body_selectors = ["main article", "article", "[data-testid*='article-body']", "main", ".article-body"]
        profile.next_page_selectors = ["a[rel='next']", "a:has-text('Next')", "a[href*='page=']"]
    elif "capital.com" in name or name == "capital.com":
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/analysis/commodities-news']", "a[href*='/analysis/']", "article a"]
        profile.listing_card_selectors = ["article", ".article-card", ".news-card", ".card", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", "main", ".article-content", ".post-content", ".rich-text"]
    elif "argus" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='latest-market-news']", "a[href*='/news-and-insights/']", "article a"]
        profile.listing_card_selectors = ["article", ".news-card", ".card", ".listing-item", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".article-content", ".content", "main"]
    elif "fitch" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/bmi/']", "a[href*='/insights/']", "article a"]
        profile.listing_card_selectors = ["article", ".card", ".insight-card", ".listing-item", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".article-content", ".entry-content", "main"]
    elif "ainvest" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/news/articles-latest/']", "a[href*='/news/']", "article a"]
        profile.listing_card_selectors = ["article", ".news-card", ".card", "[data-testid*='article']", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".article-content", ".rich-text", "main"]
        profile.scroll_passes = max(profile.scroll_passes, 4)
        profile.max_load_more_clicks = max(profile.max_load_more_clicks, 3)
    elif "discovery" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/articles/']", "article a"]
        profile.listing_card_selectors = ["article", ".article-card", ".card", ".listing-item", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".entry-content", ".article-content", "main"]
    elif "metal.com" in name or "smm" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='news']", "article a"]
        profile.listing_card_selectors = ["article", ".news-item", ".news-card", ".card", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".article-content", ".news-content", ".content", "main"]
        profile.scroll_passes = max(profile.scroll_passes, 4)
        profile.max_load_more_clicks = max(profile.max_load_more_clicks, 3)
    elif "lme" in name:
        profile.render_listing_pages = True
        profile.render_article_pages = True
        profile.listing_wait_selectors = ["main", "a[href*='/news']", "a[href*='/metals/non-ferrous']", "article a"]
        profile.listing_card_selectors = ["article", ".card", ".news-card", ".listing-item", "li"]
        profile.article_wait_selectors = ["main", "article", "h1"]
        profile.article_body_selectors = ["article", ".article-content", ".rich-text", "main"]
        profile.section_link_hints.extend(["stocks", "warehousing", "physical-markets", "market-data"])

    overrides = _load_dynamic_profile_overrides().get((source.name or "").strip().lower(), {})
    if overrides:
        profile.render_listing_pages = bool(overrides.get("render_listing_pages", profile.render_listing_pages))
        profile.render_article_pages = bool(overrides.get("render_article_pages", profile.render_article_pages))
        profile.listing_wait_selectors = _deep_list(overrides.get("listing_wait_selectors", profile.listing_wait_selectors))
        profile.article_wait_selectors = _deep_list(overrides.get("article_wait_selectors", profile.article_wait_selectors))
        profile.dismiss_selectors = _deep_list(overrides.get("dismiss_selectors", profile.dismiss_selectors))
        profile.load_more_selectors = _deep_list(overrides.get("load_more_selectors", profile.load_more_selectors))
        profile.listing_card_selectors = _deep_list(overrides.get("listing_card_selectors", profile.listing_card_selectors))
        profile.article_body_selectors = _deep_list(overrides.get("article_body_selectors", profile.article_body_selectors))
        profile.section_link_hints = _deep_list(overrides.get("section_link_hints", profile.section_link_hints))
        profile.next_page_selectors = _deep_list(overrides.get("next_page_selectors", profile.next_page_selectors))
        profile.extra_wait_ms = int(overrides.get("extra_wait_ms", profile.extra_wait_ms))
        profile.scroll_passes = int(overrides.get("scroll_passes", profile.scroll_passes))
        profile.max_listing_pages = int(overrides.get("max_listing_pages", profile.max_listing_pages))
        profile.second_hop_limit = int(overrides.get("second_hop_limit", profile.second_hop_limit))
        profile.max_pagination_pages = int(overrides.get("max_pagination_pages", profile.max_pagination_pages))
        profile.max_load_more_clicks = int(overrides.get("max_load_more_clicks", profile.max_load_more_clicks))
        profile.render_timeout_ms = int(overrides.get("render_timeout_ms", profile.render_timeout_ms))

    profile.listing_wait_selectors = _selectors_with_text(profile.listing_wait_selectors)
    profile.article_wait_selectors = _selectors_with_text(profile.article_wait_selectors)
    profile.dismiss_selectors = _selectors_with_text(profile.dismiss_selectors)
    profile.load_more_selectors = _selectors_with_text(profile.load_more_selectors)
    profile.listing_card_selectors = _selectors_with_text(profile.listing_card_selectors)
    profile.article_body_selectors = _selectors_with_text(profile.article_body_selectors)
    profile.section_link_hints = _selectors_with_text(profile.section_link_hints)
    profile.next_page_selectors = _selectors_with_text(profile.next_page_selectors)
    return profile


class DynamicRenderer:
    def __init__(self, user_agent: str) -> None:
        self.user_agent = user_agent
        self.enabled = bool(sync_playwright is not None) and env_bool("ENABLE_DYNAMIC_RENDERING", True)
        self.owner_thread_id = get_ident()
        self.owner_thread_name = current_thread().name
        self._pw = None
        self._browser = None
        self._context = None

    def start(self) -> None:
        if not self.enabled or self._browser is not None:
            return
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=env_bool("DYNAMIC_RENDER_HEADLESS", True))
        self._context = self._browser.new_context(user_agent=self.user_agent, viewport={"width": 1440, "height": 2200})

    def close(self) -> None:
        try:
            if self._context is not None:
                self._context.close()
        finally:
            self._context = None
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            self._browser = None
        try:
            if self._pw is not None:
                self._pw.stop()
        finally:
            self._pw = None

    def _click_first(self, page: Any, selector: str, timeout_ms: int) -> bool:
        try:
            page.locator(selector).first.click(timeout=timeout_ms)
            return True
        except Exception:
            return False

    def _wait_for_any(self, page: Any, wait_selectors: list[str], timeout_ms: int) -> bool:
        if not wait_selectors:
            return True
        for selector in wait_selectors:
            try:
                page.wait_for_selector(selector, timeout=timeout_ms, state="attached")
                return True
            except Exception:
                continue
        return False

    def render_html(
        self,
        url: str,
        *,
        source_name: str = "",
        wait_selectors: list[str] | None = None,
        dismiss_selectors: list[str] | None = None,
        load_more_selectors: list[str] | None = None,
        extra_wait_ms: int = 1200,
        scroll_passes: int = 2,
        max_load_more_clicks: int = 2,
        timeout_ms: int = 25000,
    ) -> tuple[str, str, list[str]]:
        warnings: list[str] = []
        if not self.enabled:
            return "", url, ["dynamic_renderer_disabled"]
        current_tid = get_ident()
        if self.owner_thread_id != current_tid:
            warnings.append("dynamic_thread_affinity_protection")
            LOG.warning(
                "Dynamic renderer skipped due to thread-affinity protection | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | action=skip_affinity",
                current_tid,
                self.owner_thread_id,
                source_name,
                url,
            )
            return "", url, warnings
        self.start()
        page = self._context.new_page()
        try:
            page.route(
                "**/*",
                lambda route: route.abort() if route.request.resource_type in {"image", "media", "font"} else route.continue_(),
            )
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(400)

            for selector in dismiss_selectors or []:
                if self._click_first(page, selector, timeout_ms=1500):
                    page.wait_for_timeout(250)

            ready = self._wait_for_any(page, wait_selectors or [], timeout_ms=3500)
            if not ready and wait_selectors:
                warnings.append("dynamic_wait_selector_timeout")

            total_clicks = 0
            last_height = 0
            for _ in range(max(0, scroll_passes)):
                try:
                    height = int(page.evaluate("() => document.body ? document.body.scrollHeight : 0") or 0)
                except Exception:
                    height = 0
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                page.wait_for_timeout(500)
                for selector in load_more_selectors or []:
                    while total_clicks < max(0, max_load_more_clicks) and self._click_first(page, selector, timeout_ms=1800):
                        total_clicks += 1
                        page.wait_for_timeout(700)
                if height and height == last_height:
                    page.wait_for_timeout(200)
                last_height = height

            if extra_wait_ms > 0:
                page.wait_for_timeout(extra_wait_ms)
            return page.content(), page.url, warnings
        except Exception as exc:
            message = normalize_space(str(exc))
            if _is_dynamic_thread_affinity_error(exc):
                warnings.append("dynamic_thread_affinity_error")
                LOG.warning(
                    "Dynamic render thread error | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | error=%s",
                    current_tid,
                    self.owner_thread_id,
                    source_name,
                    url,
                    message or type(exc).__name__,
                )
            else:
                warnings.append(f"dynamic_render_error:{type(exc).__name__}")
                LOG.warning(
                    "Dynamic render failed | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | error=%s",
                    current_tid,
                    self.owner_thread_id,
                    source_name,
                    url,
                    message or type(exc).__name__,
                )
            return "", url, warnings
        finally:
            try:
                page.close()
            except Exception:
                pass


_DYNAMIC_RENDERER_LOCAL = local()
_DYNAMIC_RENDERERS: dict[int, DynamicRenderer] = {}
_DYNAMIC_RENDERERS_LOCK = Lock()


def _is_dynamic_thread_affinity_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return "cannot switch to a different thread" in text or "switch to a different thread" in text or "greenlet" in text


def _log_dynamic_renderer_event(
    *,
    action: str,
    source_name: str = "",
    url: str = "",
    renderer: Optional[DynamicRenderer] = None,
    note: str = "",
) -> None:
    thread_id = get_ident()
    owner_thread_id = getattr(renderer, "owner_thread_id", thread_id) if renderer is not None else thread_id
    LOG.info(
        "Dynamic renderer | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | action=%s%s",
        thread_id,
        owner_thread_id,
        source_name,
        url,
        action,
        f" | note={note}" if note else "",
    )


def _get_dynamic_renderer(fetcher: BaseHttpFetcher, source_name: str = "", url: str = "") -> DynamicRenderer:
    current_tid = get_ident()
    renderer = getattr(_DYNAMIC_RENDERER_LOCAL, "renderer", None)
    if renderer is not None and getattr(renderer, "owner_thread_id", current_tid) != current_tid:
        _log_dynamic_renderer_event(
            action="skip_affinity",
            source_name=source_name,
            url=url,
            renderer=renderer,
            note="thread_local_owner_mismatch",
        )
        try:
            delattr(_DYNAMIC_RENDERER_LOCAL, "renderer")
        except Exception:
            pass
        renderer = None
    if renderer is None:
        renderer = DynamicRenderer(user_agent=fetcher.user_agent)
        _DYNAMIC_RENDERER_LOCAL.renderer = renderer
        with _DYNAMIC_RENDERERS_LOCK:
            _DYNAMIC_RENDERERS[current_tid] = renderer
        _log_dynamic_renderer_event(action="start", source_name=source_name, url=url, renderer=renderer)
    else:
        _log_dynamic_renderer_event(action="use", source_name=source_name, url=url, renderer=renderer)
    return renderer


def _close_dynamic_renderer(source_name: str = "", url: str = "", reason: str = "") -> None:
    current_tid = get_ident()
    renderer = getattr(_DYNAMIC_RENDERER_LOCAL, "renderer", None)
    if renderer is None:
        return
    owner_thread_id = getattr(renderer, "owner_thread_id", current_tid)
    if owner_thread_id != current_tid:
        _log_dynamic_renderer_event(
            action="skip_affinity_close",
            source_name=source_name,
            url=url,
            renderer=renderer,
            note=reason or "close_from_non_owner_thread",
        )
        try:
            delattr(_DYNAMIC_RENDERER_LOCAL, "renderer")
        except Exception:
            pass
        return
    try:
        renderer.close()
    except Exception as exc:
        LOG.warning(
            "Dynamic renderer close failed | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | error=%s",
            current_tid,
            owner_thread_id,
            source_name,
            url,
            exc,
        )
    finally:
        try:
            delattr(_DYNAMIC_RENDERER_LOCAL, "renderer")
        except Exception:
            pass
        with _DYNAMIC_RENDERERS_LOCK:
            if _DYNAMIC_RENDERERS.get(current_tid) is renderer:
                _DYNAMIC_RENDERERS.pop(current_tid, None)
        _log_dynamic_renderer_event(action="close", source_name=source_name, url=url, renderer=renderer, note=reason)


def _close_all_dynamic_renderers() -> None:
    current_tid = get_ident()
    with _DYNAMIC_RENDERERS_LOCK:
        owned_thread_ids = list(_DYNAMIC_RENDERERS.keys())
    for owner_thread_id in owned_thread_ids:
        if owner_thread_id != current_tid:
            LOG.info(
                "Dynamic renderer skipped due to thread-affinity protection | thread_id=%s | owner_thread_id=%s | source=%s | url=%s | action=skip_close_foreign",
                current_tid,
                owner_thread_id,
                "",
                "",
            )
            continue
        _close_dynamic_renderer(reason="close_all_current_thread")


def _dynamic_discovery_enabled(source: SourceConfig) -> bool:
    # Defensive switches for unstable Playwright/threading environments:
    # ENABLE_DYNAMIC_DISCOVERY=false
    # ENABLE_DYNAMIC_ARTICLE_RENDER=false
    if not env_bool("ENABLE_DYNAMIC_DISCOVERY", True):
        return False
    return _profile_defaults_for_source(source).render_listing_pages


def _dynamic_article_enabled(source: SourceConfig) -> bool:
    if not env_bool("ENABLE_DYNAMIC_ARTICLE_RENDER", True):
        return False
    return _profile_defaults_for_source(source).render_article_pages


def _extract_card_candidates(source: SourceConfig, page_url: str, html: str, profile: DynamicProfile) -> list[Candidate]:
    soup = soupify(html)
    found: dict[str, Candidate] = {}
    keywords = source_discovery_keywords(source, profile)

    for cand in extract_candidates_with_scrapy(source, page_url, html):
        cand.method = "dynamic_scrapy"
        cand.score += 1
        _merge_candidate(found, cand)

    containers: list[Tag] = []
    for selector in profile.listing_card_selectors:
        try:
            containers.extend(soup.select(selector))
        except Exception:
            continue

    for container in containers:
        if not isinstance(container, Tag):
            continue
        anchors = container.find_all("a", href=True)
        if not anchors:
            continue
        best_url = ""
        best_title = ""
        best_published = _published_from_node(container)
        best_snippet = ""
        best_score = -999
        for anchor in anchors:
            href = normalize_url(urljoin(page_url, anchor.get("href")))
            if not _soft_accept_candidate_url(source, href):
                continue
            title = _candidate_title_from_context(anchor, container)
            snippet = _candidate_snippet_from_context(anchor, container)
            score = candidate_score(title, href, keywords, context_text=snippet)
            if looks_like_pdf(href) or looks_like_report_url(href):
                score += 2
            if len(title) >= 14:
                score += 1
            if score > best_score:
                best_url = href
                best_title = title
                best_snippet = snippet
                best_score = score
        if not best_url:
            continue
        candidate = Candidate(
            source.name,
            best_url,
            page_url,
            title_hint=best_title,
            published_hint=best_published,
            snippet_hint=best_snippet,
            method="dynamic_card",
        )
        candidate.score = best_score + 2
        _merge_candidate(found, candidate)

    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(page_url, anchor.get("href")))
        if not _soft_accept_candidate_url(source, href):
            continue
        container = anchor.find_parent(["article", "li", "div", "section"])
        title = _candidate_title_from_context(anchor, container)
        snippet = _candidate_snippet_from_context(anchor, container)
        published = _published_from_node(container)
        candidate = Candidate(
            source.name,
            href,
            page_url,
            title_hint=title,
            published_hint=published,
            snippet_hint=snippet,
            method="dynamic_listing",
        )
        candidate.score = candidate_score(title, href, keywords, context_text=snippet) + (2 if (looks_like_pdf(href) or looks_like_report_url(href)) else 1)
        if len(title) >= 14:
            candidate.score += 1
        _merge_candidate(found, candidate)

    return list(found.values())


def _extract_section_pages(source: SourceConfig, page_url: str, html: str, profile: DynamicProfile) -> list[str]:
    soup = soupify(html)
    found: list[str] = []
    tokens = source_discovery_keywords(source, profile)
    for anchor in soup.find_all("a", href=True):
        href = _normalize_listing_url(urljoin(page_url, anchor.get("href")))
        if not href or not _has_allowed_host(source, href) or looks_like_pdf(href):
            continue
        if _soft_accept_candidate_url(source, href):
            continue
        text = normalize_space(anchor.get_text(" ", strip=True)).lower()
        low_href = href.lower()
        if any(token in text or token in low_href for token in tokens):
            found.append(href)
    deduped: list[str] = []
    seen: set[str] = set()
    for href in found:
        if href in seen:
            continue
        seen.add(href)
        deduped.append(href)
    return deduped


def _extract_pagination_links(source: SourceConfig, page_url: str, html: str, profile: DynamicProfile) -> list[str]:
    soup = soupify(html)
    found: list[str] = []

    def add_href(value: str) -> None:
        href = _normalize_listing_url(urljoin(page_url, value))
        if not href or href == _normalize_listing_url(page_url):
            return
        if not _has_allowed_host(source, href) or looks_like_pdf(href):
            return
        if _soft_accept_candidate_url(source, href):
            return
        found.append(href)

    for selector in profile.next_page_selectors:
        try:
            for tag in soup.select(selector):
                href = tag.get("href")
                if href:
                    add_href(href)
        except Exception:
            continue

    for anchor in soup.find_all("a", href=True):
        text = normalize_space(anchor.get_text(" ", strip=True)).lower()
        href = anchor.get("href") or ""
        if any(token in text for token in ("next", "older", "previous", "more")):
            add_href(href)
            continue
        if re.search(r"([?&](page|p|pg|offset|start)=\d+)|(/page/\d+)", href.lower()):
            add_href(href)

    deduped: list[str] = []
    seen: set[str] = set()
    for href in found:
        if href in seen:
            continue
        seen.add(href)
        deduped.append(href)
    return deduped


class HttpFetcher(BaseHttpFetcher):
    pass

def _discover_candidates_dynamic(source: SourceConfig, fetcher: HttpFetcher) -> list[Candidate]:
    if not _dynamic_discovery_enabled(source):
        return []

    profile = _profile_defaults_for_source(source)
    found: dict[str, Candidate] = {}
    seen_pages: set[str] = set()

    queue: deque[tuple[str, int, str]] = deque()
    for seed in prioritized_seed_urls(source):
        normalized = _normalize_listing_url(seed)
        if normalized:
            queue.append((normalized, 0, "seed"))

    section_slots = profile.second_hop_limit
    pagination_slots = profile.max_pagination_pages

    while queue and len(seen_pages) < max(1, profile.max_listing_pages):
        page_url, depth, via = queue.popleft()
        normalized_page_url = _normalize_listing_url(page_url)
        if not normalized_page_url or normalized_page_url in seen_pages:
            continue
        seen_pages.add(normalized_page_url)

        try:
            renderer = _get_dynamic_renderer(fetcher, source_name=source.name, url=page_url)
            html, final_url, warnings = renderer.render_html(
                page_url,
                source_name=source.name,
                wait_selectors=profile.listing_wait_selectors,
                dismiss_selectors=profile.dismiss_selectors,
                load_more_selectors=profile.load_more_selectors,
                extra_wait_ms=profile.extra_wait_ms,
                scroll_passes=profile.scroll_passes,
                max_load_more_clicks=profile.max_load_more_clicks,
                timeout_ms=profile.render_timeout_ms,
            )
        except Exception as exc:
            html, final_url, warnings = "", page_url, [f"dynamic_render_exception:{type(exc).__name__}"]
            LOG.warning(
                "Dynamic discovery fallback to non-dynamic fetch | thread_id=%s | source=%s | url=%s | error=%s",
                get_ident(),
                source.name,
                page_url,
                exc,
            )
        if not html:
            if any(
                warning.startswith("dynamic_thread_affinity")
                or warning.startswith("dynamic_render_error")
                or warning.startswith("dynamic_render_exception")
                for warning in warnings
            ):
                LOG.info(
                    "Dynamic discovery skipped and falling back to non-dynamic behavior | thread_id=%s | source=%s | url=%s | warnings=%s",
                    get_ident(),
                    source.name,
                    page_url,
                    warnings,
                )
                _close_dynamic_renderer(source_name=source.name, url=page_url, reason="dynamic_discovery_failure")
            _debug_event("dynamic_listing_failed", source, page_url=page_url, via=via, warnings=warnings)
            continue

        before = len(found)
        for candidate in _extract_card_candidates(source, final_url or page_url, html, profile):
            if depth > 0:
                candidate.method = "dynamic_second_hop"
                candidate.score += 1
            _merge_candidate(found, candidate)

        queued_sections = 0
        if depth == 0 and section_slots > 0:
            for section_url in _extract_section_pages(source, final_url or page_url, html, profile):
                if section_slots <= 0:
                    break
                if section_url in seen_pages:
                    continue
                queue.append((section_url, depth + 1, "section"))
                section_slots -= 1
                queued_sections += 1

        queued_pages = 0
        if pagination_slots > 0:
            for next_page in _extract_pagination_links(source, final_url or page_url, html, profile):
                if pagination_slots <= 0:
                    break
                if next_page in seen_pages:
                    continue
                queue.append((next_page, depth, "pagination"))
                pagination_slots -= 1
                queued_pages += 1

        _debug_event(
            "dynamic_listing_page",
            source,
            page_url=page_url,
            final_url=final_url,
            via=via,
            depth=depth,
            added_candidates=len(found) - before,
            queued_sections=queued_sections,
            queued_pages=queued_pages,
            warnings=warnings,
        )

    return sorted(found.values(), key=lambda item: item.score, reverse=True)


def _firecrawl_candidate_score(
    source: SourceConfig,
    url: str,
    title_hint: str,
    snippet_hint: str,
    published_hint: str,
    target_date: Optional[str],
    method: str,
) -> int:
    text = f"{title_hint}\n{snippet_hint}\n{url}"
    score = candidate_score(title_hint or url, text, source_discovery_keywords(source))
    score += fast_relevance_score(title_hint or url, snippet_hint)
    if has_required_target_metal(text):
        score += 3
    score += sum(2 for term in SHORTLIST_PRIORITY_TERMS if term in text.lower())
    if has_market_context(text):
        score += 2
    if looks_like_article(url):
        score += 2
    if looks_like_report_url(url):
        score += 1
    if _looks_like_section_page(url):
        score -= 8
    effective_target_date = _effective_target_date(target_date)
    if published_hint:
        parsed = parse_isoish_date(published_hint)
        if parsed and parsed[:10] == effective_target_date:
            score += 4
        elif _date_hint_matches_target(published_hint, effective_target_date):
            score += 3
    if _same_day_url_match(url, effective_target_date):
        score += 6
    if _is_obviously_old_archive_url(url, effective_target_date):
        score -= 20
    if method == "firecrawl_search":
        score -= 1
    return score


def _firecrawl_candidate_from_payload(
    source: SourceConfig,
    url: str,
    listing_url: str,
    title_hint: str,
    published_hint: str,
    snippet_hint: str,
    method: str,
    target_date: Optional[str],
) -> Candidate | None:
    normalized_url = normalize_url(url)
    effective_target_date = _effective_target_date(target_date)
    if not normalized_url or not _has_allowed_host(source, normalized_url):
        return None
    if _is_obviously_old_archive_url(normalized_url, effective_target_date):
        return None
    if any(hint in normalized_url.lower() for hint in source.exclude_hints):
        return None
    is_pdf = looks_like_pdf(normalized_url)
    if not is_pdf and not looks_like_article(normalized_url):
        return None
    if is_pdf and not getattr(source, "pdf_report_ok", True):
        return None
    topical_text = f"{title_hint}\n{snippet_hint}\n{normalized_url}"
    topical_low = topical_text.lower()
    has_metal_evidence = has_required_target_metal(topical_text)
    has_priority_evidence = any(term in topical_low for term in SHORTLIST_PRIORITY_TERMS)
    has_date_evidence = _date_hint_matches_target(published_hint, effective_target_date) or _same_day_url_match(normalized_url, effective_target_date)
    if not (has_metal_evidence or has_priority_evidence or has_date_evidence or source_accepts_url(source, normalized_url) or _soft_accept_candidate_url(source, normalized_url)):
        return None
    return Candidate(
        source_name=source.name,
        url=normalized_url,
        listing_url=listing_url,
        title_hint=normalize_space(title_hint),
        published_hint=normalize_space(published_hint),
        snippet_hint=normalize_space(snippet_hint),
        method=method,
        score=_firecrawl_candidate_score(
            source,
            normalized_url,
            title_hint,
            snippet_hint,
            published_hint,
            target_date,
            method,
        ),
    )


def _firecrawl_search_queries(source: SourceConfig, target_date: Optional[str]) -> list[str]:
    hosts = [host for host in (source.allowed_hosts or []) if host]
    if not hosts:
        hosts = [host_of(url) for url in source.seed_urls if host_of(url)]
    hosts = list(dict.fromkeys(hosts))[:2]
    date_hint = _effective_target_date(target_date)
    queries: list[str] = []
    for host in hosts:
        queries.append(f"site:{host} copper market analysis {date_hint}")
        queries.append(f"site:{host} aluminum market analysis {date_hint}")
        queries.append(f"site:{host} aluminium market analysis {date_hint}")
    return list(dict.fromkeys(queries))


def _discover_candidates_firecrawl(
    source: SourceConfig,
    firecrawl_client: FirecrawlClient,
    target_date: Optional[str] = None,
) -> list[Candidate]:
    merged: dict[str, Candidate] = {}
    map_limit = env_int("FIRECRAWL_MAP_LIMIT", 80)
    seed_urls = prioritized_seed_urls(source)[:3] or source.seed_urls[:3]
    if not seed_urls:
        return []
    per_seed_limit = max(10, map_limit // max(1, len(seed_urls)))
    for seed_url in seed_urls:
        try:
            mapped_rows = firecrawl_client.map_url(seed_url, include_subdomains=False, limit=per_seed_limit)
        except Exception as exc:
            LOG.warning("Firecrawl map failed | source=%s | url=%s | error=%s", source.name, seed_url, exc)
            _debug_event("firecrawl_map_failed", source, page_url=seed_url, error=str(exc))
            continue
        for row in mapped_rows:
            candidate = _firecrawl_candidate_from_payload(
                source=source,
                url=str(row.get("url") or ""),
                listing_url=seed_url,
                title_hint=str(row.get("title") or ""),
                published_hint=str(row.get("published") or ""),
                snippet_hint=str(row.get("snippet") or ""),
                method="firecrawl_map",
                target_date=target_date,
            )
            if candidate is not None:
                _merge_candidate(merged, candidate)

    firecrawl_map_candidate_count = len(merged)
    if firecrawl_map_candidate_count < 12 and _firecrawl_search_fallback_enabled():
        for query in _firecrawl_search_queries(source, target_date):
            try:
                search_rows = firecrawl_client.search_web(query, limit=env_int("FIRECRAWL_SEARCH_LIMIT", 5))
            except Exception as exc:
                LOG.warning("Firecrawl search failed | source=%s | query=%s | error=%s", source.name, query, exc)
                _debug_event("firecrawl_search_failed", source, query=query, error=str(exc))
                continue
            for row in search_rows:
                candidate = _firecrawl_candidate_from_payload(
                    source=source,
                    url=str(row.get("url") or ""),
                    listing_url=source.seed_urls[0] if source.seed_urls else "",
                    title_hint=str(row.get("title") or ""),
                    published_hint=str(row.get("published") or ""),
                    snippet_hint=str(row.get("snippet") or ""),
                    method="firecrawl_search",
                    target_date=target_date,
                )
                if candidate is not None:
                    _merge_candidate(merged, candidate)

    ordered = sorted(merged.values(), key=lambda item: item.score, reverse=True)
    _debug_event(
        "firecrawl_discovery_done",
        source,
        count=len(ordered),
        firecrawl_map_candidate_count=firecrawl_map_candidate_count,
        provider="firecrawl",
    )
    return ordered


def discover_candidates(
    source: SourceConfig,
    fetcher: HttpFetcher,
    target_date: Optional[str] = None,
    firecrawl_client: FirecrawlClient | None = None,
) -> list[Candidate]:
    merged: dict[str, Candidate] = {}
    provider = _scrape_provider()
    firecrawl_candidates: list[Candidate] = []

    if provider in {"auto", "firecrawl"} and firecrawl_client is not None:
        try:
            firecrawl_candidates = _discover_candidates_firecrawl(source, firecrawl_client, target_date=target_date)
            for candidate in firecrawl_candidates:
                _merge_candidate(merged, candidate)
        except Exception as exc:
            LOG.warning("Firecrawl discovery failed | source=%s | error=%s", source.name, exc)
            _debug_event("firecrawl_discovery_error", source, error=str(exc))

    firecrawl_map_count = len([candidate for candidate in firecrawl_candidates if candidate.method == "firecrawl_map"])
    min_firecrawl_candidates = 12 if provider in {"auto", "firecrawl"} else max(2, min(4, env_int("MAX_SHORTLIST_PER_SOURCE", 8)))
    need_local_fallback = (
        provider == "local"
        or firecrawl_client is None
        or not firecrawl_candidates
        or (provider == "auto" and firecrawl_map_count < min_firecrawl_candidates)
        or (provider == "firecrawl" and firecrawl_map_count < min_firecrawl_candidates)
    )
    base_fallback_candidates: list[Candidate] = []
    dynamic_discovery_used = False
    if need_local_fallback:
        base_fallback_candidates = _discover_candidates_base(source, fetcher, target_date=target_date)
        for candidate in base_fallback_candidates:
            _merge_candidate(merged, candidate)
        dynamic_discovery_threshold = env_int("DYNAMIC_DISCOVERY_BASE_CANDIDATE_THRESHOLD", min_firecrawl_candidates)
        should_try_dynamic_discovery = (
            _dynamic_discovery_enabled(source)
            and bool(source.js_heavy)
            and len(base_fallback_candidates) < dynamic_discovery_threshold
        )
        if should_try_dynamic_discovery:
            dynamic_discovery_used = True
            for candidate in _discover_candidates_dynamic(source, fetcher):
                _merge_candidate(merged, candidate)
        else:
            LOG.info(
                "Dynamic discovery skipped | thread_id=%s | source=%s | base_candidates=%s | threshold=%s | js_heavy=%s",
                get_ident(),
                source.name,
                len(base_fallback_candidates),
                dynamic_discovery_threshold,
                source.js_heavy,
            )

    ordered = sorted(merged.values(), key=lambda item: item.score, reverse=True)
    methods: dict[str, int] = {}
    for candidate in ordered:
        methods[candidate.method] = methods.get(candidate.method, 0) + 1
    LOG.info("Discovered %s candidates for %s | methods=%s", len(ordered), source.name, methods)
    _debug_event(
        "discover_candidates_done",
        source,
        count=len(ordered),
        methods=methods,
        provider=provider,
        firecrawl_map_count=firecrawl_map_count,
        used_local_fallback=need_local_fallback,
        base_fallback_count=len(base_fallback_candidates),
        dynamic_discovery_used=dynamic_discovery_used,
    )
    return ordered


def _text_from_selectors(soup: BeautifulSoup, selectors: list[str], min_chars: int) -> tuple[str, str]:
    for selector in selectors:
        try:
            nodes = soup.select(f"{selector} p, {selector} li")
        except Exception:
            nodes = []
        text = collect_text(nodes)
        if len(text) >= min_chars:
            return text, f"profile:{selector}"
        try:
            container = soup.select_one(selector)
        except Exception:
            container = None
        if container is None:
            continue
        text = collect_text(container.find_all(["p", "li"]))
        if len(text) >= min_chars:
            return text, f"profile:{selector}"
    return "", ""


def _extract_article_body(source: SourceConfig, profile: DynamicProfile, html: str, url: str) -> tuple[str, str]:
    body_text, extraction_method = extract_main_text(html, url, source)
    if len(body_text) >= source.min_body_chars:
        return body_text, extraction_method

    soup = soupify(html)
    profile_text, profile_method = _text_from_selectors(
        soup,
        profile.article_body_selectors,
        max(60, min(source.min_body_chars, env_int("ARTICLE_BODY_RESCUE_MIN_CHARS", 110))),
    )
    if len(profile_text) > len(body_text):
        body_text = profile_text
        extraction_method = profile_method or extraction_method

    jsonld = title_from_jsonld(soup)
    jsonld_body = normalize_space(jsonld.get("body"))
    if len(jsonld_body) > len(body_text):
        body_text = jsonld_body
        extraction_method = "jsonld_body"

    lead_text = ""
    for selector in (
        "article h2 + p",
        "main h1 + p",
        ".article-content p",
        ".entry-content p",
        ".story-content p",
        ".news-content p",
    ):
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        lead_text = collect_text(nodes[:3], min_len=20)
        if lead_text:
            break

    if len(body_text) < max(60, source.min_body_chars // 2):
        fallback = collect_text(soup.find_all("p"), min_len=35)
        merged = merge_text_blocks(body_text, profile_text, jsonld_body, lead_text, fallback, max_chars=9000)
        if len(merged) > len(body_text):
            body_text = merged
            extraction_method = extraction_method or "paragraphs_rescue"

    return body_text, extraction_method


def _extract_article_from_html(
    source: SourceConfig,
    candidate: Candidate,
    html: str,
    final_url: str,
    method: str,
    warnings: list[str] | None = None,
) -> Optional[Article]:
    if not html:
        return None

    profile = _profile_defaults_for_source(source)
    soup = soupify(html)
    jsonld = title_from_jsonld(soup)

    title = first_nonempty(
        jsonld.get("title"),
        meta_content(soup, "og:title", "twitter:title", "headline"),
        soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "",
        candidate.title_hint,
        soup.title.get_text(" ", strip=True) if soup.title else "",
    )
    author = first_nonempty(
        jsonld.get("author"),
        meta_content(soup, "author", "article:author", "parsely-author", "twitter:creator"),
    )
    published = first_nonempty(
        jsonld.get("published"),
        meta_content(soup, "article:published_time", "og:published_time", "publish-date", "parsely-pub-date", "date"),
        _published_from_node(soup.find("article") or soup.find("main") or soup),
        candidate.published_hint,
        find_publish_date(html, final_url),
        date_from_url(final_url),
    )

    body_text, extraction_method = _extract_article_body(source, profile, html, final_url)
    meta_desc = meta_content(soup, "description", "og:description", "twitter:description")
    enriched_body = merge_text_blocks(
        body_text,
        jsonld.get("body"),
        meta_desc,
        candidate.snippet_hint,
        max_chars=9000,
    )
    if len(enriched_body) > len(body_text):
        body_text = enriched_body
        extraction_method = extraction_method + "+context_rescue" if extraction_method else "context_rescue"

    if len(body_text) < max(60, source.min_body_chars // 2):
        if source.paywalled_preview_ok and (title or meta_desc):
            body_text = merge_text_blocks(body_text, meta_desc, jsonld.get("body"), candidate.snippet_hint, title, max_chars=2500)
            extraction_method = extraction_method + "+metadata_only" if extraction_method else "metadata_only"

    if not title:
        return None

    article = Article(
        source_name=source.name,
        source_url=normalize_url(jsonld.get("url") or final_url),
        title=title,
        author=author,
        published_at=parse_isoish_date(published),
        body_text=body_text,
        scope=classify_scope(title, body_text),
        fetch_method=method,
        extraction_method=extraction_method or "empty",
        discovery_published_hint=candidate.published_hint,
        discovery_snippet=candidate.snippet_hint,
        warnings=list(warnings or []),
    )
    article.score = fast_relevance_score(article.title, article.body_text)
    return article


@dataclass(slots=True)
class VerificationDecision:
    has_target_metal: bool = False
    verified_metals: list[str] = field(default_factory=list)
    has_market_context: bool = False
    has_valid_same_day_date: bool = False
    published_at: str = ""
    same_day_hint: bool = False
    date_sources_checked: list[str] = field(default_factory=list)
    reject_reasons: list[str] = field(default_factory=list)
    score: int = 0
    confidence: str = "low"
    page_type: str = "unknown"
    narrative_chars: int = 0
    narrative_paragraphs: int = 0
    table_density: float = 0.0
    quote_signal: str = "low"


@dataclass(slots=True)
class ExtractedPage:
    article: Optional[Article]
    raw_html: str = ""
    final_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    verification: Optional[VerificationDecision] = None


def _page_visible_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    soup = soupify(raw_html)
    text = collect_text(soup.find_all(["h1", "h2", "h3", "p", "li", "time", "span"]), min_len=2)
    if text:
        return truncate_text(text, max_chars=12000)
    return truncate_text(soup.get_text(" ", strip=True), max_chars=12000)


def _jsonld_date_values(soup: BeautifulSoup) -> list[str]:
    values: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        stack: list[Any] = [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            if "@graph" in item:
                stack.append(item["@graph"])
            for key in ("datePublished", "dateModified"):
                value = normalize_space(item.get(key))
                if value:
                    values.append(value)
    return values


def _visible_date_values(soup: BeautifulSoup) -> list[str]:
    values: list[str] = []
    selectors = (
        "header time",
        "article time",
        "main time",
        "time",
        "relative-time",
        "abbr[data-utime]",
        "[data-testid*='date']",
        "[class*='publish']",
        "[class*='updated']",
        "[class*='date']",
        "[itemprop='datePublished']",
        "[itemprop='dateModified']",
    )
    for selector in selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        for node in nodes[:12]:
            value = first_nonempty(
                node.get("datetime"),
                node.get("content"),
                node.get("data-date"),
                node.get("data-datetime"),
                node.get("title"),
                node.get("aria-label"),
                node.get_text(" ", strip=True),
            )
            if value:
                values.append(value)
    return values


def _narrative_metrics(raw_html: str) -> tuple[int, int, float]:
    if not raw_html:
        return 0, 0, 0.0
    soup = soupify(raw_html)
    paragraphs: list[str] = []
    for node in soup.find_all(["p", "li"]):
        if node.find_parent(["table", "thead", "tbody", "tfoot"]):
            continue
        text = normalize_space(node.get_text(" ", strip=True))
        if len(text) < 40:
            continue
        if re.fullmatch(r"[\d\s,.\-+/%:$]+", text):
            continue
        paragraphs.append(text)
    narrative_chars = sum(len(item) for item in paragraphs)
    table_nodes = soup.find_all(["table", "tr", "td", "th"])
    visible_text = truncate_text(soup.get_text(" ", strip=True), max_chars=20000)
    visible_chars = max(1, len(visible_text))
    table_text = " ".join(normalize_space(node.get_text(" ", strip=True)) for node in table_nodes[:80])
    table_density = min(1.0, (len(table_text) / visible_chars))
    return narrative_chars, len(paragraphs), round(table_density, 3)


def _quote_signal(raw_html: str, article: Optional[Article], url: str) -> tuple[str, list[str]]:
    reasons: list[str] = []
    title_text = normalize_space(article.title if article else "").lower()
    text = merge_text_blocks(
        article.title if article else "",
        article.body_text if article else "",
        raw_html,
        url,
        max_chars=20000,
    ).lower()
    score = 0
    low_url = normalize_url(url).lower()
    hard_terms = (
        "/prices/",
        "/price/",
        "/future/",
        "/futures/",
        "/quotes/",
        "/quote/",
        "/spot/",
        "/symbol/",
        "/ticker/",
        "/lme/",
        "/shfe/",
        "/comex/",
    )
    if any(term in low_url for term in hard_terms):
        score += 4
        reasons.append("price_url_pattern")
    if re.search(r"\bprice of .+ today\b|\bfutures prices\b|\bper ton today\b|\b3m\b|\bcash\b", text):
        score += 3
        reasons.append("price_title_pattern")
    if re.search(r"\bprice of .+ today\b|\bfutures prices\b|\bper ton today\b|\blmeselect\b|\bshfe\b|\bcomex\b", title_text):
        score += 3
        reasons.append("exchange_title_pattern")
    if any(token in text for token in ("lme", "shfe", "comex")) and any(token in text for token in ("3m", "cash", "settlement", "open interest", "contract")):
        score += 3
        reasons.append("contract_quote_terms")
    if len(re.findall(r"\b[a-z]{1,3}\d{1,4}\b", text)) >= 3:
        score += 2
        reasons.append("contract_codes")
    if len(re.findall(r"\b(?:cash|3m|bid|ask|open|high|low|volume|change)\b", text)) >= 6:
        score += 2
        reasons.append("ticker_table_terms")
    if score >= 6:
        return "high", reasons
    if score >= 3:
        return "medium", reasons
    return "low", reasons


def _classify_page_profile(article: Optional[Article], raw_html: str, url: str) -> tuple[str, int, int, float, str, list[str]]:
    return _classify_page_profile_for_role(article, raw_html, url, "")


def _source_role_value(source: Optional[SourceConfig]) -> str:
    role = normalize_space(getattr(source, "source_role", "") or "").lower()
    if role in {"narrative_news", "market_data", "market_research"}:
        return role
    return "narrative_news"


def _narrative_page_types() -> set[str]:
    return {"analyst_article", "market_news", "report"}


def _market_data_summary_allowed(decision: VerificationDecision) -> bool:
    min_chars = env_int("MARKET_DATA_NARRATIVE_MIN_CHARS", max(env_int("NARRATIVE_MIN_CHARS", 450), 650))
    min_paragraphs = env_int("MARKET_DATA_NARRATIVE_MIN_PARAGRAPHS", max(env_int("NARRATIVE_MIN_PARAGRAPHS", 3), 4))
    return (
        decision.page_type in _narrative_page_types()
        and decision.narrative_chars >= min_chars
        and decision.narrative_paragraphs >= min_paragraphs
        and decision.quote_signal == "low"
        and decision.table_density <= env_float("MARKET_DATA_MAX_TABLE_DENSITY", 0.24)
    )


def _market_research_summary_allowed(decision: VerificationDecision, reportish: bool = False) -> bool:
    min_chars = env_int("MARKET_RESEARCH_NARRATIVE_MIN_CHARS", max(env_int("NARRATIVE_MIN_CHARS", 450), 520))
    return (
        (decision.page_type in _narrative_page_types() or reportish)
        and decision.narrative_chars >= min_chars
        and decision.narrative_paragraphs >= env_int("NARRATIVE_MIN_PARAGRAPHS", 3)
    )


def _classify_page_profile_for_role(
    article: Optional[Article],
    raw_html: str,
    url: str,
    source_role: str = "",
) -> tuple[str, int, int, float, str, list[str]]:
    low_url = normalize_url(url).lower()
    narrative_chars, narrative_paragraphs, table_density = _narrative_metrics(raw_html)
    quote_signal, quote_reasons = _quote_signal(raw_html, article, url)
    title_text = normalize_space(article.title if article else "").lower()
    combined_text = merge_text_blocks(
        article.title if article else "",
        article.body_text if article else "",
        article.discovery_snippet if article else "",
        raw_html,
        max_chars=20000,
    ).lower()
    narrative_min_chars = env_int("NARRATIVE_MIN_CHARS", 450)
    narrative_min_paragraphs = env_int("NARRATIVE_MIN_PARAGRAPHS", 3)
    quote_table_threshold = env_float("QUOTE_TABLE_DENSITY_THRESHOLD", 0.32)
    structural_quote = (
        narrative_chars < narrative_min_chars
        or narrative_paragraphs < narrative_min_paragraphs
        or table_density >= quote_table_threshold
    )
    contract_labels = len(re.findall(r"\b[a-z]{1,3}\d{1,4}\b", combined_text))
    repeated_exchange_labels = len(re.findall(r"\b(?:lme|lmeselect|shfe|comex|cash|3m|bid|ask|open|high|low|volume|change|settlement|contract|warehouse|symbol|ticker)\b", combined_text))
    data_terms = (
        "price",
        "prices",
        "market data",
        "inventory",
        "stocks",
        "warehouse",
        "premium",
        "spread",
        "settlement",
        "open interest",
        "futures",
        "cash",
    )
    if "/archive/" in low_url or "/archives/" in low_url:
        return "archive_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if any(token in low_url for token in ("/category/", "/tag/", "/tags/", "/author/", "/authors/")) or _looks_like_section_page(url):
        return "category_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if "/symbol/" in low_url or ("symbol" in title_text and structural_quote and repeated_exchange_labels >= 6):
        return "symbol_landing_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if "/ticker/" in low_url or (structural_quote and repeated_exchange_labels >= 10 and any(term in low_url for term in ("/quote/", "/quotes/", "/price/", "/prices/"))):
        return "ticker_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if any(term in low_url for term in ("/future/", "/futures/")) and (quote_signal != "low" or structural_quote or contract_labels >= 3 or repeated_exchange_labels >= 8):
        return "futures_table_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if any(term in low_url for term in ("/prices/", "/price/", "/quote/", "/quotes/", "/spot/")) and (quote_signal != "low" or structural_quote):
        return "quote_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if quote_signal == "high" and (structural_quote or contract_labels >= 3 or repeated_exchange_labels >= 8):
        return "quote_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if looks_like_report_url(url) or any(term in combined_text for term in ("report", "research", "outlook", "forecast")):
        return "report", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if any(term in combined_text for term in ("analysis", "commentary", "insight", "review")) and narrative_chars >= narrative_min_chars:
        return "analyst_article", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if has_market_context(combined_text) and narrative_chars >= narrative_min_chars:
        return "market_news", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    if (
        source_role == "market_data"
        or any(term in combined_text for term in data_terms)
    ) and (
        quote_signal != "low"
        or table_density >= env_float("MARKET_DATA_TABLE_DENSITY_THRESHOLD", 0.18)
        or repeated_exchange_labels >= 6
        or narrative_chars < narrative_min_chars
    ):
        return "market_data_page", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons
    return "unknown", narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons


def classify_page_type(article: Optional[Article], raw_html: str, url: str) -> str:
    page_type, _, _, _, _, _ = _classify_page_profile_for_role(article, raw_html, url, "")
    return page_type


def _hard_excluded_page_types() -> set[str]:
    return {
        "category_page",
        "archive_page",
        "market_data_page",
        "quote_page",
        "ticker_page",
        "futures_table_page",
        "symbol_landing_page",
    }


def _page_type_override_allowed(decision: VerificationDecision) -> bool:
    if decision.page_type not in _hard_excluded_page_types():
        return False
    narrative_override_chars = env_int(
        "PAGE_TYPE_OVERRIDE_MIN_CHARS",
        max(env_int("NARRATIVE_MIN_CHARS", 450), 700),
    )
    narrative_override_paragraphs = env_int(
        "PAGE_TYPE_OVERRIDE_MIN_PARAGRAPHS",
        max(env_int("NARRATIVE_MIN_PARAGRAPHS", 3), 4),
    )
    max_table_density = env_float(
        "PAGE_TYPE_OVERRIDE_MAX_TABLE_DENSITY",
        min(env_float("QUOTE_TABLE_DENSITY_THRESHOLD", 0.32), 0.28),
    )
    return (
        decision.has_target_metal
        and decision.has_market_context
        and decision.has_valid_same_day_date
        and decision.narrative_chars >= narrative_override_chars
        and decision.narrative_paragraphs >= narrative_override_paragraphs
        and decision.table_density <= max_table_density
    )


def _verification_structure_accepted(decision: VerificationDecision) -> bool:
    if decision.narrative_chars < env_int("NARRATIVE_MIN_CHARS", 450):
        return False
    if decision.narrative_paragraphs < env_int("NARRATIVE_MIN_PARAGRAPHS", 3):
        return False
    if decision.page_type in _hard_excluded_page_types() and not _page_type_override_allowed(decision):
        return False
    if decision.quote_signal == "high" and not _page_type_override_allowed(decision):
        return False
    return True


def _candidate_same_day_hint(candidate: Candidate, target_date: Optional[str]) -> bool:
    effective_target_date = _effective_target_date(target_date)
    if not effective_target_date:
        return False
    if _date_hint_matches_target(candidate.published_hint, effective_target_date):
        return True
    return _same_day_url_match(candidate.url, effective_target_date)


def _candidate_relative_same_day_hint(candidate: Candidate) -> bool:
    low = normalize_space(candidate.published_hint).lower()
    return any(token in low for token in ("today", "just now", "hour ago", "hours ago", "minute ago", "minutes ago"))


def _fast_candidate_prefilter(
    source: SourceConfig,
    candidate: Candidate,
    target_date: Optional[str],
) -> tuple[bool, int, list[str]]:
    source_role = _source_role_value(source)
    topical_text = merge_text_blocks(candidate.title_hint, candidate.snippet_hint, candidate.url, max_chars=2500)
    topical_low = topical_text.lower()
    reasons: list[str] = []
    score = int(candidate.score)

    metal_signal = has_required_target_metal(topical_text)
    market_signal = has_market_context(topical_text) or any(term in topical_low for term in SHORTLIST_PRIORITY_TERMS)
    date_signal = _candidate_same_day_hint(candidate, target_date)
    relative_same_day_signal = _candidate_relative_same_day_hint(candidate)
    report_signal = looks_like_report_url(candidate.url) or any(term in topical_low for term in getattr(source, "report_terms", []))
    old_archive = _is_obviously_old_archive_url(candidate.url, _effective_target_date(target_date))
    section_like = _looks_like_section_page(candidate.url)

    if old_archive:
        return False, score - 20, ["old_archive_url"]
    if section_like and not date_signal and not relative_same_day_signal:
        return False, score - 10, ["section_like_url"]

    if metal_signal:
        score += 5
        reasons.append("metal_signal")
    if market_signal:
        score += 3
        reasons.append("market_signal")
    if date_signal:
        score += 5
        reasons.append("date_signal")
    elif relative_same_day_signal:
        score += 4
        reasons.append("relative_same_day_signal")
    if candidate.method.startswith("firecrawl"):
        score += 2
        reasons.append("firecrawl_candidate")
    if report_signal:
        score += 2
        reasons.append("report_signal")

    accepted = False
    if source_role == "narrative_news":
        accepted = (metal_signal and (date_signal or relative_same_day_signal or market_signal)) or (date_signal and market_signal)
    elif source_role == "market_data":
        accepted = metal_signal or date_signal or relative_same_day_signal
    elif source_role == "market_research":
        accepted = metal_signal and (report_signal or market_signal or date_signal)

    if not accepted and candidate.method.startswith("firecrawl") and score >= env_int("FAST_PREFILTER_FIRECRAWL_RESCUE_SCORE", 7):
        accepted = True
        reasons.append("firecrawl_rescue")

    if not accepted:
        reject_reasons: list[str] = []
        if not metal_signal:
            reject_reasons.append("missing_metal_signal")
        if not date_signal and not relative_same_day_signal:
            reject_reasons.append("missing_same_day_hint")
        if not market_signal and source_role != "market_data":
            reject_reasons.append("missing_market_hint")
        return False, score, reject_reasons
    return True, score, reasons


def _resolve_page_date_evidence(
    raw_html: str,
    candidate: Candidate,
    target_date: Optional[str],
    final_url: str = "",
    extracted_metadata: Optional[dict[str, Any]] = None,
) -> tuple[str, bool, list[str]]:
    effective_target_date = _effective_target_date(target_date)
    sources_checked: list[str] = []
    fallback_published = ""

    def _check_value(label: str, value: str) -> tuple[bool, str, bool]:
        nonlocal fallback_published
        cleaned = normalize_space(value)
        if not cleaned:
            return False, "", False
        sources_checked.append(label)
        parsed = parse_isoish_date(cleaned)
        if parsed and not fallback_published:
            fallback_published = parsed
        if parsed and parsed[:10] == effective_target_date:
            return True, parsed, False
        if _date_hint_matches_target(cleaned, effective_target_date):
            return True, parsed or "", True
        return False, parsed or "", False

    soup = soupify(raw_html) if raw_html else None
    if soup is not None:
        for value in _jsonld_date_values(soup):
            matched, published_at, same_day_hint = _check_value("jsonld", value)
            if matched:
                return published_at, same_day_hint, sources_checked
        for meta_name in (
            "article:published_time",
            "og:published_time",
            "publish-date",
            "parsely-pub-date",
            "last-modified",
            "article:modified_time",
            "date",
            "dc.date",
        ):
            matched, published_at, same_day_hint = _check_value(f"meta:{meta_name}", meta_content(soup, meta_name))
            if matched:
                return published_at, same_day_hint, sources_checked
        for value in _visible_date_values(soup):
            matched, published_at, same_day_hint = _check_value("visible_date_selector", value)
            if matched:
                return published_at, same_day_hint, sources_checked
        matched, published_at, same_day_hint = _check_value("node_time_tags", _published_from_node(soup.find("article") or soup.find("main") or soup))
        if matched:
            return published_at, same_day_hint, sources_checked

    metadata = extracted_metadata or {}
    for key in ("published", "publishedTime", "datePublished", "dateModified", "lastModified", "modified"):
        value = metadata.get(key)
        if isinstance(value, str):
            matched, published_at, same_day_hint = _check_value(f"metadata:{key}", value)
            if matched:
                return published_at, same_day_hint, sources_checked

    matched, published_at, same_day_hint = _check_value("candidate_published_hint", candidate.published_hint)
    if matched:
        return published_at, same_day_hint, sources_checked

    matched, published_at, same_day_hint = _check_value("final_url_date", date_from_url(final_url or candidate.url))
    if matched:
        return published_at, same_day_hint, sources_checked

    visible_text = _page_visible_text(raw_html)
    if _date_hint_matches_target(visible_text, effective_target_date):
        sources_checked.append("visible_relative_text")
        return fallback_published, True, sources_checked

    return fallback_published, False, sources_checked


def verify_page_relevance_and_date(
    article: Optional[Article],
    raw_html: str,
    candidate: Candidate,
    target_date: Optional[str],
    final_url: str = "",
    extracted_metadata: Optional[dict[str, Any]] = None,
    source: Optional[SourceConfig] = None,
) -> VerificationDecision:
    article = article or Article(
        source_name=candidate.source_name,
        source_url=normalize_url(final_url or candidate.url),
        title=candidate.title_hint,
        author="",
        published_at="",
        body_text="",
        scope="Unknown",
        fetch_method=candidate.method,
        extraction_method="empty",
        discovery_published_hint=candidate.published_hint,
        discovery_snippet=candidate.snippet_hint,
        warnings=[],
    )
    visible_text = _page_visible_text(raw_html)
    metadata_text = ""
    if extracted_metadata:
        metadata_text = merge_text_blocks(*(str(value) for value in extracted_metadata.values() if isinstance(value, str)), max_chars=4000)
    combined_text = merge_text_blocks(
        article.title,
        article.body_text,
        article.discovery_snippet,
        metadata_text,
        visible_text,
        candidate.title_hint,
        candidate.snippet_hint,
        candidate.url,
        max_chars=16000,
    )
    verified_metals = normalize_metal_mentions(combined_text)
    published_at, same_day_hint, date_sources_checked = _resolve_page_date_evidence(
        raw_html=raw_html,
        candidate=candidate,
        target_date=target_date,
        final_url=final_url or article.source_url,
        extracted_metadata=extracted_metadata,
    )
    has_valid_same_day_date = bool(published_at and published_at[:10] == _effective_target_date(target_date)) or same_day_hint
    decision = VerificationDecision(
        has_target_metal=bool(verified_metals),
        verified_metals=verified_metals,
        has_market_context=has_market_context(combined_text),
        has_valid_same_day_date=has_valid_same_day_date,
        published_at=published_at,
        same_day_hint=same_day_hint,
        date_sources_checked=date_sources_checked,
    )
    page_type, narrative_chars, narrative_paragraphs, table_density, quote_signal, quote_reasons = _classify_page_profile_for_role(
        article,
        raw_html,
        final_url or article.source_url,
        _source_role_value(source),
    )
    decision.page_type = page_type
    decision.narrative_chars = narrative_chars
    decision.narrative_paragraphs = narrative_paragraphs
    decision.table_density = table_density
    decision.quote_signal = quote_signal
    if decision.has_target_metal:
        decision.score += 4
    else:
        decision.reject_reasons.append("missing_target_metal_after_page_check")
    if decision.has_market_context:
        decision.score += 3
    else:
        decision.reject_reasons.append("missing_market_context_after_page_check")
    if decision.has_valid_same_day_date:
        decision.score += 4
    elif published_at:
        decision.reject_reasons.append("date_mismatch_after_full_page_check")
    else:
        decision.reject_reasons.append("missing_date_after_full_page_check")
    narrative_min_chars = env_int("NARRATIVE_MIN_CHARS", 450)
    narrative_min_paragraphs = env_int("NARRATIVE_MIN_PARAGRAPHS", 3)
    page_type_override = _page_type_override_allowed(decision)
    if page_type in _hard_excluded_page_types() and not page_type_override:
        decision.reject_reasons.append(f"page_type_{page_type}")
    if narrative_chars < narrative_min_chars or narrative_paragraphs < narrative_min_paragraphs:
        decision.reject_reasons.append("insufficient_narrative_body")
    if quote_signal == "high" and not page_type_override:
        decision.reject_reasons.append("quote_signal_high")
    if page_type_override:
        decision.score += 1
    if quote_reasons:
        decision.reject_reasons.extend([f"quote_reason:{reason}" for reason in quote_reasons[:5]])
    if decision.score >= 10:
        decision.confidence = "high"
    elif decision.score >= 6:
        decision.confidence = "medium"
    decision.reject_reasons = list(dict.fromkeys(decision.reject_reasons))
    return decision


def _should_try_dynamic_article(
    source: SourceConfig,
    article: Optional[Article],
    result_method: str,
    target_date: Optional[str] = None,
    verification: VerificationDecision | None = None,
) -> bool:
    if not _dynamic_article_enabled(source):
        return False
    if article is None:
        return True
    has_title = bool(normalize_space(article.title))
    has_usable_body = len(normalize_space(article.body_text)) >= max(60, source.min_body_chars // 2)
    has_valid_date = verification.has_valid_same_day_date if verification is not None else _article_same_day_signal(article, target_date)
    has_metal_signal = verification.has_target_metal if verification is not None else has_required_target_metal(f"{article.title}\n{article.discovery_snippet}\n{article.body_text}")
    if has_title and has_usable_body and has_valid_date and has_metal_signal:
        return False
    return not has_title or not has_usable_body or not has_valid_date


def _render_article_html(source: SourceConfig, candidate: Candidate, fetcher: HttpFetcher) -> tuple[str, str, list[str]]:
    profile = _profile_defaults_for_source(source)
    try:
        renderer = _get_dynamic_renderer(fetcher, source_name=source.name, url=candidate.url)
        html, final_url, warnings = renderer.render_html(
            candidate.url,
            source_name=source.name,
            wait_selectors=profile.article_wait_selectors,
            dismiss_selectors=profile.dismiss_selectors,
            load_more_selectors=[],
            extra_wait_ms=profile.extra_wait_ms,
            scroll_passes=profile.scroll_passes,
            max_load_more_clicks=0,
            timeout_ms=profile.render_timeout_ms,
        )
    except Exception as exc:
        LOG.warning(
            "Dynamic article fallback to non-dynamic fetch | thread_id=%s | source=%s | url=%s | error=%s",
            get_ident(),
            source.name,
            candidate.url,
            exc,
        )
        return "", candidate.url, [f"dynamic_render_exception:{type(exc).__name__}"]
    if not html and any(
        warning.startswith("dynamic_thread_affinity")
        or warning.startswith("dynamic_render_error")
        or warning.startswith("dynamic_render_exception")
        for warning in warnings
    ):
        LOG.info(
            "Dynamic article skipped and falling back to base extraction | thread_id=%s | source=%s | url=%s | warnings=%s",
            get_ident(),
            source.name,
            candidate.url,
            warnings,
        )
        _close_dynamic_renderer(source_name=source.name, url=candidate.url, reason="dynamic_article_failure")
    return html, final_url, warnings


def _article_rank_tuple(article: Article) -> tuple[int, int, int, int, int]:
    extraction = article.extraction_method or ""
    return (
        fast_relevance_score(article.title, article.body_text),
        len(article.body_text or ""),
        1 if article.published_at else 0,
        1 if article.scope != "Unknown" else 0,
        0 if "metadata_only" in extraction else 1,
    )


def _choose_better_article(base_article: Optional[Article], dynamic_article: Optional[Article]) -> Optional[Article]:
    if dynamic_article is None:
        return base_article
    if base_article is None:
        return dynamic_article
    if _article_rank_tuple(dynamic_article) >= _article_rank_tuple(base_article):
        dynamic_article.warnings.extend([f"replaced_baseline:{base_article.fetch_method}/{base_article.extraction_method}"])
        return dynamic_article
    return base_article


def _page_result_rank(result: ExtractedPage | None) -> tuple[int, int, int, int, int, int]:
    if result is None or result.article is None:
        return (0, 0, 0, 0, 0, 0)
    verification = result.verification or VerificationDecision()
    article_rank = _article_rank_tuple(result.article)
    return (
        verification.score,
        1 if verification.has_valid_same_day_date else 0,
        1 if verification.has_target_metal else 0,
        1 if verification.has_market_context else 0,
        article_rank[0],
        article_rank[1],
    )


def _choose_better_extracted_page(base_result: ExtractedPage | None, other_result: ExtractedPage | None) -> ExtractedPage | None:
    if other_result is None or other_result.article is None:
        return base_result
    if base_result is None or base_result.article is None:
        return other_result
    if _page_result_rank(other_result) >= _page_result_rank(base_result):
        other_result.article.warnings.extend(
            [f"replaced_baseline:{base_result.article.fetch_method}/{base_result.article.extraction_method}"]
        )
        return other_result
    return base_result


def _is_pdf_candidate(candidate: Candidate) -> bool:
    return looks_like_pdf(candidate.url)


def _pdf_last_resort_enabled() -> bool:
    return env_bool("PDF_LAST_RESORT_ONLY", True)


def _minimum_html_target(max_articles: int) -> int:
    return max(1, min(max_articles, env_int("PDF_FALLBACK_MIN_HTML_ARTICLES", 2)))


def _apply_verification_to_article(
    article: Optional[Article],
    verification: Optional[VerificationDecision],
    source: Optional[SourceConfig] = None,
) -> Optional[Article]:
    if article is None or verification is None:
        return article
    if verification.published_at and (not article.published_at or verification.has_valid_same_day_date):
        article.published_at = verification.published_at
    metals = set(verification.verified_metals or [])
    if {"copper", "aluminum"}.issubset(metals):
        article.scope = "Copper, Aluminum"
    elif "copper" in metals:
        article.scope = "Copper"
    elif "aluminum" in metals:
        article.scope = "Aluminum"
    article.warnings.append("verified_metals:" + ",".join(verification.verified_metals or []))
    article.warnings.append(f"verified_market_context:{verification.has_market_context}")
    article.warnings.append(f"verified_same_day_date:{verification.has_valid_same_day_date}")
    article.warnings.append(f"page_type:{verification.page_type}")
    article.warnings.append(f"narrative_chars:{verification.narrative_chars}")
    article.warnings.append(f"quote_signal:{verification.quote_signal}")
    article.warnings.append(f"source_role:{_source_role_value(source)}")
    if verification.date_sources_checked:
        article.warnings.append("date_sources_checked:" + ",".join(verification.date_sources_checked[:8]))
    return article


def _extract_article_firecrawl(
    source: SourceConfig,
    candidate: Candidate,
    firecrawl_client: FirecrawlClient,
) -> ExtractedPage:
    scraped = firecrawl_client.scrape_url(
        candidate.url,
        formats=["markdown"],
        only_main_content=True,
        timeout=env_int("FIRECRAWL_SCRAPE_TIMEOUT", 45),
    )
    markdown = normalize_space(scraped.get("markdown"))
    metadata = scraped.get("metadata") if isinstance(scraped.get("metadata"), dict) else {}
    raw_html = str(scraped.get("html") or "")
    title = first_nonempty(
        scraped.get("title"),
        metadata.get("title"),
        candidate.title_hint,
    )
    if not title:
        return ExtractedPage(article=None, raw_html=raw_html, final_url=normalize_url(scraped.get("url") or candidate.url), metadata=metadata)
    body_text = merge_text_blocks(
        markdown,
        metadata.get("description"),
        candidate.snippet_hint,
        title,
        max_chars=9000,
    )
    article = Article(
        source_name=source.name,
        source_url=normalize_url(scraped.get("url") or candidate.url),
        title=title,
        author=normalize_space(scraped.get("author") or metadata.get("author")),
        published_at=parse_isoish_date(
            first_nonempty(
                scraped.get("published"),
                metadata.get("publishedTime"),
                metadata.get("published"),
                candidate.published_hint,
                date_from_url(candidate.url),
            )
        ),
        body_text=body_text,
        scope=classify_scope(title, body_text),
        fetch_method=candidate.method or "firecrawl_map",
        extraction_method="firecrawl_scrape",
        discovery_published_hint=candidate.published_hint,
        discovery_snippet=candidate.snippet_hint,
        warnings=[],
    )
    article.score = fast_relevance_score(article.title, article.body_text)
    article.warnings.append("firecrawl_provider")
    if metadata.get("statusCode"):
        article.warnings.append(f"firecrawl_status:{metadata.get('statusCode')}")
    return ExtractedPage(
        article=article,
        raw_html=raw_html,
        final_url=article.source_url,
        metadata=metadata,
        verification=None,
    )


def _firecrawl_article_needs_local_fallback(
    source: SourceConfig,
    result: ExtractedPage | None,
    target_date: Optional[str],
) -> bool:
    article = result.article if result is not None else None
    verification = result.verification if result is not None else None
    if article is None:
        return True
    if verification is not None and verification.has_target_metal and verification.has_valid_same_day_date and verification.has_market_context:
        return False
    if len(article.body_text or "") < max(40, source.min_body_chars // 3) and not _strong_market_signal(f"{article.title}\n{article.discovery_snippet}"):
        return True
    if verification is not None and not verification.has_valid_same_day_date:
        return True
    if not article.published_at and not _date_hint_matches_target(article.discovery_published_hint, _effective_target_date(target_date)):
        return True
    if verification is not None and not verification.has_target_metal:
        return True
    if article.scope == "Unknown" and not _strong_market_signal(f"{article.title}\n{article.discovery_snippet}\n{article.body_text}"):
        return True
    return False


def _extract_article_local(
    source: SourceConfig,
    candidate: Candidate,
    fetcher: HttpFetcher,
    allow_pdf: bool = True,
    target_date: Optional[str] = None,
) -> ExtractedPage:
    if looks_like_pdf(candidate.url):
        if not allow_pdf:
            return ExtractedPage(article=None, final_url=normalize_url(candidate.url))
        article = extract_pdf_article(source, candidate, fetcher)
        verification = verify_page_relevance_and_date(article, "", candidate, target_date=target_date, final_url=candidate.url, source=source)
        article = _apply_verification_to_article(article, verification, source=source)
        return ExtractedPage(article=article, final_url=normalize_url(candidate.url), verification=verification)

    result = fetcher.fetch(candidate.url, allow_browser_fallback=True)
    if "pdf" in (result.content_type or "").lower():
        if not allow_pdf:
            return ExtractedPage(article=None, raw_html=result.text, final_url=result.final_url)
        article = extract_pdf_article(source, candidate, fetcher)
        verification = verify_page_relevance_and_date(article, result.text, candidate, target_date=target_date, final_url=result.final_url, source=source)
        article = _apply_verification_to_article(article, verification, source=source)
        return ExtractedPage(article=article, raw_html=result.text, final_url=result.final_url, verification=verification)

    base_article: Optional[Article] = None
    base_verification: Optional[VerificationDecision] = None
    if result.status_code < 400 and result.text:
        base_article = _extract_article_from_html(source, candidate, result.text, result.final_url, result.method, result.warnings)
        base_verification = verify_page_relevance_and_date(
            base_article,
            result.text,
            candidate,
            target_date=target_date,
            final_url=result.final_url,
            source=source,
        )
        base_article = _apply_verification_to_article(base_article, base_verification, source=source)
    base_result = ExtractedPage(
        article=base_article,
        raw_html=result.text if result.status_code < 400 else "",
        final_url=result.final_url,
        verification=base_verification,
    )

    dynamic_article: Optional[Article] = None
    dynamic_verification: Optional[VerificationDecision] = None
    rendered_html = ""
    rendered_final_url = result.final_url
    should_try_dynamic = _should_try_dynamic_article(source, base_article, result.method, target_date=target_date, verification=base_verification)
    if should_try_dynamic:
        rendered_html, rendered_final_url, dynamic_warnings = _render_article_html(source, candidate, fetcher)
        if rendered_html:
            dynamic_article = _extract_article_from_html(
                source,
                candidate,
                rendered_html,
                rendered_final_url,
                "dynamic_playwright",
                (result.warnings or []) + dynamic_warnings,
            )
            dynamic_verification = verify_page_relevance_and_date(
                dynamic_article,
                rendered_html,
                candidate,
                target_date=target_date,
                final_url=rendered_final_url,
                source=source,
            )
            dynamic_article = _apply_verification_to_article(dynamic_article, dynamic_verification, source=source)
    else:
        LOG.info(
            "Dynamic article render skipped | thread_id=%s | source=%s | url=%s | reason=base_extraction_sufficient",
            get_ident(),
            source.name,
            candidate.url,
        )
    dynamic_result = ExtractedPage(
        article=dynamic_article,
        raw_html=rendered_html,
        final_url=rendered_final_url,
        verification=dynamic_verification,
    )

    chosen_result = _choose_better_extracted_page(base_result, dynamic_result) or base_result
    if chosen_result.article is not None:
        chosen_result.article.warnings.append(f"candidate_method:{candidate.method}")
    return chosen_result


def extract_article_page(
    source: SourceConfig,
    candidate: Candidate,
    fetcher: HttpFetcher,
    allow_pdf: bool = True,
    firecrawl_client: FirecrawlClient | None = None,
    target_date: Optional[str] = None,
) -> ExtractedPage:
    if looks_like_pdf(candidate.url):
        return _extract_article_local(source, candidate, fetcher, allow_pdf=allow_pdf, target_date=target_date)

    provider = _scrape_provider()
    firecrawl_result: Optional[ExtractedPage] = None
    if provider in {"auto", "firecrawl"} and firecrawl_client is not None:
        try:
            firecrawl_result = _extract_article_firecrawl(source, candidate, firecrawl_client)
            if firecrawl_result.article is not None and firecrawl_result.verification is not None:
                firecrawl_result.verification = verify_page_relevance_and_date(
                    firecrawl_result.article,
                    firecrawl_result.raw_html,
                    candidate,
                    target_date=target_date,
                    final_url=firecrawl_result.final_url,
                    extracted_metadata=firecrawl_result.metadata,
                    source=source,
                )
                firecrawl_result.article = _apply_verification_to_article(firecrawl_result.article, firecrawl_result.verification, source=source)
        except Exception as exc:
            LOG.warning("Firecrawl scrape failed | source=%s | url=%s | error=%s", source.name, candidate.url, exc)
            _debug_event("firecrawl_scrape_failed", source, url=candidate.url, error=str(exc))

    need_local_fallback = (
        provider == "local"
        or firecrawl_client is None
        or firecrawl_result is None
        or (provider == "auto" and _firecrawl_article_needs_local_fallback(source, firecrawl_result, target_date))
        or (provider == "firecrawl" and _firecrawl_article_needs_local_fallback(source, firecrawl_result, target_date))
    )
    if not need_local_fallback and firecrawl_result is not None:
        return firecrawl_result

    local_result = _extract_article_local(source, candidate, fetcher, allow_pdf=allow_pdf, target_date=target_date)
    return _choose_better_extracted_page(firecrawl_result, local_result) or local_result


def extract_article(
    source: SourceConfig,
    candidate: Candidate,
    fetcher: HttpFetcher,
    allow_pdf: bool = True,
    firecrawl_client: FirecrawlClient | None = None,
    target_date: Optional[str] = None,
) -> Optional[Article]:
    return extract_article_page(
        source=source,
        candidate=candidate,
        fetcher=fetcher,
        allow_pdf=allow_pdf,
        firecrawl_client=firecrawl_client,
        target_date=target_date,
    ).article

@dataclass(slots=True)
class AcceptanceDecision:
    accepted: bool
    score: int
    reasons: list[str] = field(default_factory=list)
    reject_reasons: list[str] = field(default_factory=list)


def _log_article_event(
    stage: str,
    article: Article,
    score: int = 0,
    reasons: list[str] | None = None,
    reject_reasons: list[str] | None = None,
) -> None:
    reasons = reasons or []
    reject_reasons = reject_reasons or []
    message = (
        f"stage={stage} | url={article.source_url or ''} | score={score} | "
        f"title={truncate_text(article.title, max_chars=180)} | "
        f"reasons={reasons} | reject_reasons={reject_reasons}"
    )
    if stage == "accepted":
        LOG.info("Accepted article | %s", message)
    elif stage in {"discarded", "rejected"}:
        LOG.warning("Discarded article | %s", message)
    else:
        LOG.info("Article stage | %s", message)


def _log_candidate_verification(candidate: Candidate, page_result: ExtractedPage) -> None:
    verification = page_result.verification or VerificationDecision()
    article = page_result.article
    structurally_accepted = _verification_structure_accepted(verification)
    LOG.info(
        "Candidate verify | url=%s | metals=%s | same_day=%s | market_context=%s | published_at=%s | same_day_hint=%s | date_sources_checked=%s | should_summarize=%s | reject_reasons=%s",
        page_result.final_url or candidate.url,
        verification.verified_metals,
        verification.has_valid_same_day_date,
        verification.has_market_context,
        verification.published_at,
        verification.same_day_hint,
        verification.date_sources_checked,
        verification.has_target_metal and verification.has_market_context and verification.has_valid_same_day_date and structurally_accepted,
        verification.reject_reasons,
    )
    LOG.info(
        "Candidate classify | url=%s | page_type=%s | narrative_chars=%s | table_density=%s | quote_signal=%s | accepted=%s",
        page_result.final_url or candidate.url,
        verification.page_type,
        verification.narrative_chars,
        verification.table_density,
        verification.quote_signal,
        structurally_accepted,
    )
    if article is not None:
        LOG.info(
            "Candidate extracted | url=%s | extracted_title=%s | extracted_published_at=%s",
            article.source_url,
            truncate_text(article.title, max_chars=180),
            article.published_at,
        )


def _days_from_target(published_at: str, target_date: str) -> int:
    try:
        left = date.fromisoformat(published_at[:10])
        right = date.fromisoformat(target_date)
    except Exception:
        return 999
    return abs((left - right).days)


def _date_hint_matches_target(published_hint: str, target_date: str) -> bool:
    hint = normalize_space(published_hint).lower()
    if not hint:
        return False
    if any(token in hint for token in ("today", "just now", "minute ago", "minutes ago", "hour ago", "hours ago", "moments ago")):
        return target_date == date.today().isoformat()
    parsed = parse_isoish_date(published_hint)
    if not parsed:
        return False
    return _days_from_target(parsed, target_date) == 0


def _effective_target_date(target_date: Optional[str]) -> str:
    cleaned = normalize_space(target_date)
    if cleaned:
        return cleaned
    return date.today().isoformat()


def _evaluate_article(
    source: SourceConfig,
    article: Article,
    target_date: Optional[str],
    verification: VerificationDecision | None = None,
) -> AcceptanceDecision:
    target_date = _effective_target_date(target_date)
    source_role = _source_role_value(source)
    text = f"{article.title}\n{article.body_text}\n{article.discovery_snippet}".lower()
    fast_score = fast_relevance_score(article.title, article.body_text)
    discovery_score = fast_relevance_score(article.title, article.discovery_snippet)
    body_len = len(article.body_text or "")
    reportish = article.extraction_method.startswith("pdf:") or looks_like_report_url(article.source_url)
    reportish = reportish or any(token in text for token in ("report", "research", "forecast", "outlook", "analysis", "insight"))
    verification = verification or VerificationDecision(
        has_target_metal=has_primary_target_metal(text),
        verified_metals=normalize_metal_mentions(text),
        has_market_context=has_market_context(text),
        has_valid_same_day_date=_article_same_day_signal(article, target_date),
        published_at=article.published_at,
    )
    metals_signal = verification.has_target_metal
    primary_signal = bool(verification.verified_metals)
    date_valid = verification.has_valid_same_day_date
    market_context_valid = verification.has_market_context

    score = 0
    reasons: list[str] = []
    reject_reasons: list[str] = list(verification.reject_reasons)
    title_snippet_signal = _strong_market_signal(f"{article.title}\n{article.discovery_snippet}") or has_market_context(f"{article.title}\n{article.discovery_snippet}")

    if verification.published_at and (not article.published_at or verification.has_valid_same_day_date):
        article.published_at = verification.published_at

    if not metals_signal and fast_score < env_int("ARTICLE_MIN_FAST_SCORE", 2):
        reject_reasons.append("weak_topic")
    elif metals_signal and market_context_valid:
        score += 5
        reasons.append("page_verified")
    elif topic_matches(article):
        score += 4
        reasons.append("topic_match")
    elif fast_score >= env_int("RELAXED_TOPIC_SCORE_THRESHOLD", 4):
        score += 2
        reasons.append("topic_relaxed")
    elif discovery_score >= env_int("DISCOVERY_RESCUE_TOPIC_SCORE_THRESHOLD", 4):
        score += 2
        reasons.append("listing_context_match")

    if primary_signal:
        score += 2
        reasons.append("primary_metals")

    if body_len >= source.min_body_chars:
        score += 2
        reasons.append("body_ok")
    elif source.paywalled_preview_ok and body_len >= env_int("PAYWALL_PREVIEW_MIN_BODY_CHARS", 55):
        score += 1
        reasons.append("paywall_preview_ok")
    elif reportish and body_len >= env_int("REPORT_MIN_BODY_CHARS", 90):
        score += 1
        reasons.append("report_body_ok")
    elif title_snippet_signal:
        score += 1
        reasons.append("title_snippet_rescue")
    elif fast_score < env_int("SHORT_BODY_RESCUE_FAST_SCORE", 6):
        reject_reasons.append("body_too_short")

    if market_context_valid:
        score += 3
        reasons.append("verified_market_context")
    elif content_type_matches(article):
        score += 3
        reasons.append("content_match")
    elif reportish:
        score += 2
        reasons.append("reportish")
    elif body_len >= env_int("LONGFORM_RESCUE_BODY_CHARS", 450) and fast_score >= env_int("LONGFORM_RESCUE_FAST_SCORE", 4):
        score += 1
        reasons.append("longform_rescue")
    else:
        score -= 1
        reject_reasons.append("weak_content_type")

    if date_valid:
        score += 3
        reasons.append("verified_target_date")
    elif article.published_at:
        days = _days_from_target(article.published_at, target_date)
        if days == 0:
            score += 3
            reasons.append("target_date_exact")
            date_valid = True
        elif _date_hint_matches_target(article.discovery_published_hint, target_date):
            score += 2
            reasons.append("date_hint_match")
            date_valid = True
        else:
            reject_reasons.append("date_mismatch")
    else:
        if _date_hint_matches_target(article.discovery_published_hint, target_date):
            score += 2
            reasons.append("date_hint_match")
            date_valid = True
        else:
            reject_reasons.append("missing_date")

    if article.scope != "Unknown":
        score += 1
        reasons.append("scope_known")
    if article.extraction_method.startswith("pdf:"):
        score -= env_int("PDF_ACCEPTANCE_PENALTY", 1)
        reasons.append("pdf_last_resort")
    if article.fetch_method == "dynamic_playwright":
        score += 1
        reasons.append("rendered")

    if source_role == "narrative_news":
        if verification.page_type in {"analyst_article", "market_news"}:
            score += 1
            reasons.append("narrative_source_fit")
    elif source_role == "market_data":
        if _market_data_summary_allowed(verification) or _page_type_override_allowed(verification):
            score += 1
            reasons.append("market_data_narrative_upgrade")
        else:
            reject_reasons.append("market_data_reference_only")
    elif source_role == "market_research":
        if _market_research_summary_allowed(verification, reportish=reportish):
            score += 1
            reasons.append("market_research_context")
        else:
            reject_reasons.append("market_research_background_only")

    min_accept_score = env_int("ARTICLE_ACCEPTANCE_SCORE_THRESHOLD", 4)
    hard_rejects = {
        "date_mismatch_after_full_page_check",
        "missing_date_after_full_page_check",
        "missing_target_metal_after_page_check",
        "missing_market_context_after_page_check",
        "page_type_market_data_page",
        "page_type_quote_page",
        "page_type_category_page",
        "page_type_archive_page",
        "page_type_ticker_page",
        "page_type_futures_table_page",
        "page_type_symbol_landing_page",
        "insufficient_narrative_body",
        "quote_signal_high",
        "market_data_reference_only",
        "market_research_background_only",
    }
    accepted = metals_signal and market_context_valid and date_valid and not any(reason in hard_rejects for reason in reject_reasons) and score >= min_accept_score

    if not accepted and score >= env_int("ARTICLE_RESCUE_SCORE_THRESHOLD", 7) and metals_signal and market_context_valid and date_valid and not any(reason in hard_rejects for reason in reject_reasons):
        accepted = True
        reasons.append("score_rescue")

    return AcceptanceDecision(accepted=accepted, score=score, reasons=reasons, reject_reasons=reject_reasons)


def _shortlist_candidates(
    source: SourceConfig,
    candidates: list[Candidate],
    target_date: Optional[str],
    max_candidates: int,
    dynamic_enabled: bool,
) -> tuple[list[Candidate], dict[str, int]]:
    shortlist_threshold = env_int("DISCOVERY_SHORTLIST_SCORE_THRESHOLD", 2)
    shortlist_cap = max_candidates // 2
    if dynamic_enabled:
        shortlist_cap = max(shortlist_cap, int(max_candidates * env_float("DYNAMIC_SHORTLIST_CAP_RATIO", 0.75)))
    has_firecrawl_candidates = any(candidate.method.startswith("firecrawl") for candidate in candidates)
    if has_firecrawl_candidates:
        shortlist_threshold = min(shortlist_threshold, 0)
    shortlist_limit = env_int("MAX_SHORTLIST_PER_SOURCE", max(18, shortlist_cap))
    if has_firecrawl_candidates:
        shortlist_cap = max(shortlist_cap, int(max_candidates * env_float("FIRECRAWL_SHORTLIST_CAP_RATIO", 0.9)))
        shortlist_limit = max(shortlist_limit, env_int("FIRECRAWL_SHORTLIST_LIMIT", max(15, shortlist_cap)))
    firecrawl_keep_floor = min(shortlist_limit, env_int("FIRECRAWL_SHORTLIST_MIN_KEEP", 12))

    scored_html: list[tuple[int, Candidate]] = []
    scored_pdf: list[tuple[int, Candidate]] = []
    effective_target_date = _effective_target_date(target_date)
    shortlist_debug = {
        "candidate_count": len(candidates),
        "shortlisted_count": 0,
        "shortlisted_title_snippet": 0,
        "shortlisted_date_hint": 0,
        "shortlisted_url_date_signal": 0,
    }
    for candidate in candidates:
        score = candidate.score
        topical_text = f"{candidate.title_hint}\n{candidate.snippet_hint}\n{candidate.url}"
        topical_low = topical_text.lower()
        strong_title_snippet = has_required_target_metal(topical_text) and any(term in topical_low for term in SHORTLIST_PRIORITY_TERMS)
        same_day_hint = False
        url_same_day = _same_day_url_match(candidate.url, effective_target_date)
        priority_signal = any(term in topical_low for term in SHORTLIST_PRIORITY_TERMS)
        reliable_same_day_signal = any(token in normalize_space(candidate.published_hint).lower() for token in ("today", "hour ago", "hours ago", "minute ago", "minutes ago"))
        if _is_obviously_old_archive_url(candidate.url, effective_target_date):
            continue
        if _looks_like_section_page(candidate.url):
            score -= 10
        if any(term in topical_low for term in IRRELEVANT_COMPANY_TERMS) and not priority_signal and not url_same_day:
            continue
        if not has_required_target_metal(topical_text) and not any(term in topical_low for term in MARKET_STRUCTURE_TERMS) and not priority_signal and not reliable_same_day_signal and not url_same_day and not candidate.method.startswith("firecrawl"):
            continue
        if not has_required_target_metal(topical_text):
            score -= env_int("SHORTLIST_NON_TARGET_PENALTY", 5)
        else:
            score += 7
        score += sum(3 for term in SHORTLIST_PRIORITY_TERMS if term in topical_low)
        if effective_target_date and candidate.published_hint:
            published = parse_isoish_date(candidate.published_hint)
            if published and published[:10] == effective_target_date:
                same_day_hint = True
                score += 9
            elif _date_hint_matches_target(candidate.published_hint, effective_target_date):
                same_day_hint = True
                score += 8
        if url_same_day:
            score += 10
        if "/archive/" in candidate.url.lower():
            score -= 8
        if "reuters.com" in host_of(candidate.url) and not url_same_day and effective_target_date:
            score -= 8
        if strong_title_snippet:
            score += 7
        if candidate.method in {"dynamic_card", "dynamic_listing", "dynamic_second_hop", "dynamic_scrapy"}:
            score += 1
        if candidate.method.startswith("firecrawl"):
            score += 3
        if _is_pdf_candidate(candidate):
            score -= env_int("DISCOVERY_PDF_SHORTLIST_PENALTY", 3)
            scored_pdf.append((score, candidate))
        else:
            if looks_like_report_url(candidate.url):
                score += 1
            score += env_int("DISCOVERY_HTML_SHORTLIST_BONUS", 1)
            scored_html.append((score, candidate))
    scored_html.sort(key=lambda item: item[0], reverse=True)
    scored_pdf.sort(key=lambda item: item[0], reverse=True)
    ordered = scored_html + scored_pdf

    shortlisted: list[Candidate] = []
    fallback_pool: list[Candidate] = []
    html_shortlisted = 0
    for score, candidate in ordered:
        if score >= shortlist_threshold:
            shortlisted.append(candidate)
            if not _is_pdf_candidate(candidate):
                html_shortlisted += 1
        elif score >= env_int("DISCOVERY_BACKFILL_SCORE_THRESHOLD", 1) or looks_like_report_url(candidate.url):
            fallback_pool.append(candidate)
        if len(shortlisted) >= shortlist_cap:
            break

    if has_firecrawl_candidates and len(shortlisted) < firecrawl_keep_floor:
        for score, candidate in ordered:
            if candidate in shortlisted:
                continue
            topical_text = f"{candidate.title_hint}\n{candidate.snippet_hint}\n{candidate.url}".lower()
            has_keep_signal = (
                has_required_target_metal(topical_text)
                or _date_hint_matches_target(candidate.published_hint, effective_target_date)
                or _same_day_url_match(candidate.url, effective_target_date)
                or any(term in topical_text for term in SHORTLIST_PRIORITY_TERMS)
            )
            if not has_keep_signal:
                continue
            shortlisted.append(candidate)
            if len(shortlisted) >= firecrawl_keep_floor:
                break

    if _pdf_last_resort_enabled():
        preferred_html = env_int("DISCOVERY_MIN_HTML_SHORTLIST", max(6, shortlist_limit // 2))
        if has_firecrawl_candidates:
            preferred_html = max(preferred_html, firecrawl_keep_floor)
        shortlisted_html = [candidate for candidate in shortlisted if not _is_pdf_candidate(candidate)]
        shortlisted_pdf = [candidate for candidate in shortlisted if _is_pdf_candidate(candidate)]
        fallback_html = [candidate for candidate in fallback_pool if not _is_pdf_candidate(candidate)]
        fallback_pdf = [candidate for candidate in fallback_pool if _is_pdf_candidate(candidate)]
        shortlisted = shortlisted_html[:preferred_html]
        if len(shortlisted) < shortlist_limit:
            shortlisted.extend(fallback_html[: shortlist_limit - len(shortlisted)])
        if len(shortlisted) < shortlist_limit and len(shortlisted_html) < preferred_html:
            shortlisted.extend(shortlisted_pdf[: shortlist_limit - len(shortlisted)])
        if len(shortlisted) < shortlist_limit:
            shortlisted.extend(fallback_pdf[: shortlist_limit - len(shortlisted)])

    if len(shortlisted) < shortlist_limit:
        shortlisted.extend(fallback_pool[: shortlist_limit - len(shortlisted)])

    deduped: list[Candidate] = []
    seen: set[str] = set()
    for candidate in shortlisted[:shortlist_limit]:
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        deduped.append(candidate)
    shortlist_debug["shortlisted_count"] = len(deduped)
    for candidate in deduped:
        topical_text = f"{candidate.title_hint}\n{candidate.snippet_hint}\n{candidate.url}".lower()
        if has_required_target_metal(topical_text) and any(term in topical_text for term in SHORTLIST_PRIORITY_TERMS):
            shortlist_debug["shortlisted_title_snippet"] += 1
        if _date_hint_matches_target(candidate.published_hint, effective_target_date):
            shortlist_debug["shortlisted_date_hint"] += 1
        if _same_day_url_match(candidate.url, effective_target_date):
            shortlist_debug["shortlisted_url_date_signal"] += 1
    return deduped, shortlist_debug


def scrape_source(
    source: SourceConfig,
    fetcher: HttpFetcher,
    max_candidates: int = 80,
    max_articles: int = 8,
    target_date: Optional[str] = None,
    firecrawl_client: FirecrawlClient | None = None,
) -> list[Article]:
    target_date = _effective_target_date(target_date)
    source_started = time.monotonic()
    provider = _scrape_provider()
    default_time_budget = 90 if provider in {"auto", "firecrawl"} else 150
    source_time_budget = env_int("SOURCE_TIME_BUDGET_SECONDS", default_time_budget)

    candidates = discover_candidates(source, fetcher, target_date=target_date, firecrawl_client=firecrawl_client)[:max_candidates]
    shortlisted, shortlist_debug = _shortlist_candidates(source, candidates, target_date, max_candidates, _dynamic_discovery_enabled(source))
    prefiltered: list[Candidate] = []
    prefilter_rejected = 0
    prefilter_kept = 0
    prefilter_floor = min(
        len(shortlisted),
        env_int(
            "FAST_PREFILTER_MIN_KEEP",
            6 if _source_role_value(source) == "narrative_news" else 4,
        ),
    )
    for candidate in shortlisted:
        accepted_fast, fast_score, fast_reasons = _fast_candidate_prefilter(source, candidate, target_date)
        if accepted_fast:
            candidate.score = max(candidate.score, fast_score)
            prefiltered.append(candidate)
            prefilter_kept += 1
            LOG.info(
                "Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s",
                candidate.url,
                True,
                fast_score,
                fast_reasons,
            )
            continue
        if len(prefiltered) < prefilter_floor and candidate.method.startswith("firecrawl"):
            candidate.score = max(candidate.score, fast_score)
            prefiltered.append(candidate)
            prefilter_kept += 1
            LOG.info(
                "Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s",
                candidate.url,
                True,
                fast_score,
                fast_reasons + ["firecrawl_floor_keep"],
            )
            continue
        prefilter_rejected += 1
        LOG.info(
            "Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s",
            candidate.url,
            False,
            fast_score,
            fast_reasons,
        )
    shortlisted = prefiltered
    LOG.info(
        "Shortlist summary | source=%s | candidate_count=%s | shortlisted_count=%s | prefilter_kept=%s | prefilter_rejected=%s | title_snippet=%s | date_hint=%s | url_date_signal=%s",
        source.name,
        shortlist_debug.get("candidate_count", 0),
        shortlist_debug.get("shortlisted_count", 0),
        prefilter_kept,
        prefilter_rejected,
        shortlist_debug.get("shortlisted_title_snippet", 0),
        shortlist_debug.get("shortlisted_date_hint", 0),
        shortlist_debug.get("shortlisted_url_date_signal", 0),
    )
    _debug_event(
        "shortlist_done",
        source,
        discovered=len(candidates),
        shortlisted=len(shortlisted),
        target_date=target_date,
        shortlist_debug=shortlist_debug,
    )

    articles: list[Article] = []
    seen: set[str] = set()
    default_max_attempts = max(max_articles * 2, 10 if provider in {"auto", "firecrawl"} else 14)
    max_attempts = env_int("MAX_EXTRACTION_ATTEMPTS_PER_SOURCE", default_max_attempts)
    attempts = 0
    minimum_html_articles = _minimum_html_target(max_articles)

    html_candidates = [candidate for candidate in shortlisted if not _is_pdf_candidate(candidate)]
    pdf_candidates = [candidate for candidate in shortlisted if _is_pdf_candidate(candidate)]
    ordered_candidates = html_candidates + ([] if _pdf_last_resort_enabled() else pdf_candidates)

    if _pdf_last_resort_enabled():
        _debug_event(
            "pdf_last_resort_mode",
            source,
            html_candidates=len(html_candidates),
            pdf_candidates=len(pdf_candidates),
            min_html_articles=minimum_html_articles,
        )

    for candidate in ordered_candidates:
        if len(articles) >= max_articles:
            break
        if (time.monotonic() - source_started) >= source_time_budget:
            LOG.info("Stopping source after time budget | source=%s | seconds=%s", source.name, source_time_budget)
            break
        if attempts >= max_attempts:
            LOG.info("Stopping source after attempt budget | source=%s | attempts=%s", source.name, attempts)
            break
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        attempts += 1

        LOG.info("Opened page | source=%s | url=%s | candidate_method=%s", source.name, candidate.url, candidate.method)
        page_result = extract_article_page(
            source,
            candidate,
            fetcher,
            allow_pdf=not _pdf_last_resort_enabled(),
            firecrawl_client=firecrawl_client,
            target_date=target_date,
        )
        _log_candidate_verification(candidate, page_result)
        article = page_result.article
        if article is None:
            _debug_event("extract_none", source, url=candidate.url, candidate_method=candidate.method)
            continue
        _log_article_event("extracted", article, score=article.score, reasons=[f"candidate_method:{candidate.method}"])

        decision = _evaluate_article(source, article, target_date, verification=page_result.verification)
        topic_reasons = [reason for reason in decision.reasons if reason in {"page_verified", "topic_match", "topic_relaxed", "listing_context_match", "primary_metals"}]
        topic_rejects = [reason for reason in decision.reject_reasons if reason in {"weak_topic"}]
        _log_article_event("topic_check", article, score=decision.score, reasons=topic_reasons, reject_reasons=topic_rejects)
        content_reasons = [reason for reason in decision.reasons if reason in {"verified_market_context", "content_match", "reportish", "longform_rescue", "body_ok", "paywall_preview_ok", "report_body_ok", "title_snippet_rescue"}]
        content_rejects = [reason for reason in decision.reject_reasons if reason in {"weak_content_type", "body_too_short", "missing_market_context_after_page_check"}]
        _log_article_event("content_check", article, score=decision.score, reasons=content_reasons, reject_reasons=content_rejects)
        date_reasons = [reason for reason in decision.reasons if reason in {"verified_target_date", "target_date_exact", "date_hint_match"}]
        date_rejects = [reason for reason in decision.reject_reasons if reason in {"date_mismatch", "missing_date", "date_mismatch_after_full_page_check", "missing_date_after_full_page_check"}]
        _log_article_event("date_check", article, score=decision.score, reasons=date_reasons, reject_reasons=date_rejects)
        article.warnings.append(f"acceptance_score:{decision.score}")
        if decision.reasons:
            article.warnings.append("acceptance_reasons:" + ",".join(decision.reasons))
        if decision.reject_reasons:
            article.warnings.append("reject_reasons:" + ",".join(decision.reject_reasons))

        if not decision.accepted:
            _log_article_event("discarded", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)
            _debug_event(
                "article_rejected",
                source,
                url=article.source_url,
                title=article.title,
                score=decision.score,
                reasons=decision.reasons,
                reject_reasons=decision.reject_reasons,
                published_at=article.published_at,
                extraction_method=article.extraction_method,
                fetch_method=article.fetch_method,
            )
            continue

        article.score = max(article.score, decision.score)
        articles.append(article)
        _log_article_event("accepted", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)
        _debug_event(
            "article_accepted",
            source,
            url=article.source_url,
            title=article.title,
            score=decision.score,
            reasons=decision.reasons,
            published_at=article.published_at,
            extraction_method=article.extraction_method,
            fetch_method=article.fetch_method,
        )

    html_articles = [article for article in articles if not article.extraction_method.startswith("pdf:")]
    if _pdf_last_resort_enabled() and len(articles) < max_articles and len(html_articles) < minimum_html_articles:
        for candidate in pdf_candidates:
            if len(articles) >= max_articles:
                break
            if (time.monotonic() - source_started) >= source_time_budget:
                LOG.info("Stopping pdf fallback after time budget | source=%s | seconds=%s", source.name, source_time_budget)
                break
            if attempts >= max_attempts:
                LOG.info("Stopping pdf fallback after attempt budget | source=%s | attempts=%s", source.name, attempts)
                break
            if candidate.url in seen:
                continue
            seen.add(candidate.url)
            attempts += 1

            LOG.info("Opened page | source=%s | url=%s | candidate_method=%s", source.name, candidate.url, candidate.method)
            page_result = extract_article_page(
                source,
                candidate,
                fetcher,
                allow_pdf=True,
                firecrawl_client=firecrawl_client,
                target_date=target_date,
            )
            _log_candidate_verification(candidate, page_result)
            article = page_result.article
            if article is None:
                _debug_event("pdf_extract_none", source, url=candidate.url, candidate_method=candidate.method)
                continue
            _log_article_event("extracted", article, score=article.score, reasons=[f"candidate_method:{candidate.method}", "pdf_fallback_attempt"])

            decision = _evaluate_article(source, article, target_date, verification=page_result.verification)
            topic_reasons = [reason for reason in decision.reasons if reason in {"page_verified", "topic_match", "topic_relaxed", "listing_context_match", "primary_metals"}]
            topic_rejects = [reason for reason in decision.reject_reasons if reason in {"weak_topic"}]
            _log_article_event("topic_check", article, score=decision.score, reasons=topic_reasons, reject_reasons=topic_rejects)
            content_reasons = [reason for reason in decision.reasons if reason in {"verified_market_context", "content_match", "reportish", "longform_rescue", "body_ok", "paywall_preview_ok", "report_body_ok", "title_snippet_rescue"}]
            content_rejects = [reason for reason in decision.reject_reasons if reason in {"weak_content_type", "body_too_short", "missing_market_context_after_page_check"}]
            _log_article_event("content_check", article, score=decision.score, reasons=content_reasons, reject_reasons=content_rejects)
            date_reasons = [reason for reason in decision.reasons if reason in {"verified_target_date", "target_date_exact", "date_hint_match"}]
            date_rejects = [reason for reason in decision.reject_reasons if reason in {"date_mismatch", "missing_date", "date_mismatch_after_full_page_check", "missing_date_after_full_page_check"}]
            _log_article_event("date_check", article, score=decision.score, reasons=date_reasons, reject_reasons=date_rejects)
            article.warnings.append("pdf_fallback_attempt")
            article.warnings.append(f"acceptance_score:{decision.score}")
            if decision.reasons:
                article.warnings.append("acceptance_reasons:" + ",".join(decision.reasons))
            if decision.reject_reasons:
                article.warnings.append("reject_reasons:" + ",".join(decision.reject_reasons))

            if not decision.accepted:
                _log_article_event("discarded", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)
                _debug_event(
                    "pdf_article_rejected",
                    source,
                    url=article.source_url,
                    title=article.title,
                    score=decision.score,
                    reasons=decision.reasons,
                    reject_reasons=decision.reject_reasons,
                    published_at=article.published_at,
                    extraction_method=article.extraction_method,
                    fetch_method=article.fetch_method,
                )
                continue

            article.score = max(article.score, decision.score)
            articles.append(article)
            _log_article_event("accepted", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)
            _debug_event(
                "pdf_article_accepted",
                source,
                url=article.source_url,
                title=article.title,
                score=decision.score,
                reasons=decision.reasons,
                published_at=article.published_at,
                extraction_method=article.extraction_method,
                fetch_method=article.fetch_method,
            )

    LOG.info(
        "Source scrape summary | source=%s | candidate_count=%s | shortlisted_count=%s | accepted_count=%s",
        source.name,
        len(candidates),
        len(shortlisted),
        len(articles),
    )
    return articles


def scrape_sources(
    sources: list[SourceConfig],
    target_date: Optional[str],
    max_articles_total: int = 20,
    per_source_limit: int = 8,
    user_agent: str = "Mozilla/5.0 (compatible; AnalystBriefBot/1.0; +https://example.com/bot)",
) -> list[Article]:
    target_date = _effective_target_date(target_date)
    provider = _scrape_provider()
    fetcher_kwargs = {
        "user_agent": user_agent,
        "connect_timeout": 10.0,
        "read_timeout": env_float("SCRAPE_READ_TIMEOUT", 20.0),
        "per_host_delay": env_float("SCRAPE_PER_HOST_DELAY", 1.5),
        "enable_http2": True,
        "enable_browser_fallback": True,
    }
    default_max_candidates = 45 if provider in {"auto", "firecrawl"} else 70
    scrape_max_candidates = env_int("SCRAPE_MAX_CANDIDATES", default_max_candidates)
    source_workers = max(1, min(len(sources), env_int("SCRAPE_SOURCE_WORKERS", 3)))
    LOG.info(
        "Scrape provider active | provider=%s | firecrawl=%s | target_date=%s | source_workers=%s | max_candidates=%s",
        provider,
        provider in {"auto", "firecrawl"},
        target_date,
        source_workers,
        scrape_max_candidates,
    )

    def _run_source(source: SourceConfig) -> list[Article]:
        local_fetcher = HttpFetcher(**fetcher_kwargs)
        local_firecrawl_client = _build_firecrawl_client() if provider in {"auto", "firecrawl"} else None
        try:
            return scrape_source(
                source=source,
                fetcher=local_fetcher,
                max_candidates=scrape_max_candidates,
                max_articles=per_source_limit,
                target_date=target_date,
                firecrawl_client=local_firecrawl_client,
            )
        finally:
            try:
                _close_dynamic_renderer(source_name=source.name, reason="source_worker_done")
            finally:
                try:
                    local_fetcher.close()
                finally:
                    if local_firecrawl_client is not None:
                        local_firecrawl_client.close()

    try:
        all_articles: list[Article] = []
        if source_workers > 1 and len(sources) > 1:
            with ThreadPoolExecutor(max_workers=source_workers) as executor:
                future_map = {executor.submit(_run_source, source): source for source in sources}
                for future in as_completed(future_map):
                    source = future_map[future]
                    try:
                        articles = future.result()
                        all_articles.extend(articles)
                        LOG.info("Accepted %s articles for %s", len(articles), source.name)
                        _debug_event("source_done", source, accepted=len(articles))
                    except Exception as exc:
                        LOG.exception("Source failed | source=%s | error=%s", source.name, exc)
                        _debug_event("source_failed", source, error=str(exc))
        else:
            for source in sources:
                try:
                    articles = _run_source(source)
                    all_articles.extend(articles)
                    LOG.info("Accepted %s articles for %s", len(articles), source.name)
                    _debug_event("source_done", source, accepted=len(articles))
                    if len(all_articles) >= max_articles_total:
                        break
                except Exception as exc:
                    LOG.exception("Source failed | source=%s | error=%s", source.name, exc)
                    _debug_event("source_failed", source, error=str(exc))
                    continue

        deduped: dict[str, Article] = {}
        for article in all_articles:
            key = normalize_url(article.source_url)
            previous = deduped.get(key)
            if previous is None or article.score > previous.score:
                deduped[key] = article

        ordered = sorted(deduped.values(), key=lambda article: (article.published_at, article.score), reverse=True)
        return ordered[:max_articles_total]
    finally:
        _close_all_dynamic_renderers()
