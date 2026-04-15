
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
import logging
import re
import time
import urllib.robotparser
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
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
    from htmldate import find_date
except Exception:  # pragma: no cover - optional
    find_date = None

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional
    sync_playwright = None


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


@dataclass(slots=True)
class Candidate:
    source_name: str
    url: str
    listing_url: str
    title_hint: str = ""
    published_hint: str = ""
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
        result = self.fetch(url, allow_browser_fallback=False, respect_robots=respect_robots)
        if not result.text:
            return b"", result
        body = result.text.encode("utf-8", errors="ignore")
        return body, result


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

DIRECT_TERMS = (
    "copper", "aluminum", "aluminium", "base metals", "industrial metals", "metals", "mining"
)

ANALYSIS_TERMS = (
    "analysis", "outlook", "commentary", "review", "insight", "insights", "forecast", "market"
)

NEGATIVE_TERMS = (
    "crypto", "bitcoin", "ethereum", "etf", "s&p 500", "nasdaq"
)


def candidate_score(title: str, url: str) -> int:
    text = f"{title} {url}".lower()
    score = 0
    score += sum(4 for t in DIRECT_TERMS if t in text)
    score += sum(2 for t in ANALYSIS_TERMS if t in text)
    score -= sum(5 for t in NEGATIVE_TERMS if t in text)
    if re.search(r"/20\d{2}/\d{1,2}/\d{1,2}/", url):
        score += 2
    return score


def source_accepts_url(source: SourceConfig, url: str) -> bool:
    normalized = normalize_url(url)
    if not looks_like_article(normalized):
        return False
    host = host_of(normalized)
    if source.allowed_hosts and not any(host == h or host.endswith("." + h) for h in source.allowed_hosts):
        return False
    low = normalized.lower()
    if source.exclude_hints and any(h in low for h in source.exclude_hints):
        return False
    if source.include_hints and not any(h in low for h in source.include_hints):
        # soft allow if URL still looks content-rich
        if candidate_score("", normalized) < 2:
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
            cand = Candidate(source.name, link, rss_url, title_hint=title, published_hint=published, method="rss")
            cand.score = candidate_score(title, link) + 4
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
    for seed in source.seed_urls:
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
                cand.score = candidate_score("", loc_text) + 2
                prev = found.get(loc_text)
                if prev is None or cand.score > prev.score:
                    found[loc_text] = cand

    return sorted(found.values(), key=lambda x: x.score, reverse=True)


def discover_from_listing_pages(source: SourceConfig, fetcher: HttpFetcher, second_hop_limit: int = 3) -> list[Candidate]:
    found: dict[str, Candidate] = {}

    def extract(page_url: str) -> list[Candidate]:
        result = fetcher.fetch(page_url, allow_browser_fallback=True)
        if result.status_code >= 400 or not result.text:
            return []
        soup = soupify(result.text)
        local: dict[str, Candidate] = {}
        for a in soup.find_all("a", href=True):
            href = normalize_url(urljoin(page_url, a.get("href")))
            if not source_accepts_url(source, href):
                continue
            title = normalize_space(a.get_text(" ", strip=True))
            if len(title) < 8:
                parent = a.find_parent(["article", "li", "div", "section"])
                if parent:
                    heading = parent.find(["h1", "h2", "h3", "h4"])
                    title = first_nonempty(title, heading.get_text(" ", strip=True) if heading else "")
            published = ""
            parent = a.find_parent(["article", "li", "div", "section"])
            if parent:
                time_tag = parent.find("time")
                if time_tag:
                    published = normalize_space(time_tag.get("datetime") or time_tag.get_text(" ", strip=True))
            cand = Candidate(source.name, href, page_url, title_hint=title, published_hint=published, method="listing")
            cand.score = candidate_score(title, href)
            prev = local.get(href)
            if prev is None or cand.score > prev.score:
                local[href] = cand
        return list(local.values())

    section_pages: list[str] = []
    for seed in source.seed_urls:
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
                if any(t in text or t in href.lower() for t in source.category_terms):
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


def discover_candidates(source: SourceConfig, fetcher: HttpFetcher) -> list[Candidate]:
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
    return sorted(merged.values(), key=lambda x: x.score, reverse=True)


# ---------------------------------------------------------------------------
# Extraction + filtering
# ---------------------------------------------------------------------------

def classify_scope(title: str, body: str) -> str:
    text = f"{title}\n{body}".lower()
    copper = "copper" in text
    aluminum = "aluminum" in text or "aluminium" in text
    broad = "base metals" in text or "industrial metals" in text or "metals" in text
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
    text = f"{title}\n{body}".lower()
    score = 0
    score += sum(4 for t in DIRECT_TERMS if t in title.lower())
    score += sum(2 for t in DIRECT_TERMS if t in body.lower())
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


def extract_article(source: SourceConfig, candidate: Candidate, fetcher: HttpFetcher) -> Optional[Article]:
    result = fetcher.fetch(candidate.url, allow_browser_fallback=True)
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


def topic_matches(article: Article) -> bool:
    score = fast_relevance_score(article.title, article.body_text)
    if article.scope in {"Copper", "Aluminum", "Copper, Aluminum"} and score >= 3:
        return True
    if article.scope == "Broad Metals" and score >= 5:
        return True
    return False


def content_type_matches(article: Article) -> bool:
    text = f"{article.title}\n{article.body_text}".lower()
    # broader than your current TODAY_REQUIRED_CONTENT_TYPES filter
    required = ("news", "analysis", "review", "outlook", "commentary", "insight", "market")
    return any(t in text for t in required)


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
            continue
        if not topic_matches(article):
            continue
        if not content_type_matches(article):
            continue
        if not is_recent_enough(article, target_date=target_date, allow_missing_date=False):
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
