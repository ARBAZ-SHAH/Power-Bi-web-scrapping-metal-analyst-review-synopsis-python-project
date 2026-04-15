from __future__ import annotations

"""
Scrapling-first scrape core for v10.

Minimal v10-oriented environment block:
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
"""

import logging
import os
import re
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from importlib import import_module
from threading import Lock
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scrap_logic import (
    Article,
    Candidate,
    ExtractedPage,
    FetchResult,
    HostRateLimiter,
    HostState,
    RobotsCache,
    SourceConfig,
    _apply_verification_to_article,
    _candidate_snippet_from_context,
    _candidate_title_from_context,
    _choose_better_extracted_page,
    _debug_event,
    _discover_candidates_firecrawl,
    _effective_target_date,
    _evaluate_article,
    _extract_article_from_html,
    _extract_article_firecrawl,
    _fast_candidate_prefilter,
    _is_pdf_candidate,
    _log_article_event,
    _log_candidate_verification,
    _looks_like_section_page,
    _merge_candidate,
    _minimum_html_target,
    _published_from_node,
    _pdf_last_resort_enabled,
    _shortlist_candidates,
    _same_day_url_match,
    _source_role_value,
    candidate_score,
    contains_target_metal,
    date_from_url,
    discover_from_rss,
    discover_from_search_engines,
    discover_from_sitemaps,
    env_bool,
    env_float,
    env_int,
    extract_pdf_article,
    fast_relevance_score,
    first_nonempty,
    has_required_target_metal,
    host_of,
    looks_like_pdf,
    meta_content,
    normalize_space,
    normalize_url,
    opt_env,
    parse_isoish_date,
    prioritized_seed_urls,
    soupify,
    source_discovery_keywords,
    source_accepts_url,
    title_from_jsonld,
    truncate_text,
    verify_page_relevance_and_date,
)

try:
    from firecrawl_client import FirecrawlClient
except Exception:  # pragma: no cover
    FirecrawlClient = None  # type: ignore[assignment]


LOG = logging.getLogger("analyst.scrap_final")


@dataclass(slots=True)
class PageResponse:
    requested_url: str
    final_url: str
    status_code: int
    html: str
    text: str
    title: str
    fetch_method: str
    content_type: str
    warnings: list[str] = field(default_factory=list)


_SCRAPLING_CLASS_CACHE: dict[str, tuple[Any | None, str]] = {}
_SCRAPLING_CLASS_LOCK = Lock()


def _load_scrapling_class(name: str) -> tuple[Any | None, str]:
    with _SCRAPLING_CLASS_LOCK:
        cached = _SCRAPLING_CLASS_CACHE.get(name)
        if cached is not None:
            return cached
        try:
            module = import_module("scrapling")
            klass = getattr(module, name)
            result = (klass, "")
        except Exception as exc:  # pragma: no cover - runtime dependent
            result = (None, f"{type(exc).__name__}: {exc}")
        _SCRAPLING_CLASS_CACHE[name] = result
        return result


def scrapling_runtime_status() -> dict[str, Any]:
    fetcher_cls, fetcher_error = _load_scrapling_class("Fetcher")
    dynamic_cls, dynamic_error = _load_scrapling_class("DynamicFetcher")
    stealthy_cls, stealthy_error = _load_scrapling_class("StealthyFetcher")
    return {
        "primary_provider": _scrap_final_primary_provider(),
        "static_available": fetcher_cls is not None,
        "dynamic_available": dynamic_cls is not None,
        "stealthy_available": stealthy_cls is not None,
        "static_error": fetcher_error,
        "dynamic_error": dynamic_error,
        "stealthy_error": stealthy_error,
        "requests_fallback_available": True,
        "firecrawl_discovery_enabled": _firecrawl_discovery_enabled(),
        "firecrawl_fetch_enabled": _firecrawl_fetch_enabled(),
    }


def _scrap_final_primary_provider() -> str:
    configured = normalize_space(opt_env("SCRAP_FINAL_PRIMARY_PROVIDER", "scrapling")).lower()
    if configured and configured != "scrapling":
        LOG.warning("Unsupported SCRAP_FINAL_PRIMARY_PROVIDER=%s; forcing scrapling", configured)
    return "scrapling"


def _firecrawl_discovery_enabled() -> bool:
    return env_bool("SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY", False)


def _firecrawl_fetch_enabled() -> bool:
    return env_bool("SCRAP_FINAL_ENABLE_FIRECRAWL_FETCH", False)


def _firecrawl_any_enabled() -> bool:
    return _firecrawl_discovery_enabled() or _firecrawl_fetch_enabled()


def _external_discovery_enabled() -> bool:
    return env_bool("SCRAP_FINAL_ENABLE_EXTERNAL_DISCOVERY", False)


def _crawl_depth() -> int:
    return max(0, env_int("SCRAP_FINAL_CRAWL_DEPTH", 2))


def _max_pages_per_source() -> int:
    return max(4, env_int("SCRAP_FINAL_MAX_PAGES_PER_SOURCE", 30))


def _max_follow_per_page() -> int:
    return max(4, env_int("SCRAP_FINAL_MAX_FOLLOW_PER_PAGE", 20))


def _shortlist_per_source() -> int:
    return max(6, env_int("SCRAP_FINAL_SHORTLIST_PER_SOURCE", 15))


def _headers_to_dict(headers: Any) -> dict[str, str]:
    if isinstance(headers, dict):
        return {str(k): str(v) for k, v in headers.items()}
    out: dict[str, str] = {}
    try:
        for key, value in dict(headers or {}).items():
            out[str(key)] = str(value)
    except Exception:
        pass
    return out


def _first_header(headers: dict[str, str], *names: str) -> str:
    if not headers:
        return ""
    lowered = {str(key).lower(): str(value) for key, value in headers.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value:
            return value
    return ""


def _decode_bytes(content: bytes, headers: dict[str, str]) -> str:
    content_type = _first_header(headers, "content-type")
    encoding = "utf-8"
    if "charset=" in content_type.lower():
        encoding = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or "utf-8"
    for candidate in (encoding, "utf-8", "latin-1"):
        try:
            return content.decode(candidate, errors="replace")
        except Exception:
            continue
    return content.decode("utf-8", errors="replace")


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        soup = soupify(html)
        text = normalize_space(soup.get_text(" ", strip=True))
        return truncate_text(text, max_chars=16000)
    except Exception:
        return truncate_text(normalize_space(html), max_chars=16000)


def _extract_title_from_html(html: str) -> str:
    if not html:
        return ""
    try:
        soup = soupify(html)
        jsonld = title_from_jsonld(soup)
        return first_nonempty(
            jsonld.get("title"),
            meta_content(soup, "og:title", "twitter:title", "headline"),
            soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "",
            soup.title.get_text(" ", strip=True) if soup.title else "",
        )
    except Exception:
        return ""


def _looks_like_cloudflare_or_block(html: str) -> bool:
    low = normalize_space(html).lower()
    if not low:
        return False
    signals = (
        "checking your browser",
        "cf-browser-verification",
        "attention required",
        "access denied",
        "captcha",
        "verify you are human",
    )
    return any(signal in low for signal in signals)


class ScraplingClient:
    def __init__(
        self,
        user_agent: str,
        default_mode: str = "auto",
        http_timeout: int = 25,
        dynamic_timeout_ms: int = 18000,
        stealth_timeout_ms: int = 25000,
        session: Optional[Session] = None,
    ) -> None:
        self.user_agent = user_agent
        self.default_mode = (default_mode or "auto").strip().lower() or "auto"
        self.http_timeout = max(5, int(http_timeout))
        self.dynamic_timeout_ms = max(7000, int(dynamic_timeout_ms))
        self.stealth_timeout_ms = max(9000, int(stealth_timeout_ms))
        self.session = session or requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": opt_env("HTTP_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _page_quality_score(
        self,
        page: PageResponse,
        *,
        page_kind: str,
        source: Optional[SourceConfig] = None,
    ) -> int:
        score = 0
        if 200 <= page.status_code < 400:
            score += 4
        elif page.status_code < 500:
            score += 1
        if page.title:
            score += 3
        if page.html:
            score += min(4, len(page.html) // 1200)
        if page.text:
            score += min(4, len(page.text) // 600)
        if page_kind == "listing":
            score += min(4, page.html.lower().count("<a ") // 8)
        if page_kind == "article" and source is not None:
            body_floor = max(120, source.min_body_chars // 2)
            if len(page.text) >= body_floor:
                score += 2
        if _looks_like_cloudflare_or_block(page.html):
            score -= 6
        return score

    def _is_weak_result(
        self,
        page: PageResponse,
        *,
        page_kind: str,
        source: Optional[SourceConfig] = None,
    ) -> bool:
        if page.status_code >= 400:
            return True
        if not page.html:
            return True
        if _looks_like_cloudflare_or_block(page.html):
            return True
        if page_kind == "listing":
            link_count = page.html.lower().count("<a ")
            return len(page.html) < 1200 or (link_count < 5 and len(page.text) < 450)
        body_floor = max(120, (source.min_body_chars if source else 180) // 2)
        return (not page.title) or len(page.text) < body_floor or len(page.html) < 1500

    def _should_try_dynamic(
        self,
        page: PageResponse,
        *,
        page_kind: str,
        source: Optional[SourceConfig],
        allow_dynamic: bool,
    ) -> bool:
        if not allow_dynamic:
            return False
        if not self._is_weak_result(page, page_kind=page_kind, source=source):
            return False
        if source is not None and source.js_heavy:
            return True
        return page_kind == "listing"

    def _should_try_stealthy(
        self,
        current_best: PageResponse,
        *,
        page_kind: str,
        source: Optional[SourceConfig],
        allow_dynamic: bool,
    ) -> bool:
        if not allow_dynamic:
            return False
        if current_best.status_code in {401, 403, 429}:
            return True
        if _looks_like_cloudflare_or_block(current_best.html):
            return True
        if self._is_weak_result(current_best, page_kind=page_kind, source=source):
            return True
        return False

    def _normalize_scrapling_response(
        self,
        *,
        requested_url: str,
        response: Any,
        fetch_method: str,
        warnings: Optional[list[str]] = None,
    ) -> PageResponse:
        headers = _headers_to_dict(getattr(response, "headers", {}))
        body = getattr(response, "body", b"")
        if isinstance(body, str):
            body_bytes = body.encode("utf-8", errors="replace")
        else:
            body_bytes = bytes(body or b"")
        html = _decode_bytes(body_bytes, headers)
        final_url = normalize_url(str(getattr(response, "url", "") or requested_url))
        title = _extract_title_from_html(html)
        return PageResponse(
            requested_url=normalize_url(requested_url),
            final_url=final_url,
            status_code=int(getattr(response, "status", 0) or 0),
            html=html,
            text=_html_to_text(html),
            title=title,
            fetch_method=fetch_method,
            content_type=_first_header(headers, "content-type"),
            warnings=list(warnings or []),
        )

    def _fetch_via_requests(self, url: str) -> PageResponse:
        warnings: list[str] = []
        started = time.monotonic()
        try:
            response = self.session.get(url, timeout=self.http_timeout, allow_redirects=True)
            elapsed = round(time.monotonic() - started, 2)
            headers = {str(k): str(v) for k, v in response.headers.items()}
            html = response.text or _decode_bytes(response.content, headers)
            LOG.info(
                "Scrapling fetch ok | mode=requests_fallback | url=%s | status=%s | elapsed_s=%s",
                url,
                response.status_code,
                elapsed,
            )
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(response.url or url),
                status_code=int(response.status_code or 0),
                html=html,
                text=_html_to_text(html),
                title=_extract_title_from_html(html),
                fetch_method="scrapling_http_requests_fallback",
                content_type=_first_header(headers, "content-type"),
                warnings=warnings,
            )
        except Exception as exc:
            warnings.append(f"requests_fallback_error:{type(exc).__name__}")
            LOG.warning("Scrapling fetch failed | mode=requests_fallback | url=%s | error=%s", url, exc)
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                html="",
                text="",
                title="",
                fetch_method="scrapling_http_requests_fallback",
                content_type="",
                warnings=warnings,
            )

    def _fetch_http(self, url: str) -> PageResponse:
        fetcher_cls, import_error = _load_scrapling_class("Fetcher")
        if fetcher_cls is None:
            fallback = self._fetch_via_requests(url)
            fallback.warnings.append(f"scrapling_fetcher_unavailable:{import_error}")
            return fallback
        started = time.monotonic()
        try:
            response = fetcher_cls.get(
                url,
                timeout=self.http_timeout,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept-Language": opt_env("HTTP_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
                },
                follow_redirects=True,
            )
            elapsed = round(time.monotonic() - started, 2)
            LOG.info(
                "Scrapling fetch ok | mode=http | url=%s | status=%s | elapsed_s=%s",
                url,
                getattr(response, "status", 0),
                elapsed,
            )
            return self._normalize_scrapling_response(requested_url=url, response=response, fetch_method="scrapling_http")
        except Exception as exc:  # pragma: no cover - runtime dependent
            LOG.warning("Scrapling fetch failed | mode=http | url=%s | error=%s", url, exc)
            fallback = self._fetch_via_requests(url)
            fallback.warnings.append(f"scrapling_http_error:{type(exc).__name__}")
            return fallback

    def _fetch_dynamic(self, url: str) -> PageResponse:
        fetcher_cls, import_error = _load_scrapling_class("DynamicFetcher")
        if fetcher_cls is None:
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                html="",
                text="",
                title="",
                fetch_method="scrapling_dynamic",
                content_type="",
                warnings=[f"scrapling_dynamic_unavailable:{import_error}"],
            )
        started = time.monotonic()
        try:
            response = fetcher_cls.fetch(
                url,
                headless=True,
                disable_resources=True,
                block_ads=True,
                timeout=self.dynamic_timeout_ms,
                wait=1200,
                load_dom=True,
                useragent=self.user_agent,
            )
            elapsed = round(time.monotonic() - started, 2)
            LOG.info(
                "Scrapling fetch ok | mode=dynamic | url=%s | status=%s | elapsed_s=%s",
                url,
                getattr(response, "status", 0),
                elapsed,
            )
            return self._normalize_scrapling_response(requested_url=url, response=response, fetch_method="scrapling_dynamic")
        except Exception as exc:  # pragma: no cover - runtime dependent
            LOG.warning("Scrapling fetch failed | mode=dynamic | url=%s | error=%s", url, exc)
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                html="",
                text="",
                title="",
                fetch_method="scrapling_dynamic",
                content_type="",
                warnings=[f"scrapling_dynamic_error:{type(exc).__name__}"],
            )

    def _fetch_stealthy(self, url: str) -> PageResponse:
        fetcher_cls, import_error = _load_scrapling_class("StealthyFetcher")
        if fetcher_cls is None:
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                html="",
                text="",
                title="",
                fetch_method="scrapling_stealthy",
                content_type="",
                warnings=[f"scrapling_stealthy_unavailable:{import_error}"],
            )
        started = time.monotonic()
        try:
            response = fetcher_cls.fetch(
                url,
                headless=True,
                disable_resources=True,
                block_ads=True,
                timeout=self.stealth_timeout_ms,
                wait=1200,
                load_dom=True,
                useragent=self.user_agent,
                google_search=False,
            )
            elapsed = round(time.monotonic() - started, 2)
            LOG.info(
                "Scrapling fetch ok | mode=stealthy | url=%s | status=%s | elapsed_s=%s",
                url,
                getattr(response, "status", 0),
                elapsed,
            )
            return self._normalize_scrapling_response(requested_url=url, response=response, fetch_method="scrapling_stealthy")
        except Exception as exc:  # pragma: no cover - runtime dependent
            LOG.warning("Scrapling fetch failed | mode=stealthy | url=%s | error=%s", url, exc)
            return PageResponse(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                html="",
                text="",
                title="",
                fetch_method="scrapling_stealthy",
                content_type="",
                warnings=[f"scrapling_stealthy_error:{type(exc).__name__}"],
            )

    def fetch_page(
        self,
        url: str,
        *,
        mode: Optional[str] = None,
        page_kind: str = "article",
        source: Optional[SourceConfig] = None,
        allow_dynamic: bool = True,
    ) -> PageResponse:
        selected_mode = (mode or self.default_mode or "auto").strip().lower()
        if selected_mode not in {"auto", "http", "dynamic", "stealthy"}:
            selected_mode = "auto"

        if selected_mode == "http":
            return self._fetch_http(url)
        if selected_mode == "dynamic":
            page = self._fetch_dynamic(url)
            if page.status_code >= 400 and page.fetch_method == "scrapling_dynamic":
                fallback = self._fetch_http(url)
                fallback.warnings.extend(page.warnings)
                return fallback
            return page
        if selected_mode == "stealthy":
            page = self._fetch_stealthy(url)
            if page.status_code >= 400 and page.fetch_method == "scrapling_stealthy":
                fallback = self._fetch_http(url)
                fallback.warnings.extend(page.warnings)
                return fallback
            return page

        http_page = self._fetch_http(url)
        best_page = http_page
        if self._should_try_dynamic(http_page, page_kind=page_kind, source=source, allow_dynamic=allow_dynamic):
            dynamic_page = self._fetch_dynamic(url)
            if self._page_quality_score(dynamic_page, page_kind=page_kind, source=source) > self._page_quality_score(best_page, page_kind=page_kind, source=source):
                best_page = dynamic_page
        if self._should_try_stealthy(best_page, page_kind=page_kind, source=source, allow_dynamic=allow_dynamic):
            stealthy_page = self._fetch_stealthy(url)
            if self._page_quality_score(stealthy_page, page_kind=page_kind, source=source) > self._page_quality_score(best_page, page_kind=page_kind, source=source):
                best_page = stealthy_page
        return best_page

    def fetch_listing_page(
        self,
        url: str,
        *,
        mode: Optional[str] = None,
        source: Optional[SourceConfig] = None,
        allow_dynamic: bool = True,
    ) -> PageResponse:
        return self.fetch_page(url, mode=mode, page_kind="listing", source=source, allow_dynamic=allow_dynamic)

    def fetch_article_page(
        self,
        url: str,
        *,
        mode: Optional[str] = None,
        source: Optional[SourceConfig] = None,
        allow_dynamic: bool = True,
    ) -> PageResponse:
        return self.fetch_page(url, mode=mode, page_kind="article", source=source, allow_dynamic=allow_dynamic)

    def fetch_binary(self, url: str) -> tuple[bytes, FetchResult]:
        started = time.monotonic()
        warnings: list[str] = []
        try:
            response = self.session.get(url, timeout=self.http_timeout, allow_redirects=True)
            elapsed = round(time.monotonic() - started, 2)
            LOG.info("Scrapling binary fetch ok | url=%s | status=%s | elapsed_s=%s", url, response.status_code, elapsed)
            headers = {str(k): str(v) for k, v in response.headers.items()}
            fetch_result = FetchResult(
                requested_url=normalize_url(url),
                final_url=normalize_url(response.url or url),
                status_code=int(response.status_code or 0),
                text=_decode_bytes(response.content, headers) if "text" in _first_header(headers, "content-type").lower() or "xml" in _first_header(headers, "content-type").lower() else "",
                content_type=_first_header(headers, "content-type"),
                method="scrapling_binary_requests",
                warnings=warnings,
            )
            return bytes(response.content or b""), fetch_result
        except Exception as exc:
            warnings.append(f"binary_fetch_error:{type(exc).__name__}")
            LOG.warning("Scrapling binary fetch failed | url=%s | error=%s", url, exc)
            return b"", FetchResult(
                requested_url=normalize_url(url),
                final_url=normalize_url(url),
                status_code=599,
                text="",
                content_type="",
                method="scrapling_binary_requests",
                warnings=warnings,
            )


class ScraplingFetcherAdapter:
    def __init__(
        self,
        user_agent: str,
        *,
        default_mode: Optional[str] = None,
        http_timeout: Optional[int] = None,
        dynamic_timeout_ms: Optional[int] = None,
        stealth_timeout_ms: Optional[int] = None,
        per_host_delay: Optional[float] = None,
    ) -> None:
        self.user_agent = user_agent
        self.default_mode = (default_mode or opt_env("SCRAPLING_MODE", "auto")).strip().lower() or "auto"
        self.client = ScraplingClient(
            user_agent=user_agent,
            default_mode=self.default_mode,
            http_timeout=http_timeout or env_int("SCRAPLING_HTTP_TIMEOUT", 25),
            dynamic_timeout_ms=dynamic_timeout_ms or env_int("SCRAPLING_DYNAMIC_TIMEOUT_MS", 25000),
            stealth_timeout_ms=stealth_timeout_ms or env_int("SCRAPLING_STEALTH_TIMEOUT_MS", 32000),
        )
        self.robots = RobotsCache(user_agent=user_agent)
        self.rate = HostRateLimiter(default_delay=per_host_delay or env_float("SCRAPE_PER_HOST_DELAY", 1.0))
        self.hosts: dict[str, HostState] = defaultdict(HostState)

    def close(self) -> None:
        self.client.close()

    def _mark_failure(self, host: str, status_code: int) -> None:
        state = self.hosts[host]
        state.failures += 1
        state.last_status = status_code
        if status_code in {401, 403, 429}:
            from datetime import timedelta

            from scrap_logic import utc_now

            state.blocked_until = utc_now() + timedelta(minutes=min(20, 2 * state.failures))
            if status_code == 403:
                state.sitemap_disabled = True

    def _mark_success(self, host: str) -> None:
        state = self.hosts[host]
        state.failures = 0
        state.blocked_until = None
        state.last_status = 200

    def fetch(self, url: str, allow_browser_fallback: bool = True, respect_robots: bool = True) -> FetchResult:
        requested_url = normalize_url(url)
        host = host_of(requested_url)
        if respect_robots and requested_url and not self.robots.can_fetch(requested_url):
            return FetchResult(
                requested_url=requested_url,
                final_url=requested_url,
                status_code=999,
                text="",
                content_type="",
                method="robots_blocked",
                blocked=True,
                warnings=["robots_disallow"],
            )
        if self.hosts[host].is_paused():
            return FetchResult(
                requested_url=requested_url,
                final_url=requested_url,
                status_code=429,
                text="",
                content_type="",
                method="host_paused",
                blocked=True,
                warnings=["host_temporarily_paused"],
            )

        self.rate.wait(host)
        page = self.client.fetch_listing_page(
            requested_url,
            mode="http" if not allow_browser_fallback else self.default_mode,
            source=None,
            allow_dynamic=allow_browser_fallback,
        )
        if page.status_code >= 400:
            self._mark_failure(host, page.status_code)
        else:
            self._mark_success(host)
        return FetchResult(
            requested_url=requested_url,
            final_url=page.final_url,
            status_code=page.status_code,
            text=page.html,
            content_type=page.content_type,
            method=page.fetch_method,
            warnings=list(page.warnings),
        )

    def fetch_bytes(self, url: str, respect_robots: bool = True) -> tuple[bytes, FetchResult]:
        requested_url = normalize_url(url)
        host = host_of(requested_url)
        if respect_robots and requested_url and not self.robots.can_fetch(requested_url):
            return b"", FetchResult(
                requested_url=requested_url,
                final_url=requested_url,
                status_code=999,
                text="",
                content_type="",
                method="robots_blocked",
                blocked=True,
                warnings=["robots_disallow"],
            )
        if self.hosts[host].is_paused():
            return b"", FetchResult(
                requested_url=requested_url,
                final_url=requested_url,
                status_code=429,
                text="",
                content_type="",
                method="host_paused",
                blocked=True,
                warnings=["host_temporarily_paused"],
            )

        self.rate.wait(host)
        raw, result = self.client.fetch_binary(requested_url)
        if result.status_code >= 400:
            self._mark_failure(host, result.status_code)
        else:
            self._mark_success(host)
        return raw, result


def _source_allowed_host(source: SourceConfig, url: str) -> bool:
    host = host_of(url)
    if not host:
        return False
    allowed_hosts = [item for item in (source.allowed_hosts or []) if item]
    if allowed_hosts:
        return any(host == allowed or host.endswith("." + allowed) for allowed in allowed_hosts)
    seed_hosts = [host_of(seed) for seed in (source.seed_urls or []) if host_of(seed)]
    return not seed_hosts or host in seed_hosts


def _non_content_path(url: str) -> bool:
    low = normalize_url(url).lower()
    bad_tokens = (
        "/login",
        "/register",
        "/signup",
        "/sign-in",
        "/contact",
        "/about",
        "/privacy",
        "/terms",
        "/careers",
        "/download",
        "/downloads",
        "/app",
        "/apps",
        "/podcast",
        "/podcasts",
        "/video",
        "/videos",
    )
    return any(token in low for token in bad_tokens)


def _is_pagination_link(url: str, text: str) -> bool:
    combined = f"{normalize_url(url)} {normalize_space(text)}".lower()
    if any(token in combined for token in ("?page=", "&page=", "/page/", "next", "older", "more", "load more")):
        return True
    return bool(re.search(r"(?:[?&](?:p|pg|page)=\d+|/page/\d+/?)", combined))


def _traversal_link_score(
    source: SourceConfig,
    url: str,
    text: str,
    target_date: Optional[str],
) -> int:
    low = f"{normalize_space(text)} {normalize_url(url)}".lower()
    score = 0
    keywords = source_discovery_keywords(source)
    if _is_pagination_link(url, text):
        score += 6
    if _looks_like_section_page(url):
        score += 4
    if _same_day_url_match(url, target_date):
        score += 4
    if has_required_target_metal(low):
        score += 3
    if any(term in low for term in keywords):
        score += 3
    if any(hint in low for hint in (source.include_hints or [])):
        score += 2
    if "news" in low or "analysis" in low or "market" in low or "report" in low:
        score += 2
    if source_accepts_url(source, url):
        score -= 4
    if _non_content_path(url):
        score -= 10
    return score


def _should_follow_internal_link(
    source: SourceConfig,
    url: str,
    anchor_text: str,
    target_date: Optional[str],
) -> bool:
    if not url or not _source_allowed_host(source, url):
        return False
    if looks_like_pdf(url):
        return False
    if _non_content_path(url):
        return False
    if any(hint in normalize_url(url).lower() for hint in (source.exclude_hints or []) if hint and not _is_pagination_link(url, anchor_text)):
        return False
    return _traversal_link_score(source, url, anchor_text, target_date) >= 4


def _build_candidate_from_anchor(
    source: SourceConfig,
    page_url: str,
    anchor: Any,
    target_date: Optional[str],
    method: str,
) -> Optional[Candidate]:
    href = normalize_url(urljoin(page_url, anchor.get("href") or ""))
    if not href or not source_accepts_url(source, href):
        return None
    parent = anchor.find_parent(["article", "li", "div", "section"])
    title = _candidate_title_from_context(anchor, parent)
    snippet = _candidate_snippet_from_context(anchor, parent)
    published = _published_from_node(parent)
    score = candidate_score(title, href, source_discovery_keywords(source), context_text=snippet)
    if has_required_target_metal(f"{title}\n{snippet}\n{href}"):
        score += 2
    if _same_day_url_match(href, target_date):
        score += 3
    if _is_pagination_link(href, title):
        score -= 4
    return Candidate(
        source_name=source.name,
        url=href,
        listing_url=page_url,
        title_hint=title,
        published_hint=published,
        snippet_hint=snippet,
        method=method,
        score=score,
    )


def _extract_candidates_from_listing_html(
    source: SourceConfig,
    page_url: str,
    html: str,
    target_date: Optional[str],
    method: str,
) -> list[Candidate]:
    soup = soupify(html)
    local: dict[str, Candidate] = {}
    for anchor in soup.find_all("a", href=True):
        candidate = _build_candidate_from_anchor(source, page_url, anchor, target_date, method)
        if candidate is None:
            continue
        previous = local.get(candidate.url)
        if previous is None or candidate.score > previous.score:
            local[candidate.url] = candidate
    return sorted(local.values(), key=lambda item: item.score, reverse=True)


def _extract_follow_links(
    source: SourceConfig,
    page_url: str,
    html: str,
    target_date: Optional[str],
    max_follow_per_page: int,
) -> list[str]:
    soup = soupify(html)
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(page_url, anchor.get("href") or ""))
        if not href or href in seen:
            continue
        text = normalize_space(anchor.get_text(" ", strip=True))
        if not _should_follow_internal_link(source, href, text, target_date):
            continue
        seen.add(href)
        ranked.append((_traversal_link_score(source, href, text, target_date), href))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [href for _, href in ranked[:max_follow_per_page]]


def discover_from_seed_urls(
    source: SourceConfig,
    fetcher: ScraplingFetcherAdapter,
    target_date: Optional[str],
) -> list[Candidate]:
    seeds = prioritized_seed_urls(source)[: max(1, min(6, _max_pages_per_source()))]
    found: dict[str, Candidate] = {}
    for seed_url in seeds:
        page = fetcher.client.fetch_listing_page(seed_url, source=source, mode=fetcher.default_mode, allow_dynamic=True)
        LOG.info(
            "Seed page fetched | source=%s | url=%s | fetch_mode=%s | status=%s",
            source.name,
            seed_url,
            page.fetch_method,
            page.status_code,
        )
        if page.status_code >= 400 or not page.html:
            continue
        for candidate in _extract_candidates_from_listing_html(source, page.final_url or seed_url, page.html, target_date, method="scrapling_seed"):
            _merge_candidate(found, candidate)
    return sorted(found.values(), key=lambda item: item.score, reverse=True)


def discover_from_scrapling_traversal(
    source: SourceConfig,
    fetcher: ScraplingFetcherAdapter,
    target_date: Optional[str],
) -> list[Candidate]:
    crawl_depth = _crawl_depth()
    max_pages = _max_pages_per_source()
    max_follow = _max_follow_per_page()
    queue: deque[tuple[str, int, str]] = deque(
        (normalize_url(seed), 0, "seed") for seed in prioritized_seed_urls(source) if normalize_url(seed)
    )
    visited_pages: set[str] = set()
    found: dict[str, Candidate] = {}

    while queue and len(visited_pages) < max_pages:
        page_url, depth, via = queue.popleft()
        if not page_url or page_url in visited_pages:
            continue
        if not _source_allowed_host(source, page_url) or _non_content_path(page_url):
            continue
        visited_pages.add(page_url)
        page = fetcher.client.fetch_listing_page(page_url, source=source, mode=fetcher.default_mode, allow_dynamic=True)
        LOG.info(
            "Traversal page | source=%s | depth=%s | via=%s | url=%s | fetch_mode=%s | status=%s",
            source.name,
            depth,
            via,
            page_url,
            page.fetch_method,
            page.status_code,
        )
        if page.status_code >= 400 or not page.html:
            continue

        for candidate in _extract_candidates_from_listing_html(source, page.final_url or page_url, page.html, target_date, method=f"scrapling_traversal_d{depth}"):
            _merge_candidate(found, candidate)

        if depth >= crawl_depth:
            continue

        for next_url in _extract_follow_links(source, page.final_url or page_url, page.html, target_date, max_follow):
            if next_url not in visited_pages:
                queue.append((next_url, depth + 1, page.fetch_method))

    LOG.info(
        "Traversal summary | source=%s | pages_visited=%s | candidates=%s | crawl_depth=%s | max_pages=%s | max_follow_per_page=%s",
        source.name,
        len(visited_pages),
        len(found),
        crawl_depth,
        max_pages,
        max_follow,
    )
    _debug_event(
        "scrap_final_traversal_done",
        source,
        pages_visited=len(visited_pages),
        candidates=len(found),
        crawl_depth=crawl_depth,
        max_pages=max_pages,
        max_follow_per_page=max_follow,
    )
    return sorted(found.values(), key=lambda item: item.score, reverse=True)


def _build_firecrawl_client() -> FirecrawlClient | None:
    if FirecrawlClient is None:
        return None
    if not _firecrawl_any_enabled():
        return None
    api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not api_key:
        return None
    try:
        return FirecrawlClient(
            api_key=api_key,
            timeout=env_int("FIRECRAWL_SCRAPE_TIMEOUT", 45),
            max_retries=env_int("FIRECRAWL_MAX_RETRIES", 3),
        )
    except Exception as exc:
        LOG.warning("Firecrawl fallback disabled for scrap_final | error=%s", exc)
        return None


def _firecrawl_fetch_fallback(
    source: SourceConfig,
    candidate: Candidate,
    firecrawl_client: FirecrawlClient | None,
    target_date: Optional[str],
    current_result: ExtractedPage | None,
) -> ExtractedPage | None:
    if firecrawl_client is None or not _firecrawl_fetch_enabled():
        return current_result
    if current_result is not None and current_result.article is not None:
        return current_result
    try:
        firecrawl_result = _extract_article_firecrawl(source, candidate, firecrawl_client)
        if firecrawl_result.article is not None:
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
            if firecrawl_result.article is not None:
                firecrawl_result.article.warnings.append("firecrawl_emergency_fetch_fallback")
        LOG.info(
            "Firecrawl fetch fallback used | source=%s | url=%s | article_found=%s",
            source.name,
            candidate.url,
            firecrawl_result.article is not None,
        )
        _debug_event("scrap_final_firecrawl_fetch_fallback", source, url=candidate.url, article_found=firecrawl_result.article is not None)
        return _choose_better_extracted_page(current_result, firecrawl_result) or firecrawl_result
    except Exception as exc:
        LOG.warning("Firecrawl fetch fallback failed | source=%s | url=%s | error=%s", source.name, candidate.url, exc)
        _debug_event("scrap_final_firecrawl_fetch_failed", source, url=candidate.url, error=str(exc))
        return current_result


def discover_candidates(
    source: SourceConfig,
    fetcher: ScraplingFetcherAdapter,
    target_date: Optional[str] = None,
    firecrawl_client: FirecrawlClient | None = None,
) -> list[Candidate]:
    target_date = _effective_target_date(target_date)
    merged: dict[str, Candidate] = {}
    stage_counts: dict[str, int] = {}

    for stage_name, strategy in (
        ("seed_urls", lambda: discover_from_seed_urls(source, fetcher, target_date)),
        ("rss", lambda: discover_from_rss(source, fetcher)),
        ("sitemaps", lambda: discover_from_sitemaps(source, fetcher)),
        ("scrapling_traversal", lambda: discover_from_scrapling_traversal(source, fetcher, target_date)),
    ):
        try:
            candidates = strategy()
            stage_counts[stage_name] = len(candidates)
        except Exception as exc:
            LOG.warning("Discovery strategy failed | source=%s | strategy=%s | error=%s", source.name, stage_name, exc)
            continue
        for candidate in candidates:
            _merge_candidate(merged, candidate)

    search_threshold = env_int("SCRAP_FINAL_SEARCH_DISCOVERY_THRESHOLD", 14)
    source_role = _source_role_value(source)
    if (
        _external_discovery_enabled()
        and target_date
        and len(merged) < search_threshold
        and source_role in {"narrative_news", "market_data"}
    ):
        try:
            candidates = discover_from_search_engines(source, fetcher, target_date)
            stage_counts["external_search"] = len(candidates)
            for candidate in candidates:
                _merge_candidate(merged, candidate)
        except Exception as exc:
            LOG.warning("Search discovery failed | source=%s | error=%s", source.name, exc)

    firecrawl_threshold = env_int("SCRAP_FINAL_FIRECRAWL_DISCOVERY_THRESHOLD", 12)
    if firecrawl_client is not None and _firecrawl_discovery_enabled() and len(merged) < firecrawl_threshold:
        try:
            firecrawl_candidates = _discover_candidates_firecrawl(source, firecrawl_client, target_date=target_date)
            stage_counts["firecrawl_discovery"] = len(firecrawl_candidates)
            for candidate in firecrawl_candidates:
                _merge_candidate(merged, candidate)
        except Exception as exc:
            LOG.warning("Firecrawl discovery failed | source=%s | error=%s", source.name, exc)

    ordered = sorted(merged.values(), key=lambda item: item.score, reverse=True)
    methods: dict[str, int] = {}
    for candidate in ordered:
        methods[candidate.method] = methods.get(candidate.method, 0) + 1
    LOG.info(
        "Discovered %s candidates for %s | stages=%s | methods=%s",
        len(ordered),
        source.name,
        stage_counts,
        methods,
    )
    _debug_event(
        "scrap_final_discovery_done",
        source,
        count=len(ordered),
        methods=methods,
        stages=stage_counts,
        provider=_scrap_final_primary_provider(),
    )
    return ordered


def extract_article_page(
    source: SourceConfig,
    candidate: Candidate,
    fetcher: ScraplingFetcherAdapter,
    *,
    allow_pdf: bool = True,
    target_date: Optional[str] = None,
    firecrawl_client: FirecrawlClient | None = None,
) -> ExtractedPage:
    if looks_like_pdf(candidate.url):
        if not allow_pdf:
            return ExtractedPage(article=None, final_url=normalize_url(candidate.url))
        article = extract_pdf_article(source, candidate, fetcher)
        verification = verify_page_relevance_and_date(article, "", candidate, target_date=target_date, final_url=candidate.url, source=source)
        article = _apply_verification_to_article(article, verification, source=source)
        return ExtractedPage(article=article, final_url=normalize_url(candidate.url), verification=verification)

    page = fetcher.client.fetch_article_page(
        candidate.url,
        source=source,
        mode=fetcher.default_mode,
        allow_dynamic=True,
    )
    if "pdf" in (page.content_type or "").lower():
        if not allow_pdf:
            result = ExtractedPage(article=None, raw_html=page.html, final_url=page.final_url)
            return _firecrawl_fetch_fallback(source, candidate, firecrawl_client, target_date, result) or result
        article = extract_pdf_article(source, candidate, fetcher)
        verification = verify_page_relevance_and_date(article, page.html, candidate, target_date=target_date, final_url=page.final_url, source=source)
        article = _apply_verification_to_article(article, verification, source=source)
        return ExtractedPage(article=article, raw_html=page.html, final_url=page.final_url, verification=verification)

    if page.status_code >= 400 or not page.html:
        result = ExtractedPage(article=None, raw_html=page.html, final_url=page.final_url)
        return _firecrawl_fetch_fallback(source, candidate, firecrawl_client, target_date, result) or result

    article = _extract_article_from_html(source, candidate, page.html, page.final_url, page.fetch_method, page.warnings)
    verification = verify_page_relevance_and_date(
        article,
        page.html,
        candidate,
        target_date=target_date,
        final_url=page.final_url,
        source=source,
    )
    article = _apply_verification_to_article(article, verification, source=source)
    if article is not None:
        article.warnings.append(f"scrapling_fetch_method:{page.fetch_method}")
    result = ExtractedPage(
        article=article,
        raw_html=page.html,
        final_url=page.final_url,
        verification=verification,
    )
    return _firecrawl_fetch_fallback(source, candidate, firecrawl_client, target_date, result) or result


def scrape_source(
    source: SourceConfig,
    fetcher: ScraplingFetcherAdapter,
    *,
    max_candidates: int = 80,
    max_articles: int = 8,
    target_date: Optional[str] = None,
    firecrawl_client: FirecrawlClient | None = None,
) -> list[Article]:
    target_date = _effective_target_date(target_date)
    os.environ.setdefault("MAX_SHORTLIST_PER_SOURCE", str(_shortlist_per_source()))
    source_started = time.monotonic()
    source_role = _source_role_value(source)
    default_time_budget = 75 if source_role == "narrative_news" else 55
    source_time_budget = env_int("SOURCE_TIME_BUDGET_SECONDS", default_time_budget)

    candidates = discover_candidates(source, fetcher, target_date=target_date, firecrawl_client=firecrawl_client)[:max_candidates]
    shortlisted, shortlist_debug = _shortlist_candidates(source, candidates, target_date, max_candidates, dynamic_enabled=bool(source.js_heavy))

    prefiltered: list[Candidate] = []
    prefilter_rejected = 0
    prefilter_kept = 0
    prefilter_floor = min(
        len(shortlisted),
        env_int("FAST_PREFILTER_MIN_KEEP", 6 if source_role == "narrative_news" else 4),
    )
    for candidate in shortlisted:
        accepted_fast, fast_score, fast_reasons = _fast_candidate_prefilter(source, candidate, target_date)
        if accepted_fast:
            candidate.score = max(candidate.score, fast_score)
            prefiltered.append(candidate)
            prefilter_kept += 1
            LOG.info("Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s", candidate.url, True, fast_score, fast_reasons)
            continue
        if len(prefiltered) < prefilter_floor and candidate.score >= env_int("FAST_PREFILTER_FLOOR_SCORE", 6):
            candidate.score = max(candidate.score, fast_score)
            prefiltered.append(candidate)
            prefilter_kept += 1
            LOG.info("Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s", candidate.url, True, fast_score, fast_reasons + ["floor_keep"])
            continue
        prefilter_rejected += 1
        LOG.info("Candidate prefilter | url=%s | accepted=%s | score=%s | reasons=%s", candidate.url, False, fast_score, fast_reasons)
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
        "scrap_final_shortlist_done",
        source,
        discovered=len(candidates),
        shortlisted=len(shortlisted),
        target_date=target_date,
        shortlist_debug=shortlist_debug,
    )

    articles: list[Article] = []
    seen: set[str] = set()
    max_attempts = env_int("MAX_EXTRACTION_ATTEMPTS_PER_SOURCE", max(max_articles * 2, 10))
    attempts = 0
    minimum_html_articles = _minimum_html_target(max_articles)

    html_candidates = [candidate for candidate in shortlisted if not _is_pdf_candidate(candidate)]
    pdf_candidates = [candidate for candidate in shortlisted if _is_pdf_candidate(candidate)]
    ordered_candidates = html_candidates + ([] if _pdf_last_resort_enabled() else pdf_candidates)

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
            target_date=target_date,
            firecrawl_client=firecrawl_client,
        )
        _log_candidate_verification(candidate, page_result)
        article = page_result.article
        if article is None:
            _debug_event("scrap_final_extract_none", source, url=candidate.url, candidate_method=candidate.method)
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
                "scrap_final_article_rejected",
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
            "scrap_final_article_accepted",
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
                target_date=target_date,
                firecrawl_client=firecrawl_client,
            )
            _log_candidate_verification(candidate, page_result)
            article = page_result.article
            if article is None:
                _debug_event("scrap_final_pdf_extract_none", source, url=candidate.url, candidate_method=candidate.method)
                continue
            _log_article_event("extracted", article, score=article.score, reasons=[f"candidate_method:{candidate.method}", "pdf_fallback_attempt"])

            decision = _evaluate_article(source, article, target_date, verification=page_result.verification)
            if not decision.accepted:
                _log_article_event("discarded", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)
                continue

            article.score = max(article.score, decision.score)
            article.warnings.append("pdf_fallback_attempt")
            articles.append(article)
            _log_article_event("accepted", article, score=decision.score, reasons=decision.reasons, reject_reasons=decision.reject_reasons)

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
    source_workers = max(1, min(len(sources), env_int("SCRAPE_SOURCE_WORKERS", 5)))
    default_max_candidates = env_int("SCRAPE_MAX_CANDIDATES", 80)
    runtime_status = scrapling_runtime_status()
    LOG.info(
        "Scrapling provider active | provider=%s | target_date=%s | source_workers=%s | max_candidates=%s | crawl_depth=%s | max_pages_per_source=%s | max_follow_per_page=%s | firecrawl_discovery=%s | firecrawl_fetch=%s | runtime=%s",
        _scrap_final_primary_provider(),
        target_date,
        source_workers,
        default_max_candidates,
        _crawl_depth(),
        _max_pages_per_source(),
        _max_follow_per_page(),
        _firecrawl_discovery_enabled(),
        _firecrawl_fetch_enabled(),
        runtime_status,
    )

    def _run_source(source: SourceConfig) -> list[Article]:
        local_fetcher = ScraplingFetcherAdapter(
            user_agent=user_agent,
            default_mode=opt_env("SCRAPLING_MODE", "auto"),
            http_timeout=env_int("SCRAPLING_HTTP_TIMEOUT", 25),
            dynamic_timeout_ms=env_int("SCRAPLING_DYNAMIC_TIMEOUT_MS", 18000),
            stealth_timeout_ms=env_int("SCRAPLING_STEALTH_TIMEOUT_MS", 25000),
            per_host_delay=env_float("SCRAPE_PER_HOST_DELAY", 1.0),
        )
        local_firecrawl_client = _build_firecrawl_client()
        try:
            return scrape_source(
                source=source,
                fetcher=local_fetcher,
                max_candidates=default_max_candidates,
                max_articles=per_source_limit,
                target_date=target_date,
                firecrawl_client=local_firecrawl_client,
            )
        finally:
            try:
                local_fetcher.close()
            finally:
                if local_firecrawl_client is not None:
                    local_firecrawl_client.close()

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
                    _debug_event("scrap_final_source_done", source, accepted=len(articles))
                except Exception as exc:
                    LOG.exception("Source failed | source=%s | error=%s", source.name, exc)
                    _debug_event("scrap_final_source_failed", source, error=str(exc))
    else:
        for source in sources:
            try:
                articles = _run_source(source)
                all_articles.extend(articles)
                LOG.info("Accepted %s articles for %s", len(articles), source.name)
                _debug_event("scrap_final_source_done", source, accepted=len(articles))
                if len(all_articles) >= max_articles_total:
                    break
            except Exception as exc:
                LOG.exception("Source failed | source=%s | error=%s", source.name, exc)
                _debug_event("scrap_final_source_failed", source, error=str(exc))

    deduped: dict[str, Article] = {}
    for article in all_articles:
        key = normalize_url(article.source_url)
        previous = deduped.get(key)
        if previous is None or article.score > previous.score:
            deduped[key] = article

    ordered = sorted(deduped.values(), key=lambda article: (article.published_at, article.score), reverse=True)
    return ordered[:max_articles_total]


__all__ = [
    "Article",
    "Candidate",
    "PageResponse",
    "ScraplingClient",
    "ScraplingFetcherAdapter",
    "SourceConfig",
    "discover_candidates",
    "extract_article_page",
    "scrape_source",
    "scrape_sources",
    "scrapling_runtime_status",
]
