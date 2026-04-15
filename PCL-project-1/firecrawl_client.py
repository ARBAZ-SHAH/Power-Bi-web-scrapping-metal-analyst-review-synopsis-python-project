from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

import requests
from requests import Session


LOG = logging.getLogger("analyst.firecrawl")


class FirecrawlClientError(RuntimeError):
    pass


def _normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


class FirecrawlClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.firecrawl.dev/v2",
        timeout: int = 45,
        max_retries: int = 3,
        session: Optional[Session] = None,
    ) -> None:
        self.api_key = (api_key or os.getenv("FIRECRAWL_API_KEY", "")).strip()
        if not self.api_key:
            raise FirecrawlClientError("Missing FIRECRAWL_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = max(5, int(timeout))
        self.max_retries = max(1, int(max_retries))
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": os.getenv(
                    "HTTP_USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36",
                ),
            }
        )

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _request(self, path: str, payload: dict[str, Any], timeout: Optional[int] = None) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_timeout = max(5, int(timeout or self.timeout))
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            started = time.monotonic()
            try:
                response = self.session.post(url, json=payload, timeout=request_timeout)
                elapsed = round(time.monotonic() - started, 2)
                if response.status_code >= 500:
                    raise FirecrawlClientError(
                        f"Firecrawl HTTP {response.status_code} for {path}: {response.text[:400]}"
                    )
                if response.status_code >= 400:
                    raise FirecrawlClientError(
                        f"Firecrawl HTTP {response.status_code} for {path}: {response.text[:800]}"
                    )
                data = response.json()
                LOG.info("Firecrawl %s ok | attempt=%s | elapsed_s=%s", path, attempt, elapsed)
                return data
            except (requests.RequestException, ValueError, FirecrawlClientError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                sleep_for = min(6, attempt * 1.5)
                LOG.warning(
                    "Firecrawl %s failed | attempt=%s/%s | error=%s | retry_in_s=%s",
                    path,
                    attempt,
                    self.max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
        raise FirecrawlClientError(f"Firecrawl request failed for {path}: {last_error}")

    @staticmethod
    def _normalize_candidate(item: Any) -> dict[str, Any]:
        if isinstance(item, str):
            return {"url": item, "title": "", "published": "", "snippet": "", "metadata": {}}
        if not isinstance(item, dict):
            return {"url": "", "title": "", "published": "", "snippet": "", "metadata": {}}
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        return {
            "url": _normalize_space(
                item.get("url")
                or item.get("sourceURL")
                or metadata.get("sourceURL")
            ),
            "title": _normalize_space(item.get("title") or metadata.get("title")),
            "published": _normalize_space(
                item.get("published")
                or item.get("publishedAt")
                or metadata.get("publishedTime")
                or metadata.get("published")
            ),
            "snippet": _normalize_space(
                item.get("description")
                or item.get("snippet")
                or metadata.get("description")
            ),
            "metadata": metadata,
        }

    def map_url(self, base_url: str, include_subdomains: bool = False, limit: int = 80) -> list[dict[str, Any]]:
        payload = {
            "url": base_url,
            "includeSubdomains": bool(include_subdomains),
            "limit": max(1, int(limit)),
        }
        data = self._request("map", payload)
        rows = data.get("data") if isinstance(data.get("data"), list) else data.get("links", [])
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows or []:
            normalized = self._normalize_candidate(item)
            if not normalized["url"] or normalized["url"] in seen:
                continue
            seen.add(normalized["url"])
            out.append(normalized)
        return out

    def scrape_url(
        self,
        url: str,
        formats: Optional[list[str]] = None,
        only_main_content: bool = True,
        timeout: Optional[int] = None,
    ) -> dict[str, Any]:
        payload = {
            "url": url,
            "formats": formats or ["markdown"],
            "onlyMainContent": bool(only_main_content),
        }
        data = self._request("scrape", payload, timeout=timeout)
        row = data.get("data") if isinstance(data.get("data"), dict) else {}
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        return {
            "url": _normalize_space(row.get("url") or metadata.get("sourceURL") or url),
            "title": _normalize_space(row.get("title") or metadata.get("title")),
            "author": _normalize_space(metadata.get("author")),
            "published": _normalize_space(
                row.get("published")
                or metadata.get("publishedTime")
                or metadata.get("published")
            ),
            "markdown": row.get("markdown") or "",
            "html": row.get("html") or "",
            "metadata": metadata,
        }

    def search_web(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        payload = {
            "query": query,
            "limit": max(1, int(limit)),
            "sources": ["web"],
        }
        data = self._request("search", payload)
        rows = data.get("data") if isinstance(data.get("data"), list) else []
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in rows:
            normalized = self._normalize_candidate(item)
            if not normalized["url"] or normalized["url"] in seen:
                continue
            seen.add(normalized["url"])
            out.append(normalized)
        return out
