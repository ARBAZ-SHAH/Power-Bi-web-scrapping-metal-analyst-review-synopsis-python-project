from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Optional

import requests


LOG = logging.getLogger("analyst.groq")


class GroqClientError(RuntimeError):
    pass


class GroqHTTPError(GroqClientError):
    def __init__(self, status_code: int, body_snippet: str) -> None:
        self.status_code = status_code
        self.body_snippet = body_snippet
        super().__init__(f"Groq HTTP {status_code}: {body_snippet}")


def _required_keys(schema: dict[str, Any]) -> list[str]:
    required = schema.get("required")
    if isinstance(required, list):
        return [str(x) for x in required if str(x)]
    return []


def _validate_minimally(payload: Any, schema: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise GroqClientError("Structured response was not a JSON object")
    for key in _required_keys(schema):
        if key not in payload:
            raise GroqClientError(f"Structured response missing required key: {key}")


def _extract_content(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise GroqClientError("Groq response did not contain choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text") or ""))
        return "\n".join(parts).strip()
    raise GroqClientError("Groq response content was empty")


def groq_generate_structured(
    prompt: str,
    schema: dict,
    model: str,
    timeout: int,
    temperature: float,
    max_completion_tokens: int,
) -> tuple[str, dict]:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        raise GroqClientError("Missing GROQ_API_KEY")

    url = "https://api.groq.com/openai/v1/chat/completions"
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
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
    schema_name = str(schema.get("title") or "analyst_summary")
    base_messages = [
        {"role": "system", "content": "Return only valid JSON that matches the supplied schema."},
        {"role": "user", "content": prompt},
    ]
    attempts = [
        {
            "label": "json_schema",
            "messages": base_messages,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "schema": schema,
                },
            },
        },
        {
            "label": "json_object",
            "messages": [
                {"role": "system", "content": "Return only valid JSON."},
                {
                    "role": "user",
                    "content": (
                        f"{prompt}\n\n"
                        "The JSON object must satisfy this schema:\n"
                        f"{json.dumps(schema, ensure_ascii=False)}"
                    ),
                },
            ],
            "response_format": {"type": "json_object"},
        },
    ]

    last_error: Optional[Exception] = None
    try:
        for mode in attempts:
            for attempt in range(1, 4):
                payload = {
                    "model": model,
                    "messages": mode["messages"],
                    "temperature": temperature,
                    "max_completion_tokens": max_completion_tokens,
                    "response_format": mode["response_format"],
                }
                started = time.monotonic()
                try:
                    response = session.post(url, json=payload, timeout=max(5, int(timeout)))
                    elapsed = round(time.monotonic() - started, 2)
                    if response.status_code >= 500:
                        body_snippet = response.text[:600]
                        LOG.warning("Groq HTTP failure | status=%s | body=%s", response.status_code, body_snippet)
                        raise GroqHTTPError(response.status_code, body_snippet)
                    if response.status_code >= 400:
                        body_snippet = response.text[:1000]
                        LOG.warning("Groq HTTP failure | status=%s | body=%s", response.status_code, body_snippet)
                        raise GroqHTTPError(response.status_code, body_snippet)
                    body = response.json()
                    json_text = _extract_content(body).strip()
                    parsed = json.loads(json_text)
                    _validate_minimally(parsed, schema)
                    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
                    meta = {
                        "model": body.get("model") or model,
                        "id": body.get("id") or "",
                        "usage": usage,
                        "response_format_mode": mode["label"],
                        "finish_reason": (
                            body.get("choices", [{}])[0].get("finish_reason")
                            if isinstance(body.get("choices"), list) and body.get("choices")
                            else ""
                        ),
                    }
                    LOG.info(
                        "Groq structured generation ok | model=%s | mode=%s | elapsed_s=%s | usage=%s",
                        meta["model"],
                        mode["label"],
                        elapsed,
                        usage,
                    )
                    return json.dumps(parsed, ensure_ascii=False), meta
                except (requests.RequestException, ValueError, GroqClientError) as exc:
                    last_error = exc
                    LOG.warning("Groq structured generation error | model=%s | mode=%s | attempt=%s | error=%s", model, mode["label"], attempt, exc)
                    if attempt >= 3:
                        LOG.warning(
                            "Groq structured generation failed | mode=%s | attempt=%s/3 | error=%s",
                            mode["label"],
                            attempt,
                            exc,
                        )
                        break
                    sleep_for = min(6, attempt * 1.5)
                    LOG.warning(
                        "Groq structured generation retry | mode=%s | attempt=%s/3 | error=%s | retry_in_s=%s",
                        mode["label"],
                        attempt,
                        exc,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
        raise GroqClientError(f"Groq structured generation failed: {last_error}")
    finally:
        try:
            session.close()
        except Exception:
            pass
