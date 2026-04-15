from __future__ import annotations

"""
Analyst brief generator v10.

Minimal v10 environment block:
    SUMMARY_BACKEND=groq
    GROQ_API_KEY=
    GROQ_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
    SCRAPLING_MODE=auto
    SCRAPE_SOURCE_WORKERS=3
    SCRAPE_MAX_CANDIDATES=40
    SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY=true
"""

import argparse
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import analyst_brief_generator_v9 as legacy
from scrap_final import (
    Article as ScrapedArticle,
    SourceConfig as ScrapeSourceConfig,
    scrape_sources,
    scrapling_runtime_status,
)


LOG = legacy.LOG
ACTIVE_SCRAPE_CORE = "scrap_final_v10_scrapling"


def build_source_config_from_spec(spec: Dict[str, Any]) -> ScrapeSourceConfig:
    name = spec["name"]
    env_var = spec.get("env_var") or legacy.derive_generic_env_name(name)
    raw_seed_urls = list(spec.get("seed_urls") or [])
    if raw_seed_urls:
        env_override = legacy.opt_env(env_var, raw_seed_urls[0])
        seed_urls = legacy.dedupe_keep_order([env_override] + raw_seed_urls[1:])
    else:
        seed_urls = legacy.dedupe_keep_order([legacy.opt_env(env_var, "")])
    seed_urls = [url for url in seed_urls if url]
    allowed_hosts = legacy.dedupe_keep_order(spec.get("allowed_hosts") or legacy.infer_allowed_hosts_from_urls(seed_urls))
    category_terms, search_terms = legacy.derive_default_terms(name, seed_urls)
    category_terms = legacy.dedupe_keep_order((spec.get("category_terms") or []) + category_terms)
    search_terms = legacy.dedupe_keep_order((spec.get("search_terms") or []) + search_terms)
    js_heavy = bool(spec.get("js_heavy", False))
    config_kwargs: Dict[str, Any] = {
        "name": name,
        "seed_urls": seed_urls,
        "allowed_hosts": allowed_hosts,
        "include_hints": legacy.dedupe_keep_order(spec.get("include_hints") or []),
        "exclude_hints": legacy.dedupe_keep_order(spec.get("exclude_hints") or []),
        "category_terms": category_terms,
        "search_terms": search_terms,
        "paywalled_preview_ok": bool(spec.get("paywalled_preview_ok", True)),
        "min_body_chars": legacy.source_min_body_chars(int(spec.get("min_body_chars", 55)), js_heavy=js_heavy),
        "js_heavy": js_heavy,
        "sitemap_urls": legacy.dedupe_keep_order(spec.get("sitemap_urls") or []),
        "rss_urls": legacy.dedupe_keep_order(spec.get("rss_urls") or []),
        "pdf_report_ok": bool(spec.get("pdf_report_ok", True)),
        "report_terms": legacy.dedupe_keep_order(spec.get("report_terms") or ["report", "research", "analysis", "outlook", "forecast", "pdf"]),
        "source_role": legacy.normalize_space(str(spec.get("source_role") or "narrative_news")).lower(),
    }
    return ScrapeSourceConfig(**config_kwargs)


def build_source_configs() -> List[ScrapeSourceConfig]:
    specs = list(legacy.SOURCE_REGISTRY_V6) + legacy.build_additional_source_specs_from_env()
    return [build_source_config_from_spec(spec) for spec in specs]


def resolve_target_date(explicit_target_date: str = "") -> Optional[str]:
    return legacy.resolve_target_date(explicit_target_date)


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
    headroom_multiplier = max(1, legacy.env_int("SCRAPE_HEADROOM_MULTIPLIER", 1))
    fetch_total = max_articles_total * headroom_multiplier
    fetch_per_source = max(per_source_limit, per_source_limit * headroom_multiplier)
    LOG.info(
        "Fetch sizing | provider=scrapling | headroom_multiplier=%s | fetch_total=%s | fetch_per_source=%s | mode=%s",
        headroom_multiplier,
        fetch_total,
        fetch_per_source,
        legacy.opt_env("SCRAPLING_MODE", "auto"),
    )

    def _run(source_configs: List[ScrapeSourceConfig]) -> List[ScrapedArticle]:
        articles = scrape_sources(
            sources=source_configs,
            target_date=target_date,
            max_articles_total=fetch_total,
            per_source_limit=fetch_per_source,
            user_agent=legacy.opt_env(
                "HTTP_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            ),
        )
        articles = legacy.maybe_filter_with_llm_relevance(articles, backend=backend, target_date=target_date)
        scoped = [article for article in articles if legacy.scope_matches(requested_scope, article.scope)]
        return scoped[:max_articles_total]

    articles = _run(selected_source_configs)
    LOG.info(
        "Selected-source pass finished | provider=scrapling | selected_sources=%s | accepted_articles=%s",
        [cfg.name for cfg in selected_source_configs],
        len(articles),
    )
    if articles:
        return articles

    if legacy.env_bool("ENABLE_TODAY_FALLBACK", True):
        fallback_sources = legacy.get_fallback_sources(all_source_configs, selected_source_configs)
        if fallback_sources:
            LOG.info(
                "No qualifying articles found from selected sources for date %s. Expanding to fallback sources: %s",
                target_date,
                [source.name for source in fallback_sources],
            )
            legacy.append_run_log(run_id, {
                "stage": "today_fallback_started",
                "target_date": target_date,
                "fallback_sources": [source.name for source in fallback_sources],
                "ts": legacy.utc_iso_now(),
            })
            return _run(fallback_sources)
    return []


def main() -> int:
    legacy.load_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    parser = argparse.ArgumentParser(
        description="Analyst brief generator v10 (Scrapling-first daily metal intelligence pipeline + Groq/Ollama summarization + SharePoint + email alerts)."
    )
    parser.add_argument("--interactive", action="store_true", help="Prompt the user to choose market scope and sources")
    parser.add_argument("--metal", default="all", help="copper | aluminum | broad-metals | copper,aluminum | all")
    parser.add_argument("--sources", default="all", help="Comma-separated source names, e.g. Reuters,Argus Media,LME.com")
    parser.add_argument("--target-date", default="", help="Override article publish date filter in YYYY-MM-DD format")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to SharePoint")
    parser.add_argument("--backend", default=legacy.opt_env("SUMMARY_BACKEND", "auto"), help="groq | ollama | auto | rule")
    parser.add_argument("--max-articles-total", type=int, default=legacy.env_int("MAX_ARTICLES_TOTAL", 12))
    parser.add_argument("--per-source-limit", type=int, default=legacy.env_int("PER_SOURCE_LIMIT", 6))
    parser.add_argument("--output-json", default="", help="Optional file path for dry-run JSON output")
    parser.add_argument("--test-email-alert", action="store_true", help="Send a test email alert and exit after Graph auth")
    parser.add_argument("--email-alert-mode", default=legacy.opt_env("EMAIL_ALERT_MODE", "created_only"), help="created_only | created_or_duplicates | all_generated")
    parser.add_argument("--scrapling-mode", default=legacy.opt_env("SCRAPLING_MODE", "auto"), help="auto | http | dynamic | stealthy")
    parser.add_argument("--source-workers", type=int, default=legacy.env_int("SCRAPE_SOURCE_WORKERS", 5), help="Concurrent source workers for scrape stage")
    parser.add_argument("--scrape-max-candidates", type=int, default=legacy.env_int("SCRAPE_MAX_CANDIDATES", 80), help="Maximum discovered candidates to consider per source")
    parser.add_argument("--disable-firecrawl-discovery", action="store_true", help="Disable optional Firecrawl discovery expansion inside scrap_final")
    args = parser.parse_args()

    os.environ["EMAIL_ALERT_MODE"] = (args.email_alert_mode or legacy.opt_env("EMAIL_ALERT_MODE", "created_only")).strip()
    os.environ["SCRAPLING_MODE"] = (args.scrapling_mode or legacy.opt_env("SCRAPLING_MODE", "auto")).strip().lower()
    os.environ["SCRAPE_SOURCE_WORKERS"] = str(max(1, args.source_workers))
    os.environ["SCRAPE_MAX_CANDIDATES"] = str(max(8, args.scrape_max_candidates))
    if args.disable_firecrawl_discovery:
        os.environ["SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY"] = "false"

    source_configs = build_source_configs()
    requested_scope = "All"
    selected_source_configs = source_configs
    if args.interactive:
        requested_scope, selected_source_configs = legacy.prompt_user_preferences(source_configs)
        customize = input("\nDo you also want to customize run settings like article count and AI detail? (y/n): ").strip().lower()
        if customize in {"y", "yes"}:
            legacy.prompt_runtime_settings()
            args.max_articles_total = legacy.env_int("MAX_ARTICLES_TOTAL", args.max_articles_total)
            args.per_source_limit = legacy.env_int("PER_SOURCE_LIMIT", args.per_source_limit)
    else:
        requested_scope = legacy.normalize_requested_scope(args.metal)
        selected_source_configs = legacy.parse_requested_sources(args.sources, source_configs)

    if not selected_source_configs:
        raise RuntimeError("No source URLs configured.")

    run_id = str(uuid.uuid4())
    os.environ["SCRAPE_RUN_ID"] = run_id
    target_date = resolve_target_date(args.target_date)
    backend_order = legacy.summary_backend_order(args.backend)
    relevance_order = legacy.relevance_backend_order(args.backend)
    selected_backend = backend_order[0]
    groq_key_present = legacy.groq_enabled()
    groq_model = legacy.opt_env("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
    runtime_status = scrapling_runtime_status()

    LOG.info("Run ID: %s", run_id)
    LOG.info("Active script: %s", Path(__file__).name)
    LOG.info("Requested scope: %s", requested_scope)
    LOG.info("Selected sources: %s", [source.name for source in selected_source_configs])
    LOG.info("Target date: %s", target_date)
    LOG.info(
        "Summary backend requested=%s selected=%s groq_key_present=%s groq_client_initialized=%s model=%s",
        args.backend,
        selected_backend,
        groq_key_present,
        legacy.GROQ_CLIENT_IMPORT_OK,
        groq_model,
    )
    LOG.info("Summary backend order: %s", backend_order)
    LOG.info("Relevance backend order: %s", relevance_order)
    LOG.info("Optional stack availability: %s", legacy.optional_imports_status())
    LOG.info("Active scrape core: %s", ACTIVE_SCRAPE_CORE)
    LOG.info("Scrapling runtime: %s", runtime_status)
    LOG.info(
        "Scrape settings | scrapling_mode=%s | source_workers=%s | max_candidates=%s | firecrawl_discovery=%s",
        legacy.opt_env("SCRAPLING_MODE", "auto"),
        legacy.env_int("SCRAPE_SOURCE_WORKERS", 3),
        legacy.env_int("SCRAPE_MAX_CANDIDATES", 40),
        legacy.env_bool("SCRAP_FINAL_ENABLE_FIRECRAWL_DISCOVERY", True),
    )
    legacy.append_run_log(run_id, {
        "stage": "run_started",
        "run_id": run_id,
        "backend": args.backend,
        "backend_order": backend_order,
        "relevance_backend_order": relevance_order,
        "selected_backend": selected_backend,
        "groq_key_present": groq_key_present,
        "groq_client_initialized": legacy.GROQ_CLIENT_IMPORT_OK,
        "active_script": Path(__file__).name,
        "active_scrape_core": ACTIVE_SCRAPE_CORE,
        "sources": [source.name for source in selected_source_configs],
        "requested_scope": requested_scope,
        "target_date": target_date,
        "max_articles_total": args.max_articles_total,
        "per_source_limit": args.per_source_limit,
        "scrapling_mode": legacy.opt_env("SCRAPLING_MODE", "auto"),
        "scrapling_runtime": runtime_status,
        "optional_imports": legacy.optional_imports_status(),
        "ts": legacy.utc_iso_now(),
    })

    ollama_runtime_needed = (
        selected_backend == "ollama"
        or (legacy.relevance_filter_enabled(args.backend) and bool(relevance_order) and relevance_order[0] == "ollama")
    )
    if ollama_runtime_needed:
        host = legacy.get_ollama_host()
        try:
            models = legacy.ollama_list_models(host)
            LOG.info("Ollama reachable | host=%s | models=%s", host, models)
            legacy.append_run_log(run_id, {"stage": "ollama_preflight", "host": host, "models": models, "ts": legacy.utc_iso_now()})
        except Exception as exc:
            LOG.warning("Ollama preflight failed | host=%s | error=%s", host, exc)
            legacy.append_run_log(run_id, {"stage": "ollama_preflight_failed", "host": host, "error": str(exc), "ts": legacy.utc_iso_now()})

    generated_rows: List[Dict[str, Any]] = []
    probe_info: Dict[str, Any] = {}
    if selected_backend == "ollama" and legacy.env_bool("ENABLE_OLLAMA_SOURCE_PROBE", True):
        try:
            probe_rows, probe_info = legacy.try_ollama_source_probe(
                source_configs=selected_source_configs,
                requested_scope=requested_scope,
                target_date=target_date,
                run_id=run_id,
            )
            legacy.append_run_log(run_id, {
                "stage": "ollama_source_probe_finished",
                "requested_scope": requested_scope,
                "target_date": target_date,
                "sources": [source.name for source in selected_source_configs],
                "probe_info": probe_info,
                "ts": legacy.utc_iso_now(),
            })
            if probe_rows:
                generated_rows = probe_rows
                LOG.info(
                    "Ollama source probe returned %d usable rows via %s; scraping will be skipped.",
                    len(generated_rows),
                    probe_info.get("model", "unknown-model"),
                )
            else:
                LOG.info(
                    "Ollama source probe returned no usable rows (status=%s). Falling back to normal scraping.",
                    probe_info.get("status", "unknown"),
                )
        except Exception as exc:
            probe_info = {"status": "error", "error": str(exc)}
            LOG.warning("Ollama source probe failed; falling back to normal scraping. Error=%s", exc)
            legacy.append_run_log(run_id, {
                "stage": "ollama_source_probe_failed",
                "requested_scope": requested_scope,
                "target_date": target_date,
                "sources": [source.name for source in selected_source_configs],
                "error": str(exc),
                "ts": legacy.utc_iso_now(),
            })

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
            LOG.warning("No qualifying articles found for target date %s.", target_date)
            legacy.append_run_log(run_id, {"stage": "run_finished_no_articles", "target_date": target_date, "ts": legacy.utc_iso_now()})
            return 0
        LOG.info("Relevant articles found: %d", len(articles))
        for idx, article in enumerate(articles, start=1):
            LOG.info("Generating summary %d/%d | %s", idx, len(articles), article.title)
            try:
                summary, generation_mode = legacy.generate_summary(article, args.backend)
                generated_rows.append(legacy.build_sharepoint_fields(article, summary, generation_mode, run_id))
                legacy.append_run_log(run_id, {
                    "stage": "summary_generated",
                    "source_name": article.source_name,
                    "url": article.source_url,
                    "title": article.title,
                    "scope": article.scope,
                    "generation_mode": generation_mode,
                    "warnings": article.warnings,
                    "ts": legacy.utc_iso_now(),
                })
            except Exception as exc:
                LOG.exception("Generation failed for article: %s", article.source_url)
                article.warnings.append(f"Generation failed: {str(exc)[:300]}")
                summary = legacy.build_rule_based_summary(article)
                generated_rows.append(legacy.build_sharepoint_fields(article, summary, "rule_based_after_error", run_id))
                legacy.append_run_log(run_id, {
                    "stage": "summary_failed_fallback_used",
                    "source_name": article.source_name,
                    "url": article.source_url,
                    "title": article.title,
                    "error": str(exc),
                    "ts": legacy.utc_iso_now(),
                })
    else:
        legacy.append_run_log(run_id, {
            "stage": "ollama_source_probe_used_rows",
            "rows": len(generated_rows),
            "probe_info": probe_info,
            "ts": legacy.utc_iso_now(),
        })

    if args.dry_run:
        output = json.dumps(generated_rows, indent=2, ensure_ascii=False)
        if args.output_json:
            Path(args.output_json).write_text(output, encoding="utf-8")
            print(f"Dry-run output written to: {args.output_json}")
        else:
            print(output)
        legacy.append_run_log(run_id, {"stage": "dry_run_output_ready", "rows": len(generated_rows), "ts": legacy.utc_iso_now()})
        return 0

    tenant_id = legacy.must_env("TENANT_ID")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    graph_token = legacy.get_graph_app_token(authority)
    if args.test_email_alert:
        test_row = {
            "Title": "Analyst Brief Email Alert Test",
            "BriefDate": legacy.today_iso(),
            "SourceName": "System Test",
            "SourceUrl": "https://example.com/alert-test",
            "PublishedAt": legacy.utc_iso_now(),
            "Metal": requested_scope if requested_scope != "All" else "All Markets",
            "Stance": "Neutral",
            "Theme": "Email pipeline connectivity test",
            "Synopsis": "This is a direct test of the analyst brief email alert pipeline.",
            "KeyPoints": "- Graph token acquired\n- HTML digest built\n- Mail request submitted",
            "WatchItems": "- Confirm inbox receipt\n- Confirm sender mailbox\n- Confirm subject line",
        }
        result = legacy.send_email_alert_digest(
            graph_token=graph_token,
            alert_rows=[test_row],
            requested_scope=requested_scope,
            target_date=target_date,
            run_id=run_id,
        )
        LOG.info("Test email alert result: %s", result)
        legacy.append_run_log(run_id, {"stage": "test_email_alert_sent", "result": result, "ts": legacy.utc_iso_now()})
        return 0

    hostname = legacy.must_env("SHAREPOINT_HOSTNAME")
    site_path = legacy.must_env("SHAREPOINT_SITE_PATH")
    list_name = legacy.must_env("SHAREPOINT_ANALYST_LIST_NAME")
    LOG.info("Resolving SharePoint site and list...")
    site_id = legacy.graph_get_site_id(graph_token, hostname, site_path)
    list_id = legacy.graph_get_list_id(graph_token, site_id, list_name)
    created_count = 0
    skipped_count = 0
    created_rows_for_alert: List[Dict[str, Any]] = []
    skipped_duplicate_rows_for_alert: List[Dict[str, Any]] = []
    for fields in generated_rows:
        try:
            unique_key = fields.get("UniqueKey", "")
            existing = legacy.graph_query_existing_items(
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
                legacy.append_run_log(run_id, {
                    "stage": "sharepoint_skipped_duplicate",
                    "title": fields.get("Title"),
                    "unique_key": unique_key,
                    "ts": legacy.utc_iso_now(),
                })
                continue
            created = legacy.graph_create_list_item(graph_token, site_id, list_id, fields)
            created_count += 1
            created_rows_for_alert.append(fields)
            LOG.info("Created SharePoint item: %s", fields.get("Title"))
            legacy.append_run_log(run_id, {
                "stage": "sharepoint_created",
                "title": fields.get("Title"),
                "unique_key": unique_key,
                "response_id": created.get("id"),
                "ts": legacy.utc_iso_now(),
            })
        except Exception as exc:
            LOG.exception("Failed to create SharePoint item for '%s': %s", fields.get("Title"), exc)
            legacy.append_run_log(run_id, {
                "stage": "sharepoint_create_error",
                "title": fields.get("Title"),
                "error": str(exc),
                "ts": legacy.utc_iso_now(),
            })

    alert_rows, alert_mode = legacy.resolve_alert_rows(
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
        legacy.env_bool("ENABLE_EMAIL_ALERTS", False),
    )
    if legacy.env_bool("ENABLE_EMAIL_ALERTS", False):
        if alert_rows:
            try:
                email_result = legacy.send_email_alert_digest(
                    graph_token=graph_token,
                    alert_rows=alert_rows,
                    requested_scope=requested_scope,
                    target_date=target_date,
                    run_id=run_id,
                )
                legacy.append_run_log(run_id, {
                    "stage": "email_alert_sent",
                    "alert_mode": alert_mode,
                    "alert_rows_count": len(alert_rows),
                    "recipients": legacy.parse_email_list(legacy.opt_env("ALERT_EMAIL_TO", "")),
                    "result": email_result,
                    "ts": legacy.utc_iso_now(),
                })
            except Exception as exc:
                LOG.exception("Email alert failed: %s", exc)
                legacy.append_run_log(run_id, {
                    "stage": "email_alert_failed",
                    "alert_mode": alert_mode,
                    "error": str(exc),
                    "alert_rows_count": len(alert_rows),
                    "ts": legacy.utc_iso_now(),
                })
        else:
            LOG.info("Email alert skipped because there are no rows for mode '%s'.", alert_mode)
            legacy.append_run_log(run_id, {
                "stage": "email_alert_skipped",
                "reason": "no_rows_for_alert_mode",
                "alert_mode": alert_mode,
                "created_rows_count": len(created_rows_for_alert),
                "duplicate_rows_count": len(skipped_duplicate_rows_for_alert),
                "rows_total": len(generated_rows),
                "ts": legacy.utc_iso_now(),
            })
    else:
        legacy.append_run_log(run_id, {
            "stage": "email_alert_skipped",
            "reason": "disabled",
            "alert_mode": alert_mode,
            "created_rows_count": len(created_rows_for_alert),
            "duplicate_rows_count": len(skipped_duplicate_rows_for_alert),
            "rows_total": len(generated_rows),
            "ts": legacy.utc_iso_now(),
        })

    LOG.info("Done. Created=%d | Skipped duplicates=%d", created_count, skipped_count)
    legacy.append_run_log(run_id, {
        "stage": "run_finished",
        "created_count": created_count,
        "skipped_count": skipped_count,
        "rows_total": len(generated_rows),
        "email_alert_rows": len(alert_rows),
        "email_alert_mode": alert_mode,
        "ts": legacy.utc_iso_now(),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
