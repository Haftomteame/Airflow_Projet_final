"""Tâches d'acquisition (scraping)."""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from airflow.models import Variable

from airflow_runtime import airflow_heartbeat
from common import (
    get_hdfs,
    get_mongo,
    get_repo,
    get_scraper,
    load_bce_from_csv,
    seed_companies_from_csv,
)
from scraper.benchmark import (
    ScrapeBenchmark,
    format_pipeline_summary,
    merge_pipeline_benchmarks,
)

logger = logging.getLogger(__name__)

SCRAPE_STALE_VAR = "pipeline_scrape_stale_bces"
SCRAPE_BENCHMARK_VAR = "pipeline_scrape_benchmarks"


def _load_pipeline_benchmarks() -> dict:
    raw = Variable.get(SCRAPE_BENCHMARK_VAR, default_var="{}")
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        logger.warning("Variable %s invalide, réinitialisation", SCRAPE_BENCHMARK_VAR)
        return {}


def _save_pipeline_benchmarks(data: dict) -> None:
    Variable.set(SCRAPE_BENCHMARK_VAR, json.dumps(data, ensure_ascii=False))


def _append_source_benchmark(summary: dict[str, Any]) -> None:
    data = _load_pipeline_benchmarks()
    sources = data.get("sources")
    if not isinstance(sources, list):
        sources = []
    sources = [s for s in sources if s.get("source") != summary.get("source")]
    sources.append(summary)
    data["sources"] = sources
    _save_pipeline_benchmarks(data)


def _sources_require_storage() -> frozenset[str]:
    """Sources pour lesquelles 0 page stockée sur HDFS fait échouer le DAG scrape."""
    raw = os.getenv("SOURCES_REQUIRE_STORAGE", "kbo,moniteur,bnb")
    return frozenset(s.strip().lower() for s in raw.split(",") if s.strip())


def _store_scrape_results(results: list[dict]) -> int:
    from datetime import datetime as dt

    hdfs = get_hdfs()
    mongo = get_mongo()
    repo = get_repo()
    stored = 0

    for item in results:
        if not item.get("valid") or item.get("status_code") != 200 or not item.get("html"):
            repo.log_error(
                "validation" if item.get("status_code") == 200 else "scraping",
                f"Page non stockée: {item.get('url')}",
                bce_number=item.get("bce_number"),
                source=item.get("source"),
            )
            proxy_label = item.get("proxy_used") or ""
            if item.get("status_code") == 0 and proxy_label not in ("", "direct"):
                repo.log_error(
                    "proxy",
                    "Échec proxy",
                    bce_number=item.get("bce_number"),
                    source=item.get("source"),
                )
            continue

        bce = item["bce_number"]
        source = item["source"]
        company = repo.upsert_company({"bce_number": bce})
        page = item.get("page", 1)
        total_pages = item.get("total_pages", 1)
        meta = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "status": "success",
            "http_code": item["status_code"],
            "proxy_used": item.get("proxy_used", ""),
            "attempts": item.get("attempts", 1),
            "page": page,
            "total_pages": total_pages,
        }
        html_path = hdfs.save_html(bce, source, item["html"], page=page)
        pg_meta = repo.add_scrape_metadata({
            "company_id": company.id,
            "source": source,
            "hdfs_path": html_path,
            "scraped_at": dt.utcnow(),
            "http_code": item["status_code"],
            "proxy_used": item.get("proxy_used"),
            "attempts": item.get("attempts"),
            "status": "success",
            "parsed": False,
        })
        mongo_id = mongo.insert_scrape_metadata(
            bce_number=bce,
            company_id=company.id,
            source=source,
            hdfs_html_path=html_path,
            scraped_at=meta["scraped_at"],
            http_code=item["status_code"],
            proxy_used=item.get("proxy_used"),
            attempts=item.get("attempts", 1),
            status="success",
            page=page,
            total_pages=total_pages,
            postgres_meta_id=pg_meta.id,
        )
        repo.set_scrape_mongo_id(pg_meta.id, mongo_id)
        repo.upsert_company({"bce_number": bce, "last_scraped": dt.utcnow()})
        stored += 1

    logger.info("Documents stockés (HTML HDFS + métadonnées MongoDB): %d", stored)
    return stored


def _env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes")


def scrape_prepare_batch(**_context):
    """Prépare la liste BCE à scraper (partagée via Variable Airflow)."""
    seed_companies_from_csv([])
    bce_list = load_bce_from_csv()
    repo = get_repo()
    queued = repo.dequeue_scrape_batch(limit=0)
    db_stale = [c.bce_number for c in repo.find_stale_companies(days=14)]
    combined = list(dict.fromkeys(bce_list + queued + db_stale))
    stale_days = int(os.getenv("SCRAPE_STALE_DAYS", "14"))
    stale = repo.filter_fresh_companies(combined, days=stale_days)

    if not stale and combined and _env_bool("SCRAPE_FALLBACK_IF_ALL_FRESH", "true"):
        fallback_limit = int(os.getenv("SCRAPE_FALLBACK_LIMIT", "100") or "100")
        stale = combined if fallback_limit <= 0 else combined[:fallback_limit]
        logger.info(
            "Toutes les entreprises sont fraîches (< %d j) — repli sur %d BCE",
            stale_days,
            len(stale),
        )

    Variable.set(SCRAPE_STALE_VAR, json.dumps(stale))
    _save_pipeline_benchmarks({
        "pipeline_started_at": datetime.now(timezone.utc).isoformat(),
        "bce_count": len(stale),
        "sources": [],
    })
    logger.info("BCE à traiter: %d", len(stale))
    if not stale:
        logger.warning("Aucun BCE disponible (CSV/SQL/queue vides)")
    return len(stale)


def _limit_bce_batch(bce_list: list[str], source: str) -> list[str]:
    """Limite optionnelle par source (Moniteur paginé = très long)."""
    env_key = {
        "moniteur": "SCRAPE_MONITEUR_BCE_LIMIT",
        "kbo": "SCRAPE_KBO_BCE_LIMIT",
        "bnb": "SCRAPE_BNB_BCE_LIMIT",
    }.get(source)
    if not env_key:
        return bce_list
    limit = int(os.getenv(env_key, "0") or "0")
    if limit > 0 and len(bce_list) > limit:
        logger.info(
            "Scraping %s limité à %d/%d BCE (%s)",
            source,
            limit,
            len(bce_list),
            env_key,
        )
        return bce_list[:limit]
    return bce_list


def _run_scrape_source(source: str, **context: Any):
    stale = json.loads(Variable.get(SCRAPE_STALE_VAR, "[]"))
    if not stale:
        logger.warning(
            "Scraping %s ignoré : liste BCE vide (lancer dag_t_scrape_prepare avant ce DAG)",
            source,
        )
        return {
            "source": source,
            "scraped": 0,
            "stored": 0,
            "invalid": 0,
            "companies": 0,
            "skipped": True,
        }
    stale = _limit_bce_batch(stale, source)
    scraper = get_scraper()
    benchmark = ScrapeBenchmark(source=source, companies_target=len(stale))
    task_started = time.monotonic()
    heartbeat_every = float(os.getenv("SCRAPE_HEARTBEAT_SEC", "30"))
    last_hb = time.monotonic()

    def on_progress(_index: int, _total: int, _bce: str) -> None:
        nonlocal last_hb
        if time.monotonic() - last_hb >= heartbeat_every:
            airflow_heartbeat(context)
            last_hb = time.monotonic()

    airflow_heartbeat(context)
    results = scraper.scrape_batch(
        stale,
        source=source,
        on_progress=on_progress,
        benchmark=benchmark,
    )
    airflow_heartbeat(context)
    storage_started = time.monotonic()
    stored = _store_scrape_results(results)
    benchmark.storage_duration_sec = time.monotonic() - storage_started
    benchmark.finish()
    invalid = sum(1 for r in results if r.get("valid") is False)
    if stored == 0 and results:
        msg = (
            f"Scraping {source}: {len(results)} pages, {invalid} invalides, "
            f"aucune stockée (vérifier validateur, proxies ou portail source)"
        )
        if source in _sources_require_storage():
            raise RuntimeError(msg)
        logger.warning("%s — le pipeline scraping continue", msg)
    companies = len({r.get("bce_number") for r in results if r.get("bce_number")})
    if invalid:
        logger.warning(
            "Scraping %s: %d/%d pages rejetées par validation (%d entreprises)",
            source,
            invalid,
            len(results),
            companies,
        )
    bench_summary = benchmark.to_dict(stored=stored)
    bench_summary["task_duration_sec"] = round(time.monotonic() - task_started, 3)
    _append_source_benchmark(bench_summary)
    logger.info(benchmark.format_summary(stored=stored))
    logger.info(
        "Scraping %s: %d entreprises, %d pages HTML, %d stockées",
        source,
        companies,
        len(results),
        stored,
    )
    return {
        "source": source,
        "scraped": len(results),
        "stored": stored,
        "invalid": invalid,
        "companies": companies,
        "benchmark": bench_summary,
    }


def scrape_kbo(**context: Any):
    return _run_scrape_source("kbo", **context)


def scrape_moniteur(**context: Any):
    return _run_scrape_source("moniteur", **context)


def scrape_bnb(**context: Any):
    return _run_scrape_source("bnb", **context)


def scrape_advance_batch_offset(**_context):
    """
    Avance le curseur KBO après un pipeline scraping réussi
    (lot suivant = offset + taille du lot traité).
    """
    from batch_utils import advance_kbo_batch_offset, get_kbo_batch_offset

    stale = json.loads(Variable.get(SCRAPE_STALE_VAR, "[]"))
    if not stale:
        logger.info(
            "Curseur lot inchangé (offset=%d) : aucun BCE dans le lot",
            get_kbo_batch_offset(),
        )
        return {"advanced": 0, "offset": get_kbo_batch_offset()}

    bench_data = _load_pipeline_benchmarks()
    per_source = bench_data.get("sources") or []
    pipeline_summary = merge_pipeline_benchmarks(per_source)
    pipeline_summary["pipeline_started_at"] = bench_data.get("pipeline_started_at")
    pipeline_summary["bce_count"] = bench_data.get("bce_count", len(stale))
    logger.info(format_pipeline_summary(pipeline_summary))

    new_offset = advance_kbo_batch_offset(len(stale))
    logger.info(
        "Lot de %d BCE terminé — prochain run à partir de l'offset %d",
        len(stale),
        new_offset,
    )
    return {
        "advanced": len(stale),
        "offset": new_offset,
        "benchmark_pipeline": pipeline_summary,
    }
