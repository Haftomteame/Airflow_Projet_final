"""Package scraper — imports légers pour le chargement des DAGs Airflow."""

__all__ = [
    "BelgianScraper",
    "scrape_page",
    "scrape_page_with_rotation",
    "ProxyManager",
    "HDFSClient",
    "is_valid_page",
]

_LAZY_EXPORTS = {
    "BelgianScraper": "scraper.scraper",
    "scrape_page": "scraper.scraper",
    "scrape_page_with_rotation": "scraper.scraper",
    "ProxyManager": "scraper.proxy_manager",
    "HDFSClient": "scraper.hdfs_client",
    "is_valid_page": "scraper.validator",
}


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(_LAZY_EXPORTS[name])
    return getattr(module, name)
