#!/usr/bin/env python3
"""Rafraîchit le cache des proxies actifs (ProxyScrape + validation)."""

import logging
import os
import sys
from pathlib import Path

import importlib.util

ROOT = Path(__file__).resolve().parents[1]
_fetcher_path = ROOT / "scraper" / "proxy_fetcher.py"
_spec = importlib.util.spec_from_file_location("proxy_fetcher", _fetcher_path)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
resolve_active_proxies = _mod.resolve_active_proxies

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    os.environ.setdefault("PROXY_AUTO_FETCH", "true")
    os.environ.setdefault("PROXY_VALIDATE", "true")
    os.environ.setdefault(
        "PROXY_CACHE_FILE",
        str(ROOT / "data" / "proxies_active.txt"),
    )
    active = resolve_active_proxies()
    if not active:
        print("Aucun proxy actif — vérifiez la connexion ou augmentez PROXY_VALIDATE_MAX")
        return 1
    print(f"{len(active)} proxies actifs enregistrés")
    for proxy in active[:5]:
        print(f"  - {proxy}")
    if len(active) > 5:
        print(f"  … et {len(active) - 5} autres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
