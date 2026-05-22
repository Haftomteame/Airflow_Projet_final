"""Stockage des métadonnées de scraping dans MongoDB."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MongoMetadataStore:
    """Persistance documentaire des métadonnées (remplace les .json sur HDFS)."""

    def __init__(
        self,
        uri: str | None = None,
        db_name: str | None = None,
        collection_name: str | None = None,
    ):
        self.uri = uri or os.getenv(
            "MONGODB_URI",
            "mongodb://airflow:airflow@mongodb:27017/?authSource=admin",
        )
        self.db_name = db_name or os.getenv("MONGODB_DB", "belgian_companies")
        self.collection_name = collection_name or os.getenv(
            "MONGODB_METADATA_COLLECTION",
            "scrape_metadata",
        )
        self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
        self._db: Database = self._client[self.db_name]
        self._col: Collection = self._db[self.collection_name]
        self._ensure_indexes()
        logger.info(
            "MongoMetadataStore connecté (%s / %s.%s)",
            self.uri.split("@")[-1] if "@" in self.uri else self.uri,
            self.db_name,
            self.collection_name,
        )

    def _ensure_indexes(self) -> None:
        self._col.create_index(
            [("bce_number", ASCENDING), ("source", ASCENDING), ("scraped_at", ASCENDING)],
            name="ix_bce_source_scraped",
        )
        self._col.create_index([("hdfs_html_path", ASCENDING)], name="ix_hdfs_html_path")
        self._col.create_index([("postgres_meta_id", ASCENDING)], name="ix_postgres_meta_id")

    def insert_scrape_metadata(
        self,
        *,
        bce_number: str,
        company_id: int,
        source: str,
        hdfs_html_path: str,
        scraped_at: datetime | str | None = None,
        http_code: int | None = None,
        proxy_used: str | None = None,
        attempts: int = 1,
        status: str = "success",
        page: int | None = None,
        total_pages: int | None = None,
        postgres_meta_id: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> str:
        """Insère un document métadonnées ; retourne l'identifiant MongoDB (str)."""
        if isinstance(scraped_at, str):
            scraped_at_dt = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
        elif scraped_at is None:
            scraped_at_dt = _utc_now()
        else:
            scraped_at_dt = scraped_at

        doc: dict[str, Any] = {
            "bce_number": bce_number,
            "company_id": company_id,
            "source": source,
            "hdfs_html_path": hdfs_html_path,
            "scraped_at": scraped_at_dt,
            "http_code": http_code,
            "proxy_used": proxy_used or "",
            "attempts": attempts,
            "status": status,
            "page": page,
            "total_pages": total_pages,
            "last_updated": _utc_now(),
            "postgres_meta_id": postgres_meta_id,
        }
        if extra:
            doc.update(extra)

        result = self._col.insert_one(doc)
        mongo_id = str(result.inserted_id)
        logger.debug("Métadonnées MongoDB insérées: %s", mongo_id)
        return mongo_id

    def link_postgres_meta(self, mongo_id: str, postgres_meta_id: int) -> None:
        from bson import ObjectId
        from bson.errors import InvalidId

        try:
            oid = ObjectId(mongo_id)
        except InvalidId:
            logger.warning("mongo_id invalide pour liaison: %s", mongo_id)
            return
        self._col.update_one(
            {"_id": oid},
            {"$set": {"postgres_meta_id": postgres_meta_id, "last_updated": _utc_now()}},
        )

    def get_by_postgres_id(self, postgres_meta_id: int) -> dict[str, Any] | None:
        return self._col.find_one({"postgres_meta_id": postgres_meta_id})

    def get_by_hdfs_path(self, hdfs_html_path: str) -> dict[str, Any] | None:
        return self._col.find_one({"hdfs_html_path": hdfs_html_path})

    def close(self) -> None:
        self._client.close()
