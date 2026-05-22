"""Client WebHDFS pour stockage des HTML bruts."""

import logging
import os
from datetime import datetime, timezone

import requests
from hdfs import InsecureClient
from hdfs.util import HdfsError

logger = logging.getLogger(__name__)

# Utilisateur superuser pour créer /data (hdfs n'a pas WRITE sur /)
HDFS_BOOTSTRAP_USER = os.getenv("HDFS_BOOTSTRAP_USER", "root")


class HDFSClient:
    _layout_ready = False

    def __init__(self, url: str | None = None, user: str | None = None):
        self.url = url or os.getenv("HDFS_URL", "http://namenode:9870")
        self.user = user or os.getenv("HDFS_USER", "hdfs")
        self.client = InsecureClient(self.url, user=self.user)
        self.base_path = "/data/companies"
        self._ensure_hdfs_layout()
        logger.info("HDFSClient connecté à %s", self.url)

    def _webhdfs_put(self, path: str, query: str, user: str) -> None:
        url = f"{self.url.rstrip('/')}/webhdfs/v1{path}?{query}&user.name={user}"
        requests.put(url, timeout=20)

    def _ensure_hdfs_layout(self) -> None:
        """Crée /data/companies avec droits d'écriture (perdus si le volume HDFS est recréé)."""
        if HDFSClient._layout_ready:
            return
        for path in ("/data", self.base_path):
            try:
                self._webhdfs_put(path, "op=MKDIRS", HDFS_BOOTSTRAP_USER)
            except Exception as exc:
                logger.debug("MKDIRS %s: %s", path, exc)
        for path in ("/data", self.base_path):
            try:
                self._webhdfs_put(
                    path,
                    "op=SETPERMISSION&permission=777",
                    HDFS_BOOTSTRAP_USER,
                )
            except Exception as exc:
                logger.debug("chmod %s: %s", path, exc)
        HDFSClient._layout_ready = True
        logger.info("Arborescence HDFS prête sous %s", self.base_path)

    def _ensure_dir(self, path: str) -> None:
        try:
            self.client.makedirs(path)
        except HdfsError as exc:
            err = str(exc).lower()
            if "permission denied" in err:
                HDFSClient._layout_ready = False
                self._ensure_hdfs_layout()
                self.client.makedirs(path)
            elif "already exists" not in err:
                raise

    def save_html(
        self,
        company_id: str,
        source: str,
        html: str,
        *,
        page: int | None = None,
    ) -> str:
        """
        Sauvegarde le HTML brut sur HDFS.
        Chemin: /data/companies/{company_id}/{source}/{timestamp}[_pNNNN].html
        Les métadonnées sont stockées séparément dans MongoDB (voir MongoMetadataStore).
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        page_suffix = f"_p{int(page):04d}" if page else ""
        dir_path = f"{self.base_path}/{company_id}/{source}"
        self._ensure_dir(dir_path)

        html_path = f"{dir_path}/{ts}{page_suffix}.html"

        with self.client.write(html_path, encoding="utf-8", overwrite=True) as writer:
            writer.write(html)

        logger.info("HTML HDFS sauvegardé: %s", html_path)
        return html_path

    def save_document(
        self,
        company_id: str,
        source: str,
        html: str,
        metadata: dict | None = None,
    ) -> str:
        """Rétrocompatibilité : enregistre uniquement le HTML (metadata ignoré côté HDFS)."""
        page = (metadata or {}).get("page")
        return self.save_html(company_id, source, html, page=page)

    def list_documents(self, company_id: str | None = None, source: str | None = None) -> list[str]:
        paths: list[str] = []
        root = self.base_path if not company_id else f"{self.base_path}/{company_id}"
        try:
            for path, _dirs, files in self.client.walk(root):
                for f in files:
                    if f.endswith(".html"):
                        if source and f"/{source}/" not in f"{path}/{f}":
                            continue
                        paths.append(f"{path}/{f}")
        except HdfsError as exc:
            logger.warning("Liste HDFS échouée: %s", exc)
        return paths

    def read_document(self, hdfs_path: str) -> str:
        with self.client.read(hdfs_path, encoding="utf-8") as reader:
            return reader.read()
