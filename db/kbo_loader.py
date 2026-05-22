"""
Charge les CSV KboOpenData dans PostgreSQL via COPY.
Fichiers attendus dans KBO_DATA_PATH (voir .env).
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
from pathlib import Path
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)

# Clé de filtrage par fichier (aligné sur les entreprises chargées)
_ENTITY_KEY_BY_FILE = {
    "denomination.csv": "entity_number",
    "address.csv": "entity_number",
    "activity.csv": "entity_number",
    "contact.csv": "entity_number",
    "establishment.csv": "enterprise_number",
    "branch.csv": "enterprise_number",
}

# En-têtes réels des CSV KBO Open Data (PascalCase)
_CSV_ENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "entity_number": ("EntityNumber", "entity_number"),
    "enterprise_number": ("EnterpriseNumber", "enterprise_number"),
}


def _csv_header_index(header: list[str], logical_key: str) -> int | None:
    aliases = _CSV_ENTITY_ALIASES.get(logical_key, (logical_key,))
    col_index = {name: i for i, name in enumerate(header)}
    lower_map = {name.lower(): i for i, name in enumerate(header)}
    for name in aliases:
        if name in col_index:
            return col_index[name]
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def _normalize_entity_key(raw: str) -> str:
    return (raw or "").strip()


def _entity_in_filter(raw: str, entity_filter: set[str]) -> bool:
    key = _normalize_entity_key(raw)
    if not key:
        return False
    return key in entity_filter or key.replace(".", "") in entity_filter

KBO_FILES = {
    "meta.csv": (
        "kbo_meta",
        ["variable", "value"],
    ),
    "enterprise.csv": (
        "kbo_enterprise",
        [
            "enterprise_number",
            "status",
            "juridical_situation",
            "type_of_enterprise",
            "juridical_form",
            "juridical_form_cac",
            "start_date",
        ],
    ),
    "denomination.csv": (
        "kbo_denomination",
        ["entity_number", "language", "type_of_denomination", "denomination"],
    ),
    "address.csv": (
        "kbo_address",
        [
            "entity_number",
            "type_of_address",
            "country_nl",
            "country_fr",
            "zipcode",
            "municipality_nl",
            "municipality_fr",
            "street_nl",
            "street_fr",
            "house_number",
            "box",
            "extra_address_info",
            "date_striking_off",
        ],
    ),
    "activity.csv": (
        "kbo_activity",
        ["entity_number", "activity_group", "nace_version", "nace_code", "classification"],
    ),
    "contact.csv": (
        "kbo_contact",
        ["entity_number", "entity_contact", "contact_type", "value"],
    ),
    "establishment.csv": (
        "kbo_establishment",
        ["establishment_number", "start_date", "enterprise_number"],
    ),
    "branch.csv": (
        "kbo_branch",
        ["branch_id", "start_date", "enterprise_number"],
    ),
    "code.csv": (
        "kbo_code",
        ["category", "code", "language", "description"],
    ),
}


def get_kbo_schema_dir() -> Path:
    """Répertoire SQL KBO (compatible exécution locale et conteneur Airflow)."""
    candidates = [
        os.getenv("KBO_SQL_DIR"),
        "/opt/airflow/project/db/kbo_opendata",
        str(Path(__file__).resolve().parent / "kbo_opendata"),
    ]
    for path in candidates:
        if path and Path(path).is_dir():
            return Path(path)
    raise FileNotFoundError(
        "Dossier kbo_opendata introuvable. Définissez KBO_SQL_DIR ou montez ./db dans Docker."
    )


def get_kbo_data_path() -> Path:
    candidates = [
        os.getenv("KBO_DATA_PATH"),
        "/kbo_data",
        "/opt/airflow/kbo_data",
        str(Path(__file__).resolve().parents[2] / "KboOpenData_0335_2026_04_19_Full"),
        str(Path(__file__).resolve().parents[2].parent / "KboOpenData_0335_2026_04_19_Full"),
    ]
    for path in candidates:
        if path and Path(path).exists():
            return Path(path)
    raise FileNotFoundError(
        "Dossier KboOpenData introuvable. Définissez KBO_DATA_PATH ou montez le volume Docker."
    )


def migrate_kbo_activity_primary_key(db_url: str | None = None) -> None:
    """Inclut nace_version dans la PK (données KBO : même NACE, versions 2008/2025)."""
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'kbo_activity'
                  AND c.conname = 'kbo_activity_pkey'
                  AND c.contype = 'p'
                """
            )
            if cur.fetchone()[0] == 0:
                return
            cur.execute(
                """
                SELECT a.attname
                FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
                WHERE t.relname = 'kbo_activity' AND c.conname = 'kbo_activity_pkey'
                ORDER BY cols.ord
                """
            )
            pk_cols = [row[0] for row in cur.fetchall()]
            if "nace_version" in pk_cols:
                return
            cur.execute("ALTER TABLE kbo_activity DROP CONSTRAINT kbo_activity_pkey")
            cur.execute(
                """
                ALTER TABLE kbo_activity
                ADD PRIMARY KEY (entity_number, activity_group, nace_version, nace_code, classification)
                """
            )
        logger.info("PK kbo_activity migrée (nace_version incluse)")
    finally:
        conn.close()


def run_sql_file(sql_path: Path, db_url: str | None = None) -> None:
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    sql = sql_path.read_text(encoding="utf-8")
    statements = []
    buffer = []
    for line in sql.splitlines():
        if line.strip().startswith("\\"):
            continue
        buffer.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []
    if buffer:
        statements.append("\n".join(buffer))

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
        logger.info("SQL exécuté: %s", sql_path.name)
    finally:
        conn.close()


def truncate_kbo_tables(db_url: str | None = None) -> None:
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    tables = [spec[0] for spec in KBO_FILES.values()]
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE " + ", ".join(tables) + " RESTART IDENTITY CASCADE"
            )
        logger.info("Tables KBO vidées")
    finally:
        conn.close()


def _airflow_heartbeat(context: dict[str, Any] | None) -> None:
    """Évite le zombie kill pendant les COPY longs (Celery / state mismatch)."""
    try:
        from airflow_runtime import airflow_heartbeat
    except ImportError:
        return
    airflow_heartbeat(context)


def _collect_enterprise_numbers(
    filepath: Path,
    limit: int,
    offset: int = 0,
) -> set[str]:
    numbers: set[str] = set()
    offset = max(0, int(offset))
    collected = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < offset:
                continue
            if limit > 0 and collected >= limit:
                break
            collected += 1
            raw = (
                row.get("EnterpriseNumber")
                or row.get("enterprise_number")
                or row.get("EntityNumber")
                or row.get("entity_number")
                or ""
            ).strip()
            if not raw:
                for key, value in row.items():
                    if key.lower() in (
                        "enterprisenumber",
                        "entitynumber",
                        "enterprise_number",
                        "entity_number",
                    ):
                        raw = (value or "").strip()
                        break
            if raw:
                numbers.add(raw)
                numbers.add(raw.replace(".", ""))
    return numbers


def _collect_entity_filter_from_db(db_url: str) -> set[str]:
    """Numéros d'entités déjà présents dans kbo_enterprise (import limité)."""
    numbers: set[str] = set()
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT enterprise_number FROM kbo_enterprise")
            for (raw,) in cur.fetchall():
                key = _normalize_entity_key(raw)
                if key:
                    numbers.add(key)
                    numbers.add(key.replace(".", ""))
    finally:
        conn.close()
    return numbers


def load_kbo_addresses_for_loaded_enterprises(
    data_path: Path | None = None,
    db_url: str | None = None,
    *,
    context: dict[str, Any] | None = None,
) -> int:
    """Charge address.csv pour les entreprises déjà importées (si kbo_address est vide)."""
    data_path = data_path or get_kbo_data_path()
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    filepath = data_path / "address.csv"
    if not filepath.exists():
        logger.warning("address.csv introuvable: %s", filepath)
        return 0

    entity_filter = _collect_entity_filter_from_db(db_url)
    if not entity_filter:
        logger.warning("Aucune entreprise KBO en base — import address ignoré")
        return 0

    table, columns = KBO_FILES["address.csv"]
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            rows_loaded = _load_csv_into_table(
                cur,
                filepath,
                table,
                columns,
                limit=0,
                entity_filter=entity_filter,
                entity_key="entity_number",
                context=context,
            )
            conn.commit()
    finally:
        conn.close()
    logger.info("Adresses KBO rechargées: %d lignes", rows_loaded)
    return rows_loaded


def load_kbo_activities_for_loaded_enterprises(
    data_path: Path | None = None,
    db_url: str | None = None,
    *,
    context: dict[str, Any] | None = None,
) -> int:
    """Charge activity.csv pour les entreprises déjà importées (si kbo_activity est vide)."""
    data_path = data_path or get_kbo_data_path()
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    filepath = data_path / "activity.csv"
    if not filepath.exists():
        logger.warning("activity.csv introuvable: %s", filepath)
        return 0

    entity_filter = _collect_entity_filter_from_db(db_url)
    if not entity_filter:
        logger.warning("Aucune entreprise KBO en base — import activity ignoré")
        return 0

    table, columns = KBO_FILES["activity.csv"]
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            rows_loaded = _load_csv_into_table(
                cur,
                filepath,
                table,
                columns,
                limit=0,
                entity_filter=entity_filter,
                entity_key="entity_number",
                context=context,
            )
            conn.commit()
    finally:
        conn.close()
    logger.info("Activités KBO rechargées: %d lignes", rows_loaded)
    return rows_loaded


def sync_postal_codes_to_companies(db_url: str | None = None) -> int:
    """Met à jour companies.postal_code depuis kbo_address (REGO)."""
    schema_dir = get_kbo_schema_dir()
    sql_path = schema_dir / "04_sync_postal_codes.sql"
    if not sql_path.exists():
        logger.warning("Script absent: %s", sql_path)
        return 0
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(sql_path, db_url)
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM companies "
                "WHERE postal_code IS NOT NULL AND TRIM(postal_code) <> ''"
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def sync_nace_codes_to_companies(db_url: str | None = None) -> int:
    """Met à jour companies.nace_code depuis kbo_activity (activité MAIN)."""
    schema_dir = get_kbo_schema_dir()
    sql_path = schema_dir / "05_sync_nace_codes.sql"
    if not sql_path.exists():
        logger.warning("Script absent: %s", sql_path)
        return 0
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    run_sql_file(sql_path, db_url)
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM companies "
                "WHERE nace_code IS NOT NULL AND TRIM(nace_code) <> '' AND is_deleted = FALSE"
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _copy_from_buffer(
    cur,
    copy_sql: str,
    header: list[str],
    rows: list[list[str]],
) -> int:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    buf.seek(0)
    cur.copy_expert(copy_sql, buf)
    return len(rows)


def _load_csv_into_table(
    cur,
    filepath: Path,
    table: str,
    columns: list[str],
    *,
    limit: int = 0,
    entity_filter: set[str] | None = None,
    entity_key: str | None = None,
    context: dict[str, Any] | None = None,
) -> int:
    col_list = ", ".join(columns)
    copy_sql = (
        f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true, QUOTE '\"')"
    )
    cur.execute(f"TRUNCATE TABLE {table} CASCADE")

    heartbeat_every = float(os.getenv("KBO_IMPORT_HEARTBEAT_SEC", "20"))
    last_hb = time.monotonic()

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_index = {name: i for i, name in enumerate(header)}

        if limit > 0 and filepath.name == "enterprise.csv":
            rows = []
            try:
                from db.batch_utils import get_kbo_batch_offset

                import_offset = get_kbo_batch_offset()
            except Exception:
                import_offset = int(os.getenv("KBO_BCE_BATCH_OFFSET", "0") or "0")
            collected = 0
            for i, row in enumerate(reader):
                if i < import_offset:
                    continue
                if collected >= limit:
                    break
                rows.append(row)
                collected += 1
                if time.monotonic() - last_hb >= heartbeat_every:
                    _airflow_heartbeat(context)
                    last_hb = time.monotonic()
            return _copy_from_buffer(cur, copy_sql, header, rows)

        idx = (
            _csv_header_index(header, entity_key) if entity_filter and entity_key else None
        )
        if idx is not None:
            rows = []
            for row in reader:
                key = row[idx].strip() if idx < len(row) else ""
                if _entity_in_filter(key, entity_filter):
                    rows.append(row)
                if time.monotonic() - last_hb >= heartbeat_every:
                    _airflow_heartbeat(context)
                    last_hb = time.monotonic()
            return _copy_from_buffer(cur, copy_sql, header, rows)

    # Import complet (prod) — COPY PostgreSQL direct (pas de buffer mémoire)
    with open(filepath, newline="", encoding="utf-8") as full_f:
        cur.copy_expert(copy_sql, full_f)
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return int(cur.fetchone()[0])


def load_all_csv(
    data_path: Path | None = None,
    db_url: str | None = None,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, int]:
    data_path = data_path or get_kbo_data_path()
    db_url = db_url or os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
    limit = int(os.getenv("KBO_IMPORT_LIMIT", "0") or "0")
    import_offset = 0
    if limit > 0:
        try:
            from db.batch_utils import get_kbo_batch_offset

            import_offset = get_kbo_batch_offset()
        except Exception:
            import_offset = int(os.getenv("KBO_BCE_BATCH_OFFSET", "0") or "0")

    enterprise_path = data_path / "enterprise.csv"
    entity_filter: set[str] | None = None
    if limit > 0 and enterprise_path.exists():
        entity_filter = _collect_enterprise_numbers(
            enterprise_path, limit, offset=import_offset
        )
        logger.info(
            "Mode import limité: %d entreprises (KBO_IMPORT_LIMIT=%d, offset=%d)",
            len(entity_filter),
            limit,
            import_offset,
        )

    counts: dict[str, int] = {}
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            for filename, (table, columns) in KBO_FILES.items():
                filepath = data_path / filename
                if not filepath.exists():
                    logger.warning("Fichier absent: %s", filepath)
                    continue

                entity_key = _ENTITY_KEY_BY_FILE.get(filename)
                use_filter = limit > 0 and entity_filter and entity_key

                if (
                    limit > 0
                    and not entity_filter
                    and filename not in ("meta.csv", "code.csv", "enterprise.csv")
                ):
                    logger.info("Ignoré %s (hors périmètre limité)", filename)
                    counts[filename] = 0
                    continue

                if limit > 0 and not entity_filter and filename == "enterprise.csv":
                    entity_filter = _collect_enterprise_numbers(
                        filepath, limit, offset=import_offset
                    )

                use_filter = limit > 0 and entity_filter and entity_key
                if limit > 0 and filename not in (
                    "meta.csv",
                    "code.csv",
                    "enterprise.csv",
                ) and not use_filter:
                    logger.info("Ignoré %s (hors périmètre limité)", filename)
                    counts[filename] = 0
                    continue

                rows_loaded = _load_csv_into_table(
                    cur,
                    filepath,
                    table,
                    columns,
                    limit=limit,
                    entity_filter=entity_filter if use_filter else None,
                    entity_key=entity_key,
                    context=context,
                )
                conn.commit()
                counts[filename] = rows_loaded
                logger.info("Chargé %s: %d lignes", filename, rows_loaded)
                _airflow_heartbeat(context)
    finally:
        conn.close()
    return counts


def import_kbo_opendata(
    run_seed: bool = True,
    run_views: bool = True,
    schema_dir: Path | None = None,
) -> dict:
    schema_dir = schema_dir or Path(__file__).parent / "kbo_opendata"
    db_url = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")

    run_sql_file(schema_dir / "01_schema.sql", db_url)
    migrate_kbo_activity_primary_key(db_url)
    counts = load_all_csv(db_url=db_url)

    if run_seed:
        run_sql_file(schema_dir / "02_seed_companies.sql", db_url)
    if run_views:
        run_sql_file(schema_dir / "03_analytics_views.sql", db_url)

    return {"loaded": counts, "seed": run_seed, "views": run_views}
