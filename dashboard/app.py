"""
Dashboard Streamlit — supervision temps réel de la plateforme entreprises belges.
"""

import json
import os
import re
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

st.set_page_config(
    page_title="Belgian Companies Platform",
    page_icon="🇧🇪",
    layout="wide",
    initial_sidebar_state="expanded",
)

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "30"))
DB_URL = os.getenv("APP_DB_URL", "postgresql://airflow:airflow@postgres/belgian_companies")
AIRFLOW_UI_URL = os.getenv("AIRFLOW_UI_URL", "http://localhost:8080")
HDFS_UI_URL = os.getenv("HDFS_UI_URL", "http://localhost:19870")
KBO_BASE_URL = os.getenv(
    "KBO_BASE_URL",
    "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html",
)
MONITEUR_BASE_URL = os.getenv(
    "MONITEUR_BASE_URL",
    "https://www.ejustice.just.fgov.be/cgi_tsv/list.pl",
)
BNB_BASE_URL = os.getenv("BNB_BASE_URL", "https://consult.cbso.nbb.be/")

# Palette inspirée des couleurs belges + UI moderne
COLORS = {
    "bg": "#0c1222",
    "card": "#151d33",
    "card_hover": "#1c2744",
    "border": "#2a3655",
    "text": "#f8fafc",
    "muted": "#b8c8de",
    "gold": "#f5c518",
    "red": "#e63946",
    "green": "#22c55e",
    "blue": "#3b82f6",
    "cyan": "#06b6d4",
    "purple": "#a78bfa",
}
CHART_PALETTE = ["#3b82f6", "#22c55e", "#f5c518", "#e63946", "#a78bfa", "#06b6d4", "#f97316"]

BCE_LINK_COLUMNS = ("Lien KBO", "Lien Moniteur", "Lien BNB")


def normalize_bce_digits(bce_number: str) -> str:
    digits = re.sub(r"\D", "", bce_number or "")
    return digits.zfill(10)[-10:] if digits else ""


def kbo_public_url(bce_number: str) -> str:
    digits = normalize_bce_digits(bce_number)
    if not digits:
        return ""
    return f"{KBO_BASE_URL.rstrip('/')}?{urlencode({'lang': 'fr', 'ondernemingsnummer': digits})}"


def moniteur_public_url(bce_number: str, page: int = 1) -> str:
    digits = normalize_bce_digits(bce_number)
    if not digits:
        return ""
    view_numac = digits
    btw = digits.lstrip("0") or digits
    params = {
        "language": "fr",
        "sum_date": "",
        "page": str(page),
        "view_numac": view_numac,
        "btw": btw,
    }
    return f"{MONITEUR_BASE_URL.rstrip('/')}?{urlencode(params)}"


def bnb_public_url(bce_number: str) -> str:
    digits = normalize_bce_digits(bce_number)
    if not digits:
        return ""
    formatted = f"{digits[:4]}.{digits[4:7]}.{digits[7:10]}"
    base = BNB_BASE_URL.rstrip("/")
    return f"{base}/enterprise/{formatted}?lang=FR"


def source_public_url(source: str, bce_number: str) -> str:
    src = (source or "").lower()
    if src == "moniteur":
        return moniteur_public_url(bce_number)
    if src == "bnb":
        return bnb_public_url(bce_number)
    return kbo_public_url(bce_number)


def hdfs_explorer_url(hdfs_path: str | None) -> str:
    if hdfs_path is None or (isinstance(hdfs_path, float) and pd.isna(hdfs_path)):
        return ""
    path = str(hdfs_path).strip()
    if not path:
        return ""
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{HDFS_UI_URL.rstrip('/')}/explorer.html#{path}"


def add_bce_public_links(df: pd.DataFrame, bce_col: str = "bce_number") -> pd.DataFrame:
    out = df.copy()
    if bce_col not in out.columns:
        return out
    bce = out[bce_col].astype(str)
    out["Lien KBO"] = bce.map(kbo_public_url)
    out["Lien Moniteur"] = bce.map(moniteur_public_url)
    out["Lien BNB"] = bce.map(bnb_public_url)
    return out


def link_column_config(*columns: str) -> dict:
    labels = {
        "Lien KBO": "KBO",
        "Lien Moniteur": "Moniteur",
        "Lien BNB": "BNB",
        "Lien source": "Source",
        "Lien HDFS": "HDFS",
        "Lien BCE découvert": "BCE découvert",
        "Lien entreprise source": "Source",
    }
    return {
        col: st.column_config.LinkColumn(labels.get(col, col), display_text="Ouvrir")
        for col in columns
    }


def show_linked_dataframe(df: pd.DataFrame, link_cols: tuple[str, ...], column_order: list[str] | None = None) -> None:
    if df.empty:
        return
    display = df.copy()
    if column_order:
        cols = [c for c in column_order if c in display.columns]
        extra = [c for c in display.columns if c not in cols]
        display = display[cols + extra]
    st.dataframe(
        display,
        hide_index=True,
        use_container_width=True,
        column_config=link_column_config(*link_cols),
    )


def inject_styles() -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,600;0,9..40,700&display=swap');

        :root {{
            --text-primary: {COLORS["text"]};
            --text-secondary: {COLORS["muted"]};
            --bg-main: {COLORS["bg"]};
            --bg-card: {COLORS["card"]};
            --border: {COLORS["border"]};
            --accent: {COLORS["gold"]};
        }}

        .stApp {{
            background: linear-gradient(165deg, {COLORS["bg"]} 0%, #111827 45%, #0f172a 100%);
            font-family: 'DM Sans', sans-serif;
            color: var(--text-primary) !important;
        }}

        /* --- Texte principal (zone centrale) --- */
        .main .block-container,
        .main .block-container p,
        .main .block-container span,
        .main .block-container li,
        .main .block-container label,
        .main [data-testid="stMarkdownContainer"] p,
        .main [data-testid="stMarkdownContainer"] li,
        .main [data-testid="stMarkdownContainer"] span,
        .main [data-testid="stMarkdownContainer"] strong {{
            color: var(--text-primary) !important;
        }}

        h1, h2, h3, h4, h5, h6,
        .main h1, .main h2, .main h3,
        [data-testid="stHeader"],
        [data-testid="stSubheader"],
        .main [data-testid="stMarkdownContainer"] h1,
        .main [data-testid="stMarkdownContainer"] h2,
        .main [data-testid="stMarkdownContainer"] h3 {{
            color: #ffffff !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 700 !important;
        }}

        [data-testid="stCaptionContainer"] p,
        .stCaption, .stCaption p {{
            color: var(--text-secondary) !important;
            font-size: 0.95rem !important;
        }}

        /* Liens rapides */
        .dashboard-links a {{
            color: {COLORS["cyan"]} !important;
            font-weight: 600;
            text-decoration: none;
            margin-right: 1.25rem;
        }}
        .dashboard-links a:hover {{
            color: {COLORS["gold"]} !important;
            text-decoration: underline;
        }}

        /* Onglets — retour à la ligne pour afficher Historique / Erreurs en entier */
        [data-testid="stTabs"] {{
            overflow: visible !important;
        }}
        [data-testid="stTabs"] > div {{
            overflow: visible !important;
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
            background: transparent;
            border-bottom: 1px solid var(--border);
            padding-bottom: 0.5rem;
            flex-wrap: wrap !important;
            overflow-x: visible !important;
            overflow-y: visible !important;
            height: auto !important;
            min-height: unset !important;
        }}
        .stTabs [data-baseweb="tab"] {{
            background: var(--bg-card);
            border-radius: 12px 12px 0 0;
            color: var(--text-secondary) !important;
            border: 1px solid transparent;
            padding: 0.5rem 0.9rem !important;
            font-weight: 600;
            font-size: 0.9rem !important;
            flex: 0 0 auto !important;
            min-width: max-content !important;
            max-width: none !important;
            white-space: nowrap !important;
            overflow: visible !important;
        }}
        .stTabs [data-baseweb="tab"] p,
        .stTabs [data-baseweb="tab"] span,
        .stTabs [data-baseweb="tab"] div {{
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }}
        .stTabs [aria-selected="true"] {{
            background: linear-gradient(180deg, #1e3a5f 0%, var(--bg-card) 100%) !important;
            color: var(--accent) !important;
            border-color: var(--border) !important;
        }}
        .stTabs [data-baseweb="tab-highlight"] {{
            display: none;
        }}

        /* Radio, select, multiselect */
        .stRadio label,
        .stRadio [data-testid="stMarkdownContainer"] p,
        .stSelectbox label,
        .stMultiSelect label,
        .stTextInput label,
        [data-testid="stWidgetLabel"] p,
        [data-testid="stWidgetLabel"] label {{
            color: var(--text-primary) !important;
            font-weight: 500 !important;
        }}
        .stRadio div[role="radiogroup"] label span {{
            color: var(--text-primary) !important;
        }}

        /* Cartes KPI personnalisées (évite la troncature de st.metric) */
        .kpi-card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 0.85rem 1rem;
            min-height: 5.5rem;
        }}
        .kpi-card .kpi-label {{
            color: var(--text-secondary);
            font-size: 0.8rem;
            line-height: 1.35;
            margin-bottom: 0.35rem;
            word-break: break-word;
        }}
        .kpi-card .kpi-value {{
            color: #ffffff;
            font-size: 1.75rem;
            font-weight: 700;
            line-height: 1.2;
        }}

        /* Métriques Streamlit ailleurs dans l'app */
        div[data-testid="stMetric"],
        div[data-testid="metric-container"] {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 0.85rem 1rem;
        }}
        [data-testid="stMetricLabel"],
        [data-testid="stMetricLabel"] > div,
        div[data-testid="metric-container"] label[data-testid="stMetricLabel"],
        div[data-testid="metric-container"] label[data-testid="stMetricLabel"] > div,
        div[data-testid="metric-container"] label[data-testid="stMetricLabel"] p {{
            color: var(--text-secondary) !important;
            font-size: 0.8rem !important;
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: unset !important;
            line-height: 1.35 !important;
            word-break: break-word;
        }}
        [data-testid="stMetricValue"] {{
            color: #ffffff !important;
            font-size: 1.75rem !important;
            font-weight: 700 !important;
        }}

        /* Conteneurs avec bordure */
        [data-testid="stVerticalBlockBorderWrapper"] {{
            background: var(--bg-card);
            border-color: var(--border) !important;
            border-radius: 16px;
            padding: 0.5rem 0.75rem;
        }}
        [data-testid="stVerticalBlockBorderWrapper"] h3,
        [data-testid="stVerticalBlockBorderWrapper"] p,
        [data-testid="stVerticalBlockBorderWrapper"] label {{
            color: var(--text-primary) !important;
        }}

        /* Barre de progression */
        .stProgress label {{
            color: var(--text-primary) !important;
        }}

        /* Expanders */
        .streamlit-expanderHeader {{
            color: var(--text-primary) !important;
            background-color: var(--bg-card) !important;
            font-weight: 600;
        }}
        .streamlit-expanderContent p {{
            color: var(--text-primary) !important;
        }}

        /* Alertes */
        [data-testid="stAlert"] p,
        .stAlert p {{
            color: var(--text-primary) !important;
        }}

        /* Tableaux */
        [data-testid="stDataFrame"] {{
            border-radius: 12px;
            overflow: hidden;
        }}

        /* --- Sidebar --- */
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #0a0f1c 0%, var(--bg-card) 100%);
            border-right: 1px solid var(--border);
        }}
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {{
            color: var(--text-primary) !important;
        }}
        [data-testid="stSidebar"] table th {{
            color: var(--text-secondary) !important;
            font-weight: 600;
        }}
        [data-testid="stSidebar"] table td {{
            color: var(--text-primary) !important;
        }}
        [data-testid="stSidebar"] code {{
            color: {COLORS["cyan"]} !important;
            background: rgba(6, 182, 212, 0.12) !important;
            padding: 0.1rem 0.35rem;
            border-radius: 4px;
        }}

        /* Boutons sidebar */
        [data-testid="stSidebar"] .stButton > button {{
            background: #2563eb !important;
            color: #ffffff !important;
            border: none !important;
            font-weight: 600;
        }}
        [data-testid="stSidebar"] .stLinkButton a {{
            background: #1e3a5f !important;
            color: #ffffff !important;
            border: 1px solid #3b82f6 !important;
            font-weight: 600;
            white-space: nowrap !important;
        }}

        .block-container {{
            padding-top: 1.5rem;
            max-width: 1400px;
        }}

        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="DM Sans, sans-serif", color=COLORS["text"], size=13),
        margin=dict(l=12, r=12, t=36, b=12),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(gridcolor="rgba(42,54,85,0.5)", zeroline=False),
        yaxis=dict(gridcolor="rgba(42,54,85,0.5)", zeroline=False),
        colorway=CHART_PALETTE,
    )
    fig.update_traces(
        hoverlabel=dict(bgcolor=COLORS["card"], font_color=COLORS["text"], bordercolor=COLORS["border"])
    )
    return fig


def render_kpi_card(icon: str, value: int, label: str) -> None:
    """Carte KPI en HTML : Streamlit tronque les libellés de st.metric (ellipsis)."""
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{icon} {label}</div>
            <div class="kpi-value">{value:,}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_row(
    nb_traites: int,
    nb_attente: int,
    nb_queue: int,
    nb_hdfs: int,
    nb_en_cours: int,
    nb_erreurs: int,
) -> None:
    cards = [
        ("✅", nb_traites, "Entreprises traitées"),
        ("⏳", nb_attente, "En attente scrape"),
        ("📋", nb_queue, "File scraping"),
        ("📁", nb_hdfs, "Documents HDFS"),
        ("🔄", nb_en_cours, "Scrapes du jour"),
        ("⚠️", nb_erreurs, "Erreurs (24h)"),
    ]
    for row_start in (0, 3):
        cols = st.columns(3)
        for col, (icon, value, label) in zip(cols, cards[row_start : row_start + 3]):
            with col:
                render_kpi_card(icon, value, label)


def render_source_health(source_df: pd.DataFrame) -> None:
    st.subheader("🏥 Santé par source")
    cols = st.columns(3)
    sources_meta = {
        "kbo": "KBO",
        "moniteur": "Moniteur Belge",
        "bnb": "Banque Nationale",
    }
    for i, (source, label) in enumerate(sources_meta.items()):
        row = source_df[source_df["source"] == source] if not source_df.empty else pd.DataFrame()
        with cols[i]:
            if row.empty:
                st.caption(f"**{label}** — pas de données")
                continue
            total = int(row["total"].iloc[0])
            ok = int(row["ok"].iloc[0])
            pct = (ok / total) if total else 0
            badge = "🟢" if pct >= 0.8 else "🟡" if pct >= 0.5 else "🔴"
            st.markdown(f"**{label}** {badge}")
            st.progress(min(pct, 1.0))
            st.caption(f"{ok:,} / {total:,} succès ({pct * 100:.0f} %)")


def query_df(sql: str, params: dict | list | tuple | None = None) -> pd.DataFrame:
    with psycopg2.connect(DB_URL) as conn:
        return pd.read_sql(sql, conn, params=params)


def execute_sql(sql: str, params: list | tuple | None = None) -> None:
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


@st.cache_data(ttl=REFRESH_SECONDS)
def load_postal_analytics() -> pd.DataFrame:
    return query_df(
        """
        SELECT
            COALESCE(NULLIF(TRIM(postal_code), ''), 'non renseigné') AS code_postal,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'active') AS actives,
            COUNT(*) FILTER (WHERE status IN ('closed', 'radiated', 'inactive')) AS fermees
        FROM companies
        WHERE is_deleted = FALSE
        GROUP BY COALESCE(NULLIF(TRIM(postal_code), ''), 'non renseigné')
        ORDER BY total DESC, code_postal
        """
    )


def sync_postal_codes_from_kbo() -> int:
    """Recharge kbo_address si besoin et copie zipcode → companies.postal_code."""
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM kbo_address")
            addr_rows = int(cur.fetchone()[0])

    if addr_rows == 0:
        try:
            from db.kbo_loader import load_kbo_addresses_for_loaded_enterprises, sync_postal_codes_to_companies

            load_kbo_addresses_for_loaded_enterprises(db_url=DB_URL)
            return sync_postal_codes_to_companies(DB_URL)
        except Exception as exc:
            st.warning(f"Import adresses KBO impossible : {exc}")
            return 0

    execute_sql(
        """
        UPDATE companies c
        SET postal_code = sub.zipcode
        FROM (
            SELECT DISTINCT ON (REPLACE(a.entity_number, '.', ''))
                REPLACE(a.entity_number, '.', '') AS bce_number,
                NULLIF(TRIM(a.zipcode), '') AS zipcode
            FROM kbo_address a
            WHERE a.type_of_address = 'REGO'
              AND NULLIF(TRIM(a.zipcode), '') IS NOT NULL
            ORDER BY REPLACE(a.entity_number, '.', ''), a.date_striking_off NULLS FIRST
        ) sub
        WHERE c.bce_number = sub.bce_number
          AND (c.postal_code IS NULL OR TRIM(c.postal_code) = '')
        """
    )
    stats = query_df(
        "SELECT COUNT(*) AS n FROM companies "
        "WHERE postal_code IS NOT NULL AND TRIM(postal_code) <> '' AND is_deleted = FALSE"
    )
    return int(stats["n"].iloc[0]) if not stats.empty else 0


@st.cache_data(ttl=REFRESH_SECONDS)
def load_overview() -> dict:
    snap = query_df(
        "SELECT * FROM monitoring_snapshots ORDER BY timestamp DESC LIMIT 1"
    )
    stats = query_df(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE last_scraped IS NOT NULL) AS traitees,
            COUNT(*) FILTER (WHERE last_scraped IS NULL) AS en_attente,
            COUNT(*) FILTER (WHERE is_archived) AS archivees
        FROM companies WHERE is_deleted = FALSE
        """
    )
    errors_24h = query_df(
        "SELECT COUNT(*) AS cnt FROM scrape_errors WHERE created_at >= NOW() - INTERVAL '24 hours'"
    )
    queue = query_df(
        "SELECT COUNT(*) AS cnt FROM scrape_queue WHERE processed = FALSE"
    )
    hdfs = query_df(
        "SELECT COUNT(*) AS cnt FROM scrape_metadata WHERE status = 'success'"
    )
    discovery = query_df(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE processed) AS traitees,
            COUNT(*) FILTER (WHERE NOT processed) AS en_attente
        FROM discovery_queue
        """
    )
    return {
        "snap": snap,
        "stats": stats,
        "errors_24h": errors_24h,
        "queue": queue,
        "hdfs": hdfs,
        "discovery": discovery,
    }


def render_overview(data: dict) -> None:
    stats = data["stats"]
    snap = data["snap"]
    nb_traites = int(stats["traitees"].iloc[0]) if not stats.empty else 0
    nb_attente = int(stats["en_attente"].iloc[0]) if not stats.empty else 0
    nb_archivees = int(stats["archivees"].iloc[0]) if not stats.empty else 0
    nb_erreurs = int(data["errors_24h"]["cnt"].iloc[0]) if not data["errors_24h"].empty else 0
    nb_queue = int(data["queue"]["cnt"].iloc[0]) if not data["queue"].empty else 0
    nb_hdfs = int(data["hdfs"]["cnt"].iloc[0]) if not data["hdfs"].empty else 0

    if not snap.empty:
        row = snap.iloc[0]
        nb_en_cours = int(row.get("nb_en_cours", 0))
        nb_decouvertes = int(row.get("nb_decouvertes", 0))
    else:
        nb_en_cours = 0
        nb_decouvertes = 0

    render_kpi_row(nb_traites, nb_attente, nb_queue, nb_hdfs, nb_en_cours, nb_erreurs)

    total_co = nb_traites + nb_attente
    taux_traitement = (nb_traites / total_co * 100) if total_co else 0
    st.caption(
        f"Archivées : {nb_archivees:,} · Découvertes : {nb_decouvertes:,} · "
        f"Taux traitement : {taux_traitement:.1f} %"
    )

    col_l, col_r = st.columns(2)
    with col_l:
        with st.container(border=True):
            st.subheader("📈 Évolution des scrapes (14 jours)")
            timeline = query_df(
                """
                SELECT DATE(scraped_at) AS jour, COUNT(*) AS nb
                FROM scrape_metadata
                WHERE scraped_at >= NOW() - INTERVAL '14 days'
                GROUP BY DATE(scraped_at) ORDER BY jour
                """
            )
            if not timeline.empty:
                fig = px.area(
                    timeline,
                    x="jour",
                    y="nb",
                    markers=True,
                    line_shape="spline",
                    color_discrete_sequence=[COLORS["blue"]],
                )
                fig.update_traces(
                    fill="tozeroy",
                    fillcolor="rgba(59,130,246,0.25)",
                    line=dict(width=3),
                    marker=dict(size=8),
                )
                st.plotly_chart(style_figure(fig), use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("Aucune donnée de scraping sur les 14 derniers jours.")

    with col_r:
        with st.container(border=True):
            st.subheader("🥧 Répartition des statuts")
            dist = query_df(
                """
                SELECT COALESCE(status, 'unknown') AS status, COUNT(*) AS nb
                FROM companies WHERE is_deleted = FALSE AND is_archived = FALSE
                GROUP BY status
                """
            )
            if not dist.empty:
                fig = px.pie(
                    dist,
                    names="status",
                    values="nb",
                    hole=0.45,
                    color_discrete_sequence=CHART_PALETTE,
                )
                fig.update_traces(
                    textposition="inside",
                    textinfo="percent+label",
                    pull=[0.02] * len(dist),
                    hovertemplate="<b>%{label}</b><br>%{value:,} entreprises<br>%{percent}<extra></extra>",
                )
                st.plotly_chart(style_figure(fig), use_container_width=True, config={"displayModeBar": False})

    source_df = query_df(
        """
        SELECT source,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status = 'success' AND http_code = 200) AS ok
        FROM scrape_metadata GROUP BY source
        """
    )
    with st.container(border=True):
        render_source_health(source_df)


def render_companies() -> None:
    with st.container(border=True):
        st.subheader("🏢 Entreprises")
        st.caption("Liens vers les fiches publiques KBO, Moniteur belge et BNB.")

        filtre = st.radio(
            "Afficher",
            ["Toutes", "Traitées", "En attente", "Découvertes"],
            horizontal=True,
            label_visibility="collapsed",
        )
        where = "WHERE c.is_deleted = FALSE"
        if filtre == "Traitées":
            where += " AND c.last_scraped IS NOT NULL"
        elif filtre == "En attente":
            where += " AND c.last_scraped IS NULL AND c.is_archived = FALSE"
        elif filtre == "Découvertes":
            where += " AND c.source = 'discovered'"

        search = st.text_input("Rechercher BCE ou nom", key="companies_search", placeholder="Ex. 0203430576")
        companies = query_df(
            f"""
            SELECT c.bce_number, c.name, c.status, c.source, c.postal_code,
                   c.last_scraped, c.is_archived
            FROM companies c
            {where}
            ORDER BY c.last_scraped DESC NULLS LAST, c.bce_number
            LIMIT 200
            """
        )
        if companies.empty:
            st.info("Aucune entreprise pour ce filtre.")
            return
        if search.strip():
            mask = companies["bce_number"].astype(str).str.contains(search, case=False, na=False) | companies[
                "name"
            ].astype(str).str.contains(search, case=False, na=False)
            companies = companies[mask]
            if companies.empty:
                st.warning("Aucun résultat pour cette recherche.")
                return

        display = add_bce_public_links(companies)
        show_linked_dataframe(
            display,
            BCE_LINK_COLUMNS,
            column_order=[
                "bce_number",
                "name",
                "status",
                "source",
                "postal_code",
                "last_scraped",
                "is_archived",
                *BCE_LINK_COLUMNS,
            ],
        )


def render_analytics() -> None:
    with st.container(border=True):
        st.subheader("📊 Indicateurs analytiques")

        cp_stats = query_df(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE postal_code IS NOT NULL AND TRIM(postal_code) <> ''
                ) AS avec_cp
            FROM companies WHERE is_deleted = FALSE
            """
        )
        total_co = int(cp_stats["total"].iloc[0]) if not cp_stats.empty else 0
        avec_cp = int(cp_stats["avec_cp"].iloc[0]) if not cp_stats.empty else 0

        c_sync, _ = st.columns([1, 3])
        with c_sync:
            if st.button("🔄 Synchroniser codes postaux (KBO)", use_container_width=True):
                with st.spinner("Synchronisation…"):
                    n = sync_postal_codes_from_kbo()
                    st.cache_data.clear()
                st.success(f"{n:,} entreprises avec code postal.")
                st.rerun()
        st.caption(
            f"Codes postaux renseignés : **{avec_cp:,}** / **{total_co:,}** entreprises "
            "(source : KBO Open Data `address.csv` + fiches scrapées)."
        )

        postal = load_postal_analytics()
        nace = query_df(
            "SELECT code_nace, libelle, total_entreprises FROM analytics_by_nace ORDER BY total_entreprises DESC LIMIT 20"
        )
        ratio = query_df(
            "SELECT date, taux_ouvertes, taux_fermees FROM analytics_open_closed_ratio ORDER BY computed_at DESC LIMIT 5"
        )
        temporal = query_df(
            "SELECT mois, nouvelles_entreprises, fermees_mois FROM analytics_temporal ORDER BY mois"
        )

        if postal.empty and nace.empty:
            st.warning(
                "Aucune donnée analytics. Cliquez sur **Synchroniser codes postaux** ou lancez "
                "`dag_t_kbo_seed_companies` puis `dag_pipeline_analytics`."
            )
            return

        view = st.radio(
            "Vue",
            ["Codes postaux", "Secteurs NACE", "Évolution temporelle"],
            horizontal=True,
            label_visibility="collapsed",
        )

        if view == "Codes postaux":
            if postal.empty:
                st.info("Aucun code postal — synchronisez depuis KBO.")
            else:
                postal_chart = postal[postal["code_postal"] != "non renseigné"].head(20)
                if postal_chart.empty:
                    postal_chart = postal.head(15)
                fig = px.bar(
                    postal_chart,
                    x="code_postal",
                    y="total",
                    color="actives",
                    color_continuous_scale=["#1e3a5f", COLORS["blue"], COLORS["cyan"]],
                    labels={
                        "code_postal": "Code postal",
                        "total": "Entreprises",
                        "actives": "Actives",
                    },
                )
                fig.update_layout(coloraxis_showscale=False, showlegend=False)
                st.plotly_chart(style_figure(fig), use_container_width=True, config={"displayModeBar": False})
                st.dataframe(postal, hide_index=True, use_container_width=True)

                codes = [
                    cp
                    for cp in postal["code_postal"].tolist()
                    if cp and cp != "non renseigné"
                ]
                if codes:
                    selected_cp = st.selectbox(
                        "Entreprises par code postal",
                        codes,
                        format_func=lambda cp: f"{cp} ({int(postal.loc[postal['code_postal'] == cp, 'total'].iloc[0]):,} ent.)",
                    )
                    if selected_cp:
                        entreprises_cp = query_df(
                            """
                            SELECT bce_number, name, status, source, address, postal_code,
                                   last_scraped
                            FROM companies
                            WHERE is_deleted = FALSE
                              AND TRIM(postal_code) = %s
                            ORDER BY name NULLS LAST, bce_number
                            LIMIT 200
                            """,
                            (selected_cp,),
                        )
                        st.markdown(f"**{len(entreprises_cp):,} entreprises** — code postal **{selected_cp}**")
                        if entreprises_cp.empty:
                            st.info("Aucune entreprise pour ce code postal.")
                        else:
                            cp_display = add_bce_public_links(entreprises_cp)
                            show_linked_dataframe(
                                cp_display,
                                BCE_LINK_COLUMNS,
                                column_order=[
                                    "bce_number",
                                    "name",
                                    "postal_code",
                                    "address",
                                    "status",
                                    "source",
                                    "last_scraped",
                                    *BCE_LINK_COLUMNS,
                                ],
                            )

        elif view == "Secteurs NACE" and not nace.empty:
            fig = px.bar(
                nace.head(15),
                x="total_entreprises",
                y="code_nace",
                orientation="h",
                color="total_entreprises",
                color_continuous_scale=["#1e3a5f", COLORS["purple"]],
                labels={"total_entreprises": "Entreprises", "code_nace": "NACE"},
            )
            fig.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(style_figure(fig), use_container_width=True, config={"displayModeBar": False})
            with st.expander("Détail secteurs"):
                st.dataframe(nace, hide_index=True, use_container_width=True)

        elif view == "Évolution temporelle" and not temporal.empty:
            fig = px.bar(
                temporal,
                x="mois",
                y=["nouvelles_entreprises", "fermees_mois"],
                barmode="group",
                color_discrete_map={
                    "nouvelles_entreprises": COLORS["green"],
                    "fermees_mois": COLORS["red"],
                },
            )
            st.plotly_chart(style_figure(fig), use_container_width=True, config={"displayModeBar": False})

        if not ratio.empty:
            with st.expander("Ratio ouvert / fermé"):
                st.dataframe(ratio, hide_index=True, use_container_width=True)


def render_discovery() -> None:
    with st.container(border=True):
        st.subheader("🔍 Découverte dynamique")

        stats = query_df(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE processed) AS traitees,
                COUNT(*) FILTER (WHERE NOT processed) AS en_attente
            FROM discovery_queue
            """
        )
        if not stats.empty:
            s = stats.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Total découvertes", f"{int(s['total']):,}")
            c2.metric("Traitées", f"{int(s['traitees']):,}")
            c3.metric("En attente", f"{int(s['en_attente']):,}")

        filtre = st.selectbox("Filtrer", ["Toutes", "Traitées", "En attente"], label_visibility="collapsed")
        where = ""
        if filtre == "Traitées":
            where = "WHERE d.processed = TRUE"
        elif filtre == "En attente":
            where = "WHERE d.processed = FALSE"

        disc = query_df(
            f"""
            SELECT d.discovered_at, d.discovered_bce, d.reason, d.processed,
                   c.bce_number AS source_bce, c.name AS source_name
            FROM discovery_queue d
            LEFT JOIN companies c ON c.id = d.source_company_id
            {where}
            ORDER BY d.discovered_at DESC
            LIMIT 100
            """
        )
        if disc.empty:
            st.info("Aucune découverte — elles apparaissent après parsing KBO/Moniteur.")
        else:
            disc_display = disc.copy()
            disc_display["processed"] = disc_display["processed"].map({True: "✅", False: "⏳"})
            disc_display["Lien BCE découvert"] = disc_display["discovered_bce"].astype(str).map(kbo_public_url)
            disc_display["Lien entreprise source"] = disc_display["source_bce"].astype(str).map(kbo_public_url)
            show_linked_dataframe(
                disc_display,
                ("Lien BCE découvert", "Lien entreprise source"),
                column_order=[
                    "discovered_at",
                    "discovered_bce",
                    "Lien BCE découvert",
                    "reason",
                    "processed",
                    "source_bce",
                    "source_name",
                    "Lien entreprise source",
                ],
            )

        st.markdown("**File de scraping interne**")
        queue = query_df(
            """
            SELECT bce_number, reason, priority, queued_at
            FROM scrape_queue WHERE processed = FALSE
            ORDER BY priority DESC, queued_at
            LIMIT 50
            """
        )
        if queue.empty:
            st.success("File vide — tout est à jour.")
        else:
            queue_display = add_bce_public_links(queue)
            show_linked_dataframe(
                queue_display,
                BCE_LINK_COLUMNS,
                column_order=["bce_number", "reason", "priority", "queued_at", *BCE_LINK_COLUMNS],
            )


def render_history() -> None:
    with st.container(border=True):
        st.subheader("📜 Historique des changements")

        search = st.text_input("Rechercher BCE ou nom", placeholder="Ex. 0123.456.789 ou SPRL...")
        hist = query_df(
            """
            SELECT h.changed_at, c.bce_number, c.name, h.snapshot
            FROM company_history h
            JOIN companies c ON c.id = h.company_id
            ORDER BY h.changed_at DESC
            LIMIT 80
            """
        )
        if hist.empty:
            st.info("Aucun changement enregistré.")
        else:
            if search.strip():
                mask = hist["bce_number"].astype(str).str.contains(search, case=False, na=False) | hist[
                    "name"
                ].astype(str).str.contains(search, case=False, na=False)
                hist = hist[mask]
            display = add_bce_public_links(hist.copy())
            display["snapshot"] = display["snapshot"].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else str(x)
            )
            show_linked_dataframe(
                display,
                BCE_LINK_COLUMNS,
                column_order=["changed_at", "bce_number", "name", *BCE_LINK_COLUMNS, "snapshot"],
            )

        st.subheader("🌐 Derniers scrapes")
        st.caption("Vue détaillée avec références HDFS / MongoDB : onglet **Métadonnées**.")
        source_filter = st.multiselect(
            "Sources",
            ["kbo", "moniteur", "bnb"],
            default=["kbo", "moniteur", "bnb"],
            label_visibility="collapsed",
        )
        if source_filter:
            with psycopg2.connect(DB_URL) as conn:
                meta = pd.read_sql(
                    """
                    SELECT m.scraped_at, c.bce_number, m.source, m.http_code, m.parsed,
                           m.hdfs_path, m.mongo_id
                    FROM scrape_metadata m
                    JOIN companies c ON c.id = m.company_id
                    WHERE m.source = ANY(%s)
                    ORDER BY m.scraped_at DESC
                    LIMIT 50
                    """,
                    conn,
                    params=(source_filter,),
                )
            if meta.empty:
                st.info("Aucun scrape pour ces sources.")
            else:
                meta_display = meta.copy()
                meta_display["Lien source"] = meta_display.apply(
                    lambda r: source_public_url(r["source"], r["bce_number"]), axis=1
                )
                meta_display["Lien HDFS"] = meta_display["hdfs_path"].map(hdfs_explorer_url)
                show_linked_dataframe(
                    meta_display,
                    ("Lien source", "Lien HDFS"),
                    column_order=[
                        "scraped_at",
                        "bce_number",
                        "source",
                        "http_code",
                        "parsed",
                        "hdfs_path",
                        "mongo_id",
                        "Lien source",
                        "Lien HDFS",
                    ],
                )


def render_metadata() -> None:
    with st.container(border=True):
        st.subheader("📎 Métadonnées de scraping")
        st.caption(
            "Références croisées : PostgreSQL (`id`), MongoDB (`mongo_id`), HDFS (`hdfs_path`), "
            "fiche publique de la source."
        )

        source_filter = st.multiselect(
            "Sources",
            ["kbo", "moniteur", "bnb"],
            default=["kbo", "moniteur", "bnb"],
            key="metadata_sources",
        )
        parsed_filter = st.selectbox("Parsing", ["Tous", "Parsés", "Non parsés"], label_visibility="collapsed")
        search = st.text_input(
            "Rechercher BCE, nom ou chemin HDFS",
            key="metadata_search",
            placeholder="Ex. 0203430576 ou /data/companies/",
        )

        sources = source_filter or ["kbo", "moniteur", "bnb"]
        conditions = ["m.source = ANY(%s)"]
        params: list = [sources]
        if parsed_filter == "Parsés":
            conditions.append("m.parsed = TRUE")
        elif parsed_filter == "Non parsés":
            conditions.append("m.parsed = FALSE")

        with psycopg2.connect(DB_URL) as conn:
            meta = pd.read_sql(
                f"""
                SELECT m.id AS ref_postgres, m.mongo_id AS ref_mongo, m.scraped_at,
                       c.bce_number, c.name, m.source, m.http_code, m.status, m.parsed,
                       m.hdfs_path, m.proxy_used, m.attempts
                FROM scrape_metadata m
                JOIN companies c ON c.id = m.company_id
                WHERE {" AND ".join(conditions)}
                ORDER BY m.scraped_at DESC
                LIMIT 150
                """,
                conn,
                params=tuple(params),
            )

        if meta.empty:
            st.info("Aucune métadonnée pour ces filtres.")
            return

        if search.strip():
            mask = (
                meta["bce_number"].astype(str).str.contains(search, case=False, na=False)
                | meta["name"].astype(str).str.contains(search, case=False, na=False)
                | meta["hdfs_path"].astype(str).str.contains(search, case=False, na=False)
                | meta["ref_mongo"].astype(str).str.contains(search, case=False, na=False)
            )
            meta = meta[mask]
            if meta.empty:
                st.warning("Aucun résultat pour cette recherche.")
                return

        meta_display = meta.copy()
        meta_display["Lien source"] = meta_display.apply(
            lambda r: source_public_url(r["source"], r["bce_number"]), axis=1
        )
        meta_display["Lien HDFS"] = meta_display["hdfs_path"].map(hdfs_explorer_url)
        show_linked_dataframe(
            meta_display,
            ("Lien source", "Lien HDFS"),
            column_order=[
                "ref_postgres",
                "ref_mongo",
                "scraped_at",
                "bce_number",
                "name",
                "source",
                "http_code",
                "status",
                "parsed",
                "hdfs_path",
                "Lien HDFS",
                "Lien source",
                "proxy_used",
                "attempts",
            ],
        )


def render_errors() -> None:
    with st.container(border=True):
        st.subheader("🚨 Erreurs récentes")

        types_df = query_df("SELECT DISTINCT error_type FROM scrape_errors ORDER BY 1")
        sources_df = query_df("SELECT DISTINCT source FROM scrape_errors WHERE source IS NOT NULL ORDER BY 1")

        c1, c2 = st.columns(2)
        with c1:
            err_types = st.multiselect(
                "Type d'erreur",
                types_df["error_type"].tolist() if not types_df.empty else [],
                default=types_df["error_type"].tolist() if not types_df.empty else [],
            )
        with c2:
            err_sources = st.multiselect(
                "Source",
                sources_df["source"].tolist() if not sources_df.empty else [],
                default=sources_df["source"].tolist() if not sources_df.empty else [],
            )

        conditions = ["1=1"]
        if err_types:
            conditions.append("error_type = ANY(%s)")
        if err_sources:
            conditions.append("source = ANY(%s)")

        params: list = []
        if err_types:
            params.append(err_types)
        if err_sources:
            params.append(err_sources)

        with psycopg2.connect(DB_URL) as conn:
            errors = pd.read_sql(
                f"""
                SELECT created_at, error_type, source, bce_number, LEFT(message, 200) AS message
                FROM scrape_errors
                WHERE {" AND ".join(conditions)}
                ORDER BY created_at DESC
                LIMIT 50
                """,
                conn,
                params=tuple(params) if params else None,
            )

        if errors.empty:
            st.success("Aucune erreur correspondant aux filtres.")
        else:
            st.metric("Erreurs affichées", len(errors))
            err_display = errors.copy()
            err_display["Lien KBO"] = err_display["bce_number"].astype(str).map(kbo_public_url)
            err_display["Lien source"] = err_display.apply(
                lambda r: source_public_url(r["source"], r["bce_number"])
                if pd.notna(r.get("source")) and str(r.get("source", "")).strip()
                else kbo_public_url(r["bce_number"]),
                axis=1,
            )
            show_linked_dataframe(
                err_display,
                ("Lien KBO", "Lien source"),
                column_order=[
                    "created_at",
                    "error_type",
                    "source",
                    "bce_number",
                    "message",
                    "Lien KBO",
                    "Lien source",
                ],
            )


# --- Layout principal ---
inject_styles()

if st_autorefresh:
    st_autorefresh(interval=REFRESH_SECONDS * 1000, key="dashboard_refresh")

now_str = datetime.now().strftime("%H:%M:%S")
st.title("🇧🇪 Plateforme Entreprises Belges")
st.caption(
    f"Supervision temps réel · Actualisation {now_str} · refresh {REFRESH_SECONDS}s"
)
st.markdown(
    f'<p class="dashboard-links">'
    f'<a href="{AIRFLOW_UI_URL}" target="_blank">⚡ Airflow</a>'
    f'<a href="{HDFS_UI_URL}" target="_blank">📦 HDFS</a>'
    f'<a href="{HDFS_UI_URL.rstrip("/")}/explorer.html#/data/companies" target="_blank">📁 Fichiers entreprises</a>'
    f"</p>",
    unsafe_allow_html=True,
)
st.caption("Onglets **Entreprises**, **Découverte** et **Métadonnées** : liens KBO, Moniteur, BNB et HDFS.")

try:
    overview_data = load_overview()
except Exception as exc:
    st.error(f"Connexion base de données impossible : {exc}")
    st.stop()

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    [
        "📊 Aperçu",
        "🏢 Entreprises",
        "📈 Analytics",
        "🔍 Découverte",
        "📎 Métadonnées",
        "📜 Historique",
        "🚨 Erreurs",
    ]
)

with tab1:
    render_overview(overview_data)
    if not overview_data["snap"].empty:
        with st.expander("Dernier snapshot monitoring"):
            st.json(overview_data["snap"].iloc[0].to_dict())

with tab2:
    render_companies()

with tab3:
    render_analytics()

with tab4:
    render_discovery()

with tab5:
    render_metadata()

with tab6:
    render_history()

with tab7:
    render_errors()

with st.sidebar:
    st.subheader("⚙️ Pipelines Airflow")
    st.markdown(
        """
| Pipeline | Fréquence |
|----------|-----------|
| Import KBO | hebdo |
| Scraping web | horaire |
| Extraction | trigger |
| Cycle de vie | quotidien |
| Analytics | hebdo |
| Monitoring | horaire |
        """
    )
    st.link_button("⚡ Ouvrir Airflow UI", AIRFLOW_UI_URL, use_container_width=True)
    if st.button("🔄 Rafraîchir maintenant", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
