# Plateforme Entreprises Belges

**En une phrase :** cette plateforme récupère automatiquement des informations publiques sur les entreprises belges, les organise, les conserve et les affiche dans un tableau de bord simple à consulter.


---

## Que fait la plateforme ?

Imaginez un assistant qui travaille en continu :

1. **Collecte** les fiches publiques d’entreprises sur trois sites officiels belges (registre des entreprises, Moniteur belge, Banque nationale).
2. **Enregistre** les pages web brutes dans un espace de stockage dédié (HDFS).
3. **Extrait** les informations utiles (nom, adresse, statut, chiffres, etc.) et les range dans une base de données structurée (PostgreSQL).
4. **Suit l’historique** : chaque mise à jour est conservée pour voir ce qui a changé dans le temps.
5. **Met à jour** les entreprises inactives ou fermées, relance le scraping si les données sont trop anciennes.
6. **Calcule des statistiques** (répartition par code postal, secteur d’activité, ratios financiers, etc.).
7. **Affiche tout cela** dans une interface web (tableau de bord Streamlit) et surveille que les traitements se déroulent bien.

Tout cela est **planifié et enchaîné automatiquement** par un outil d’orchestration (Apache Airflow), comme un planning de tâches qui s’exécute seul.

---

## D’où viennent les données ?

| Source | En clair | Site |
|--------|----------|------|
| **KBO / BCE** | Registre officiel des entreprises en Belgique | [kbopub.economie.fgov.be](https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html) |
| **Moniteur belge** | Publications légales (statuts, actes, etc.) — toutes les pages paginées | [ejustice.just.fgov.be](https://www.ejustice.just.fgov.be/cgi_tsv/list.pl) |
| **BNB (CBSO)** | Comptes annuels déposés à la Banque nationale | [consult.cbso.nbb.be](https://consult.cbso.nbb.be/) |

En complément, le projet peut utiliser un **export Open Data du KBO** (fichiers CSV fournis par l’administration) pour alimenter la liste initiale d’entreprises à traiter.

---

## Ce dont vous avez besoin sur votre ordinateur

| Élément | Pourquoi |
|---------|----------|
| **Docker Desktop** | Lance toute la plateforme dans des « conteneurs » isolés, sans installer chaque logiciel à la main. Docker doit être **ouvert et en marche** avant de démarrer. |
| **(Recommandé)** Dossier **KboOpenData** | Jeu de données Open Data du KBO, placé à côté du projet : `../KboOpenData_0335_2026_04_19_Full`. Sans lui, une petite liste d’exemple (`data/companies.csv`) est utilisée à la place. |

---

## Démarrer la plateforme (étape par étape)

### 1. Ouvrir Docker Desktop

Attendez que Docker indique qu’il est prêt (icône verte ou message « Running »).

### 2. Lancer le script de démarrage

**Sous Windows (PowerShell) :**

```powershell
cd belgian-companies-platform
.\scripts\start.ps1
```

**Sous Linux ou macOS :**

```bash
cd belgian-companies-platform
chmod +x scripts/start.sh
./scripts/start.sh
```

Le script vérifie Docker, construit les services si besoin, puis les démarre. La première fois peut prendre **plusieurs minutes** (téléchargement d’images).

**Alternative manuelle :**

```bash
cd belgian-companies-platform
docker compose up -d --build
```

### 3. Ouvrir les interfaces dans votre navigateur

| Interface | Adresse | Identifiants | À quoi ça sert |
|-----------|---------|--------------|----------------|
| **Tableau de bord** (le plus simple à consulter) | http://localhost:8501 | — | Voir les entreprises, statistiques, historique, file d’attente de scraping |
| **Airflow** (suivi des traitements) | http://localhost:8080 | `admin` / `admin` | Voir quelles tâches tournent, réussissent ou échouent |
| **HDFS** (stockage des fichiers bruts) | http://localhost:19870 | — | Explorer les pages HTML enregistrées |
| **PostgreSQL** (base structurée) | `localhost:5432` | voir configuration Docker | Accès direct à la base (outils type DBeaver, pgAdmin) |

> **Conseil :** pour une première découverte sans jargon technique, commencez par **http://localhost:8501** (Streamlit).

### 4. (Première fois) Lancer l’import des données KBO

Dans l’interface Airflow (http://localhost:8080) :

1. Connectez-vous avec `admin` / `admin`.
2. Cherchez le pipeline nommé **`dag_pipeline_kbo`**.
3. Cliquez sur le bouton **▶ Trigger** (lancer manuellement).

Cela importe les entreprises depuis l’Open Data KBO (ou le fichier de repli), puis enchaîne automatiquement les étapes suivantes (scraping, extraction, etc.).

---

## Que se passe-t-il automatiquement ?

Les traitements sont organisés en **pipelines** (enchaînements d’étapes). Voici l’idée en langage courant :

| Pipeline | Fréquence approximative | En résumé |
|----------|-------------------------|-----------|
| **KBO** | Chaque semaine | Importe les données Open Data, prépare la liste des entreprises à traiter, lance le scraping |
| **Scraping** | Chaque heure | Visite les sites web pour récupérer les pages des entreprises en file d’attente |
| **Extraction** | Après le scraping | Lit les fichiers enregistrés et remplit la base de données |
| **Cycle de vie** | Chaque jour | Met à jour les statuts, repère les données périmées, archive les entreprises inactives |
| **Analytique** | Chaque semaine | Recalcule les tableaux de statistiques |
| **Monitoring** | Chaque heure | Prend un instantané de l’état de la plateforme pour la supervision |

Si une étape **échoue**, les étapes suivantes du même enchaînement **ne sont pas lancées** — cela évite de traiter des données incomplètes ou erronées.

---

## Règles importantes (métier)

- Les entreprises ne sont **jamais supprimées** de la base : elles peuvent être **archivées** si elles sont fermées ou inactives.
- Les pages d’**erreur** (site indisponible, accès refusé, etc.) ne sont **pas** conservées comme données valides.
- Chaque page web enregistrée est accompagnée d’un **petit fichier descriptif** (métadonnées).
- Une entreprise dont les données n’ont pas été mises à jour depuis **plus de 14 jours** peut être **rescrapée** automatiquement.
- De **nouvelles entreprises** découvertes lors de l’analyse sont ajoutées à une file d’attente, puis traitées comme les autres.

---

## Petit glossaire

| Terme | Signification simple |
|-------|----------------------|
| **Scraping** | Récupération automatique de pages web publiques |
| **Pipeline** | Suite d’étapes exécutées l’une après l’autre |
| **DAG** | « Recette » d’une tâche ou d’un pipeline dans Airflow |
| **Airflow** | Planificateur qui lance et surveille les traitements |
| **HDFS** | Système de fichiers distribué où sont stockées les pages brutes |
| **PostgreSQL** | Base de données relationnelle (tableaux structurés) |
| **Streamlit** | Application web légère pour afficher graphiques et tableaux |
| **Docker / conteneur** | Boîte logicielle isolée qui tourne de la même façon sur toute machine |
| **BCE / numéro d’entreprise** | Identifiant officiel belge d’une entreprise (10 chiffres) |

---

## Problèmes courants

| Symptôme | Que faire |
|----------|-----------|
| Message du type « Docker n’est pas démarré » | Ouvrir **Docker Desktop**, attendre qu’il soit prêt, relancer `.\scripts\start.ps1` |
| Impossible d’ouvrir http://localhost:8501 ou 8080 | Attendre 2–3 minutes après le démarrage ; vérifier avec `docker compose ps` que les services sont « Up » |
| Port déjà utilisé (9000, 9870, etc.) | Les ports HDFS sont déjà configurés sur **19000** et **19870** dans `.env`. Faire `docker compose down` puis relancer |
| Erreur « Signature has expired » dans Airflow | Vérifier `AIRFLOW_JWT_EXPIRATION=14400` dans le fichier `.env` |
| Scraping KBO sans résultat | Vérifier la connexion internet ; éventuellement lancer d’abord la tâche de préparation (`dag_t_scrape_prepare`) depuis Airflow |
| Droits refusés sur HDFS | Exécuter : `docker compose run --rm hdfs-init` puis `docker compose run --rm airflow-init` |
| DAG introuvable dans Airflow | `docker compose run --rm airflow-init` puis redémarrer : `docker compose restart airflow-dag-processor airflow-scheduler airflow-worker` |

**Repartir de zéro** (efface toutes les données locales des conteneurs) :

```bash
docker compose down -v
```

Puis relancer le script de démarrage.

---

## Pour les développeurs

Les sections ci-dessous conservent le niveau de détail technique du projet.

### Objectifs couverts

| Objectif | Implémentation |
|----------|----------------|
| Orchestration automatique des pipelines | 6 pipelines `dag_pipeline_*` + 26 DAGs tâches `dag_t_*` |
| Gestion des dépendances entre étapes | Chaînes séquentielles `TriggerDagRunOperator` + `wait_for_completion=True` (fail-fast) |
| Stockage documents bruts HDFS | `/data/companies/{bce}/{source}/{timestamp}.html` + `.json` |
| Extraction et structuration | Parsers KBO / Moniteur / BNB → PostgreSQL |
| Historique scrapes et changements | `scrape_metadata` + `company_history` (snapshot JSON) |
| Supervision plateforme | `dag_pipeline_monitoring` + dashboard Streamlit |
| Indicateurs analytiques | `dag_pipeline_analytics` + tables `analytics_*` |
| Découverte dynamique | `discovery_queue` + `entity_linker` + file `scrape_queue` |
| Entreprises fermées / inactives | Parsing statut + sync KBO + lifecycle (archive) |
| Interface admin temps réel | Streamlit :8501 (refresh 30s, onglets analytics/découverte/historique) |

### Architecture

```
CSV / KBO Open Data → dag_pipeline_kbo → dag_pipeline_scraping → HDFS
                              ↓                      ↓
                       dag_pipeline_extraction → PostgreSQL
                              ↓
        dag_pipeline_lifecycle / dag_pipeline_analytics / dag_pipeline_monitoring
                              ↓
                    Dashboard Streamlit :8501
```

Chaque étape métier est un **DAG tâche** (`dag_t_*`). Les **pipelines** (`dag_pipeline_*`) les enchaînent en **séquence** : si une tâche échoue, les suivantes ne partent pas.

### Stack technique

- **Apache Airflow 3.0.5** (api-server, scheduler, dag-processor, triggerer, worker)
- Python 3.11 · CeleryExecutor · PostgreSQL · Redis · HDFS · Streamlit

### Commandes utiles

Réinitialiser les DAGs après mise à jour du code :

```bash
docker compose run --rm airflow-init
docker compose restart airflow-dag-processor airflow-scheduler airflow-worker
```

Lancer le pipeline KBO en ligne de commande :

```bash
docker compose exec airflow-apiserver airflow dags trigger dag_pipeline_kbo
```

Lister les DAGs :

```bash
docker compose exec airflow-apiserver airflow dags list
```

Migration Airflow 2 → 3 : `docker compose down` puis `docker compose up airflow-init`.

### Pipelines (détail)

| Pipeline | Schedule | Chaîne |
|----------|----------|--------|
| `dag_pipeline_kbo` | `@weekly` | import → seed → vues → queue → **scraping** |
| `dag_pipeline_scraping` | `@hourly` | prepare → KBO → Moniteur → BNB → **extraction** |
| `dag_pipeline_extraction` | trigger | list → parse KBO → Moniteur → BNB |
| `dag_pipeline_lifecycle` | `@daily` | sync KBO → stale → inactive → rescrape → archive → découvertes |
| `dag_pipeline_analytics` | `@weekly` | clear → postal → nace → financial → ratio → temporal |
| `dag_pipeline_monitoring` | `@hourly` | snapshot |

### DAGs tâches (`dag_t_*`)

| Domaine | DAGs |
|---------|------|
| KBO | `dag_t_kbo_import_data`, `dag_t_kbo_seed_companies`, `dag_t_kbo_create_views`, `dag_t_kbo_sync_queue` |
| Scraping | `dag_t_scrape_prepare`, `dag_t_scrape_kbo`, `dag_t_scrape_moniteur`, `dag_t_scrape_bnb` |
| Extraction | `dag_t_extract_list_files`, `dag_t_extract_parse_kbo`, `dag_t_extract_parse_moniteur`, `dag_t_extract_parse_bnb` |
| Lifecycle | `dag_t_lifecycle_sync_kbo`, `dag_t_lifecycle_find_stale`, `dag_t_lifecycle_find_inactive`, `dag_t_lifecycle_rescrape`, `dag_t_lifecycle_archive`, `dag_t_lifecycle_process_discoveries` |
| Analytics | `dag_t_analytics_clear`, `dag_t_analytics_postal`, `dag_t_analytics_nace`, `dag_t_analytics_financial`, `dag_t_analytics_ratio`, `dag_t_analytics_temporal` |
| Monitoring | `dag_t_monitoring_snapshot` |

### Structure du code

```
dags/
  common.py              # factory DAG + triggers fail-fast
  task_dags.py           # enregistrement dag_t_*
  pipeline_*.py          # orchestrateurs
  tasks/                 # callables métier
scraper/                 # HTTP, validation, HDFS
extractor/               # parsers + découverte dynamique
db/                      # modèles, repository, SQL KBO
analytics/               # moteur de rapports
dashboard/               # Streamlit
data/companies.csv       # repli si KBO absent
```

### Données KBO Open Data

Le dossier `KboOpenData_0335_2026_04_19_Full` est monté sur `/kbo_data`.

Scripts SQL dans `db/kbo_opendata/` :

| Fichier | Rôle |
|---------|------|
| `01_schema.sql` | Tables `kbo_*` |
| `02_seed_companies.sql` | Remplit `companies` depuis KBO |
| `03_analytics_views.sql` | Vues `v_analytics_*` et `v_kbo_bce_queue` |

Variables (`.env` ou docker-compose) :

| Variable | Défaut | Description |
|----------|--------|-------------|
| `KBO_IMPORT_LIMIT` | 500 | Limite lignes CSV (dev). `0` = import complet |
| `KBO_SCRAPE_QUEUE_LIMIT` | 500 | BCE mis en file par run KBO |
| Scraping batch | — | Toutes les entreprises « stale » du run sont traitées (plus de limite à 50) |
| `AIRFLOW_JWT_EXPIRATION` | 14400 | Durée JWT (s) pour pipelines longs |
| `PROXY_USE_CACHE_FILE` | true | Charge **tous** les proxies de `data/proxies_active.txt` (pas un seul) |
| `PROXY_AUTO_FETCH` | false | `true` = re-télécharge ProxyScrape à chaque tâche (lent) |
| `PROXY_HTTPS_HTTP_ONLY` | true | BNB/KBO HTTPS : n'utilise que les proxies `http://` (évite erreurs SOCKS SSL) |
| `PROXY_MAX_ROTATIONS` | 30 | Nouveau proxy à **chaque** échec de requête |
| `PROXY_VALIDATE` | true | Ne garde que les proxies qui répondent (TCP + HTTP, plusieurs URLs) |
| `PROXY_VALIDATE_URLS` | httpbin + google | URLs testées (HTTP **avant** HTTPS) |
| `PROXY_VALIDATE_MAX` | 0 | `0` = tester **toute** la liste ; sinon limite (ex. 500) |
| `PROXY_LIST_FILE` | — | Fichier `ip:port` déjà vérifié (ex. `data/proxies_input.txt`) |
| `PROXY_CACHE_FILE` | `data/proxies_active.txt` | Liste complète des proxies actifs validés |

Rafraîchir manuellement le cache des proxies :

```bash
# Rapide (~45 actifs HTTP en 1 min)
python scripts/refresh_proxies.py

# Complet HTTP+SOCKS (~500+ actifs, ~13 min)
$env:PROXY_FETCH_PROTOCOLS="http,socks5,socks4"; python scripts/refresh_proxies.py
```

Liste déjà vérifiée (comme sur votre capture, 38/99 actifs) : copiez les `ip:port` dans `data/proxies_input.txt`, puis :

```bash
$env:PROXY_LIST_FILE="data/proxies_input.txt"; python scripts/refresh_proxies.py
```

Après modification de `requirements.txt` (PySocks) : `docker compose build` puis redémarrage des services Airflow.

### Règles métier (référence technique)

- Jamais de suppression d'entreprise (`is_deleted=False`, archivage via `is_archived`)
- Pages d'erreur jamais stockées sur HDFS
- Chaque HTML a son fichier `.json` de métadonnées
- Rescraping automatique si `last_scraped` > 14 jours
- Découverte dynamique : `discovery_queue` → `scrape_queue` → scraping
- Sync quotidienne des statuts depuis `kbo_enterprise` (AC/ST/AF)
