# Les concepts clés, expliqués avec les données du projet

Ce document explique les notions du cours à partir de ce que fait réellement le projet. Les exemples utilisent les clients du cours (Alice, Bob, Carol, David, Eve).

## Sommaire

1. [ETL et ELT](#1-etl-et-elt)
2. [Le traitement incrémental et les watermarks](#2-le-traitement-incrémental-et-les-watermarks)
3. [Le SCD2 : garder l'historique](#3-le-scd2--garder-lhistorique)
4. [La qualité des données et la porte qualité](#4-la-qualité-des-données-et-la-porte-qualité)
5. [Idempotence, transactions et UPSERT](#5-idempotence-transactions-et-upsert)
6. [Checkpoints et reprise après panne](#6-checkpoints-et-reprise-après-panne)
7. [Relances et attente exponentielle](#7-relances-et-attente-exponentielle)
8. [Airflow](#8-airflow)
9. [Spark](#9-spark)
10. [dbt](#10-dbt)
11. [Docker en 5 minutes](#11-docker-en-5-minutes)

---

## 1. ETL et ELT

- **ETL** (Extract, Transform, Load) : on transforme les données *avant* de les charger. Ici, la partie Python : extraction, contrôle, SCD2, agrégats, puis chargement.
- **ELT** (Extract, Load, Transform) : on charge d'abord les données brutes dans l'entrepôt, puis on les transforme *dans* l'entrepôt avec du SQL. Ici, la partie dbt : elle lit les tables chargées et construit les tables d'analyse.

Le projet combine les deux, comme beaucoup d'entreprises : Python pour l'ingestion et la logique délicate (SCD2, qualité), SQL avec dbt pour les tables d'analyse.

Les tables sont organisées en couches :

| Couche | Tables | Rôle |
|---|---|---|
| Source | `customers` | la base de l'application (on ne fait que la lire) |
| Brute (raw) | `raw_orders`, `raw_order_items`, `raw_products` | copie fidèle et typée des sources |
| Entrepôt | `dim_customers`, `fact_daily_orders`, `fact_product_performance` | modèle en étoile : dimension + faits |
| Analyse (marts) | `dbt_marts.*` | tables prêtes pour un tableau de bord |

Une **dimension** décrit le « qui / quoi » (le client), une table de **faits** contient les mesures (montants, quantités) à un certain **grain** (ici : un jour et un client).

---

## 2. Le traitement incrémental et les watermarks

**Problème.** Relire chaque jour toute la base clients et toutes les commandes depuis 2024 coûte de plus en plus cher à mesure que l'historique grossit.

**Solution.** Ne traiter que ce qui est nouveau ou modifié depuis la dernière exécution réussie. Pour savoir « où on s'est arrêté », on mémorise un **watermark** (niveau des plus hautes eaux) par source, dans `data/state/watermarks.json`.

### Exemple réel (les deux exécutions du projet)

| | Clients | Commandes | Catalogue |
|---|---|---|---|
| **1re exécution** | pas de watermark : tout est lu (5 clients) | tout est lu (144 commandes) | fichier lu (5 produits) |
| Watermark enregistré | `2024-02-01T10:00:00` (max de `last_updated`) | `2024-02-29` (dernière date de commande) | empreinte SHA-256 du fichier |
| *Activité simulée* | *Bob et Carol modifiés, 3 nouveaux clients* | *4 nouvelles commandes* | *aucun changement* |
| **2e exécution** | `WHERE last_updated > '2024-02-01T10:00:00'` : **5 lignes** | `date_from=2024-02-29` : **7 commandes** | empreinte identique : **ignoré** |

### Trois détails qui font la différence

1. **Le watermark n'avance qu'après le COMMIT du chargement.** Si le chargement échoue, la prochaine exécution reprend les mêmes données : rien n'est perdu.
2. **Pour les commandes, le dernier jour est retraité en entier** (`date_from` inclus). Les agrégats sont calculés par jour : si on ne relisait que les commandes « après » une heure précise, le total d'un jour serait recalculé à partir d'une partie de ses commandes seulement. En relisant le jour complet, le total est toujours juste, et l'UPSERT évite les doublons.
3. **Pour les clients, la comparaison est stricte** (`>`) : un client déjà traité n'est pas relu.

### Le traitement incrémental à chaque niveau

| Niveau | Mécanisme |
|---|---|
| Extraction | watermarks (clients, commandes) et empreinte de fichier (catalogue) |
| SCD2 | on ne relit que les versions des clients du lot ; un client renvoyé sans vrai changement ne crée rien |
| Agrégats | seuls les jours présents dans le lot sont recalculés |
| Chargement | UPSERT des seules lignes du lot, jamais de rechargement complet |
| dbt | le modèle `fct_orders_enriched` est `incremental` : il ne traite que les commandes chargées depuis son dernier passage |

### Limites à connaître

- Une **suppression physique** dans la source (`DELETE`) n'est pas visible par une extraction incrémentale : il n'y a plus de ligne à lire. Elle n'est détectée qu'en rechargement complet (`python pipeline.py --full-refresh`). En pratique, les applications font plutôt des suppressions « logiques » (`status = 'inactive'`), qui sont bien suivies.
- Une commande **modifiée après coup** avec une date ancienne n'est pas relue (le filtre porte sur la date de commande). Une API réelle fournirait un champ `updated_at`, que l'on utiliserait comme watermark.

---

## 3. Le SCD2 : garder l'historique

Une **Slowly Changing Dimension** est une dimension dont les attributs changent de temps en temps (e-mail, statut...). Il existe plusieurs façons de gérer ces changements :

- **type 1** : on écrase l'ancienne valeur, l'historique est perdu ;
- **type 2 (SCD2)** : on garde chaque version, avec sa période de validité.

### Exemple réel : Bob change d'adresse e-mail le 6 octobre 2026

Avant :

| clé | client | e-mail | statut | début | fin | courante |
|---|---|---|---|---|---|---|
| 4 | 1002 | bob.smith@email.com | active | 2023-02-20 | 9999-12-31 | oui |

Après l'exécution suivante :

| clé | client | e-mail | statut | début | fin | courante |
|---|---|---|---|---|---|---|
| 4 | 1002 | bob.smith@email.com | active | 2023-02-20 | **2026-10-06** | **non** |
| 6 | 1002 | **bob.smith@newmail.com** | active | **2026-10-06** | 9999-12-31 | oui |

L'ancienne version est **fermée** (date de fin + `is_current = false`) et une nouvelle version est **insérée**. On ne modifie jamais une information du passé.

La « clé » (`dim_customer_key`) est une **clé de substitution** : un numéro propre à l'entrepôt, différent pour chaque version. L'identifiant métier (`customer_id`) reste le même d'une version à l'autre.

### La convention des dates : intervalle semi-ouvert

Une version est valide le jour J si `début <= J < fin`. La version fermée se termine le jour même où la suivante commence : il n'y a ni trou, ni chevauchement. La date `9999-12-31` signifie « toujours valide ». Deux tests dbt vérifient ces règles à chaque exécution.

### Pourquoi c'est utile : la jointure « point-in-time »

Carol était `inactive`, puis redevient `active` le 6 octobre. Avec le SCD2, chaque commande est rattachée au statut qu'avait Carol **au moment de la commande** :

```sql
SELECT o.order_id, o.order_date, c.status
FROM raw_orders o
JOIN dim_customers c
  ON c.customer_id = o.customer_id
 AND o.order_date >= c.effective_start_date
 AND o.order_date <  c.effective_end_date;
```

Résultat réel : ses commandes de février 2024 restent `inactive`, celle du 6 octobre 2026 est `active`. Sans SCD2, toutes ses commandes passées seraient comptées comme « active », ce qui fausserait l'analyse par segment.

### Comment le code détecte un changement

Pour chaque client, on calcule une **empreinte** (hash MD5) des colonnes suivies (nom, e-mail, statut), après nettoyage (espaces, majuscules). On compare ensuite l'empreinte entrante à celle de la version courante. C'est plus sûr qu'une comparaison colonne par colonne : en pandas, `NaN != NaN` vaut `True`, ce qui créerait de faux changements sur les valeurs manquantes.

Les quatre cas traités par `SCD2Processor` :

| Cas | Action | Date de début de la nouvelle version |
|---|---|---|
| nouveau client | insertion | sa date d'inscription |
| client modifié | fermeture + insertion | date du changement (`last_updated`) |
| client réactivé (fermé puis revenu) | insertion | après la fin de sa dernière version |
| client inchangé | rien | |

Pour le premier chargement, on ne connaît pas l'historique antérieur : on suppose que les attributs actuels sont valides depuis l'inscription. C'est une hypothèse classique, documentée dans le code.

### Deux implémentations, pour comparer

- **`SCD2Processor` (Python)** : le travail demandé par le cours, écrit « à la main ». Il remplit `dim_customers`.
- **`dbt snapshot`** : la méthode la plus courante en entreprise. Quelques lignes de configuration (`dbt_project/snapshots/snap_customers.yml`) et dbt gère tout. Il remplit `dbt_snapshots.snap_customers`.

Les deux donnent le même historique. Différence visible : dbt fait commencer la première version à la date de sa première capture (`last_updated`), notre version à la date d'inscription.

---

## 4. La qualité des données et la porte qualité

Des données fausses chargées dans l'entrepôt contaminent tous les tableaux de bord. On les arrête donc **avant** le chargement : c'est la **porte qualité** (quality gate).

Chaque contrôle donne un statut :

- **PASS** : tout va bien ;
- **WARN** : à surveiller mais non bloquant (ex. une commande dont le total inclut des frais de port) ;
- **FAIL** : problème bloquant.

Les règles sont dans `config/pipeline.yaml` (et non dans le code) : colonnes obligatoires, unicité, format d'e-mail, valeurs autorisées, bornes numériques, types attendus.

Décision pour chaque table :

| Situation | Décision |
|---|---|
| colonne obligatoire absente ou type incorrect | **arrêt** (erreur de structure) |
| lignes invalides au-delà du seuil `max_invalid_ratio` | **arrêt** |
| lignes invalides en dessous du seuil | **quarantaine** : ces lignes sont mises de côté dans `rejected_<table>.parquet`, le reste continue |
| aucun problème bloquant | on continue |

Par défaut le seuil vaut 0 (tolérance zéro, le plus sûr). Le rapport de validation est **sauvegardé avant la décision** : en cas d'arrêt, on sait exactement pourquoi (colonnes en cause, nombre de lignes, exemples d'identifiants). Un exemple réel est dans [exemples_rapports/03_porte_qualite_fermee_email_invalide.json](exemples_rapports/03_porte_qualite_fermee_email_invalide.json).

---

## 5. Idempotence, transactions et UPSERT

Une opération est **idempotente** si l'exécuter deux fois donne le même résultat qu'une seule fois. C'est essentiel : en cas de doute (panne, relance automatique), on doit pouvoir rejouer sans risque.

Trois mécanismes la garantissent :

- **la transaction unique** : tout le chargement d'une exécution est validé (COMMIT) ou annulé (ROLLBACK) d'un bloc. Jamais de dimension à moitié mise à jour ;
- **l'UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`) : une ligne déjà présente est mise à jour au lieu d'être dupliquée ;
- **l'empreinte SCD2** : un client renvoyé sans changement réel ne crée pas de nouvelle version.

Garde-fous supplémentaires dans la base : un index unique interdit deux versions courantes pour un même client, et une contrainte interdit une date de fin antérieure à la date de début.

---

## 6. Checkpoints et reprise après panne

Après chaque étape, son résultat est sauvegardé dans `data/checkpoints/<run_id>/` (fichiers Parquet, format en colonnes qui conserve les types) et l'étape est notée « terminée ». À la relance du même `run_id`, les étapes terminées sont sautées et leurs résultats rechargés.

```
data/checkpoints/cli_20261006T153431/
├── run.json                      statut global de l'exécution
├── stages/extract_customers.json statut et métadonnées de chaque étape
├── raw_customers.parquet         résultat de l'extraction
├── validated_customers.parquet   résultat de la validation
├── scd2_to_insert.parquet        résultat du SCD2
└── ...
```

Les écritures sont **atomiques** : on écrit un fichier temporaire, puis on le renomme. Un plantage en pleine écriture ne laisse jamais un fichier à moitié écrit.

Deux façons de reprendre :

- **en ligne de commande** : `python pipeline.py --resume` ;
- **dans Airflow** : bouton *Clear* sur la tâche en échec. Airflow relance cette tâche et les suivantes, pas les précédentes.

Attention : reprendre une exécution arrêtée par la porte qualité échouerait de nouveau, car ses données extraites sont figées dans son checkpoint. Il faut corriger la source puis lancer une **nouvelle** exécution (le watermark n'ayant pas avancé, les mêmes enregistrements seront relus).

---

## 7. Relances et attente exponentielle

Une erreur peut être **transitoire** (réseau coupé, serveur surchargé : HTTP 500, 502, 503, 504, 429) ou **définitive** (mauvaise clé : HTTP 401, mauvaise URL : 404). Relancer n'a de sens que pour les premières.

Entre deux tentatives, on attend de plus en plus longtemps : 1 s, puis 2 s, puis 4 s... C'est l'**attente exponentielle** (exponential backoff). Elle laisse au service le temps de se rétablir au lieu de le bombarder. Un petit aléa (« jitter ») évite que plusieurs clients relancent tous au même instant. Pour l'erreur 429 (trop de requêtes), on respecte le délai indiqué par le serveur dans l'en-tête `Retry-After`.

Il y a deux niveaux de relance dans le projet :

1. **dans le code** (`APIExtractor`, `DatabaseExtractor`) : quelques secondes, pour les micro-coupures ;
2. **dans Airflow** (`retries=2`, `retry_exponential_backoff=True`) : quelques minutes, pour les pannes plus longues.

---

## 8. Airflow

Airflow **orchestre** : il lance les tâches dans le bon ordre, au bon moment, les relance en cas d'échec et garde l'historique de tout.

| Notion | Dans le projet |
|---|---|
| **DAG** (graphe orienté sans cycle) | `automated_data_pipeline`, défini dans `dags/airflow_dag.py` |
| **Tâche** | une étape du graphe, ex. `extract_orders` |
| **Opérateur** | le type de tâche : `PythonOperator` (fonction Python), `BashOperator` (commande shell, ex. `spark-submit`, `dbt run`) |
| **Sensor** | une tâche qui attend une condition : `FileSensor` attend le fichier produits |
| **Dépendances** | `a >> b` signifie « b après a » ; `[a, b] >> c` : c attend a et b |
| **run_id** | identifiant unique d'une exécution ; toutes les tâches d'une exécution partagent le même dossier de checkpoints |
| **XCom** | petites données échangées entre tâches (ici les métadonnées des extractions) ; jamais de gros volumes |
| **Planification** | `schedule="0 2 * * *"` (format cron : tous les jours à 2 h) |
| **catchup=False** | ne pas rattraper toutes les journées depuis la date de début (2024) |
| **max_active_runs=1** | jamais deux exécutions en même temps (watermarks et SCD2 l'exigent) |
| **Callbacks** | `on_failure_callback` et `on_success_callback` envoient les alertes |

Le fichier du DAG est relu régulièrement par Airflow : il doit rester **léger**. C'est pourquoi pandas et pyspark sont importés *dans* les fonctions des tâches, pas en haut du fichier.

### Airflow 2.8 (cours) et Airflow 3.3 (ce projet)

Le cours utilise Airflow 2.8.1, dont le support est terminé. Le projet tourne sous Airflow 3.3.2, et le DAG reste compatible avec les deux versions.

| | Airflow 2.8 (cours) | Airflow 3.3 (ce projet) |
|---|---|---|
| Interface web | `airflow webserver` | `airflow api-server` (ici inclus dans `airflow standalone`) |
| Compte admin | `airflow users create ...` | gestionnaire d'authentification simple (fichier de mots de passe) |
| Base de métadonnées | `airflow db init` | `airflow db migrate` |
| Planification | `schedule_interval=` | `schedule=` (existe déjà depuis la 2.4) |
| Date de l'exécution | `execution_date` | `logical_date`, absente pour un déclenchement manuel : on utilise donc `run_id` |
| Imports des opérateurs | `airflow.operators.python` | `airflow.providers.standard.operators.python` |

---

## 9. Spark

pandas charge toutes les données dans la mémoire d'**une** machine. Spark découpe les données en **partitions** et les traite **en parallèle**, sur plusieurs cœurs ou plusieurs machines (un cluster). Au-delà de quelques dizaines de millions de lignes, c'est indispensable.

Ce qu'il faut retenir :

- **Évaluation paresseuse (lazy)** : `groupBy` et `agg` ne calculent rien, ils construisent un plan. Le calcul ne démarre qu'à une **action** (ici l'écriture `.write.parquet`). Spark optimise alors le plan complet.
- **`local[*]`** : Spark tourne sur tous les cœurs de la machine. Sur un cluster, seule l'option `--master` change, le code reste le même.
- **`spark-submit`** : la façon standard de lancer un job Spark, utilisée par la tâche Airflow `compute_order_metrics_spark`.
- Spark a besoin de **Java** : il est installé dans l'image Docker.

Pour 150 commandes, Spark est plus lent que pandas (le démarrage de la machine Java prend une vingtaine de secondes). Il est utilisé ici pour apprendre et pour montrer que le pipeline passerait à l'échelle. Le test `tests/test_spark_parity.py` vérifie que Spark et pandas donnent exactement les mêmes agrégats.

---

## 10. dbt

dbt (data build tool) transforme les données **dans l'entrepôt**, avec des fichiers SQL `SELECT`. Il se charge de créer les tables, dans le bon ordre, et de les tester.

| Notion | Dans le projet |
|---|---|
| **Modèle** | un fichier `.sql` = une table ou une vue, ex. `models/marts/mart_product_performance.sql` |
| **`source()`** | une table que dbt lit sans l'avoir créée (`raw_orders`, `dim_customers`...) |
| **`ref()`** | une dépendance vers un autre modèle : dbt en déduit l'ordre de construction (le graphe) |
| **Matérialisation** | `view` (staging), `table` (marts), `incremental` (`fct_orders_enriched`) |
| **Tests génériques** | dans les `schema.yml` : `unique`, `not_null`, `relationships`, `accepted_values` |
| **Tests singuliers** | dans `tests/` : une requête qui doit renvoyer zéro ligne (ex. aucun chevauchement de versions) |
| **Snapshot** | le SCD2 « natif » de dbt |
| **Sévérité** | `error` bloque, `warn` signale seulement |

`dbt docs generate` produit une documentation avec le graphe de lignage des modèles.

---

## 11. Docker en 5 minutes

Airflow ne fonctionne pas nativement sous Windows. Docker permet de faire tourner des programmes Linux dans des **conteneurs** isolés, avec une configuration identique chez tout le monde.

| Notion | Dans le projet |
|---|---|
| **Image** | un modèle figé : `postgres:17-alpine`, `pipeline-airflow:3.3.2` (construite à partir de `docker/airflow/Dockerfile`) |
| **Conteneur** | une image en cours d'exécution : `postgres`, `mock-api`, `airflow`, `adminer` |
| **Volume** | un espace de stockage qui survit aux conteneurs : `pgdata` garde la base |
| **Montage** | le dossier du projet est visible dans le conteneur Airflow sous `/opt/project` : une modification du code sur ton PC est vue immédiatement |
| **docker compose** | décrit et lance plusieurs conteneurs ensemble (`docker-compose.yml`) |

Commandes essentielles : `docker compose up -d` (démarrer), `docker compose ps` (état), `docker compose logs airflow` (journaux), `docker compose exec airflow bash` (ouvrir un terminal dans le conteneur), `docker compose stop` (arrêter), `docker compose down -v` (tout supprimer, base comprise).
