# Guide pas à pas

Ce guide t'emmène de l'installation jusqu'aux scénarios de panne. Compte environ 1 h 30 pour tout faire en prenant le temps de regarder. Toutes les commandes se tapent dans PowerShell, depuis le dossier du projet :

```powershell
cd C:\Users\gilda\OneDrive\Desktop\Projet_Pipeline_Spark_dbt_Airflow
```

## Sommaire

0. [Prérequis](#étape-0--prérequis)
1. [Lancer les tests sur ton PC](#étape-1--lancer-les-tests-sur-ton-pc)
2. [Démarrer l'environnement Docker](#étape-2--démarrer-lenvironnement-docker)
3. [Première exécution dans Airflow](#étape-3--première-exécution-dans-airflow)
4. [Explorer les résultats](#étape-4--explorer-les-résultats)
5. [Simuler une journée d'activité : incrémental et SCD2](#étape-5--simuler-une-journée-dactivité--incrémental-et-scd2)
6. [Fermer la porte qualité](#étape-6--fermer-la-porte-qualité)
7. [Rendre l'API instable](#étape-7--rendre-lapi-instable)
8. [Planter puis reprendre en ligne de commande](#étape-8--planter-puis-reprendre-en-ligne-de-commande)
9. [Lancer dbt et Spark à la main](#étape-9--lancer-dbt-et-spark-à-la-main)
10. [Arrêter et remettre à zéro](#étape-10--arrêter-et-remettre-à-zéro)
11. [Dépannage](#dépannage)

---

## Étape 0 : prérequis

- **Docker Desktop** installé et démarré (icône de baleine dans la barre des tâches).
- **8 Go de RAM** : l'environnement complet en utilise environ 1,5 Go au repos, un peu plus pendant Spark.
- Ports libres : 5432 (PostgreSQL), 8000 (API), 8080 (Airflow), 8081 (Adminer).
- Le fichier `.env` existe (sinon : `Copy-Item .env.example .env`).

---

## Étape 1 : lancer les tests sur ton PC

L'environnement Python du projet est déjà créé, **hors de OneDrive** pour éviter de synchroniser des milliers de fichiers :

```powershell
C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\python.exe -m pytest -v
```

Résultat attendu : `43 passed, 2 skipped`. Les deux tests ignorés demandent Airflow et Spark ; ils tournent dans Docker à l'étape 9.

> Pour recréer l'environnement sur une autre machine :
> `python -m venv C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow` puis
> `C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\python.exe -m pip install -r requirements.txt`

**À regarder :** ouvre `tests/test_scd2.py`. Chaque test est un petit scénario lisible (un client change d'e-mail, que doit-il se passer ?). C'est la meilleure porte d'entrée pour comprendre le SCD2.

Pour ne pas retaper le chemin complet de Python, tu peux activer l'environnement :

```powershell
C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\Activate.ps1
```

Les commandes `python ...` de la suite utilisent alors cet environnement.

---

## Étape 2 : démarrer l'environnement Docker

```powershell
docker compose up -d --build
```

La première fois, Docker télécharge et construit les images (environ 2 Go, 5 à 10 minutes). Sur ce PC elles sont déjà construites : le démarrage prend moins d'une minute.

Vérifie l'état des conteneurs :

```powershell
docker compose ps
```

Attends que `airflow` affiche `(healthy)` (environ 1 minute). Au premier démarrage, PostgreSQL exécute automatiquement les scripts de `sql/init/` : création des bases, des tables et des 5 clients du cours.

---

## Étape 3 : première exécution dans Airflow

1. Ouvre **http://localhost:8080** et connecte-toi avec `admin` / `admin`.
2. Clique sur le DAG **`automated_data_pipeline`**. Il est en pause (les nouveaux DAG le sont par défaut).
3. Ouvre l'onglet **Graph** : tu retrouves les 14 tâches et leurs dépendances. Clique sur une tâche pour lire sa documentation.
4. **Active le DAG** avec l'interrupteur. Comme `catchup=False`, Airflow ne lance que l'exécution prévue la plus récente (aujourd'hui 2 h), immédiatement.
5. Regarde les tâches passer au vert (environ 2 min 30). Remarque :
   - `extract_customers`, `extract_orders` et `dbt_snapshot` tournent **en même temps** ;
   - `compute_order_metrics_spark` est la tâche la plus longue : la machine Java de Spark démarre ;
   - `validate_data` attend que les trois extractions soient finies.
6. Clique sur `extract_products`, puis sur **Logs** : tu vois l'avertissement « Format CSV mal formé détecté... réparation automatique ». Le fichier du cours a chaque ligne entre guillemets : le pipeline le répare au lieu de planter.
7. Clique sur `generate_quality_report`, puis sur **XCom** (ou les logs) : la tâche relit ce qu'ont renvoyé les extractions.

---

## Étape 4 : explorer les résultats

### Le rapport de qualité

Ouvre le dossier `data/output/reports/` : un sous-dossier par exécution, avec `quality_report.md` (lisible) et `execution_report.json` (complet). Tu y trouves la durée de chaque étape, les volumes, chaque contrôle qualité, le lignage et les watermarks. L'avertissement `order_total_consistency` est normal : une commande des données de démo inclut volontairement des frais de port.

### La base de données avec Adminer

Ouvre **http://localhost:8081** :

| Champ | Valeur |
|---|---|
| Système | PostgreSQL |
| Serveur | `postgres` |
| Utilisateur | `pipeline_user` |
| Mot de passe | `secure_password` |
| Base de données | `analytics` |

Clique sur **Requête SQL** et essaie :

```sql
-- La dimension client : une version par client pour l'instant
SELECT customer_id, name, email, status, effective_start_date, effective_end_date, is_current
FROM dim_customers ORDER BY customer_id, effective_start_date;

-- Les produits les plus vendus (table construite par dbt)
SELECT product_name, units_sold, revenue, revenue_share_pct, revenue_rank, trend_pct
FROM dbt_marts.mart_product_performance ORDER BY revenue_rank;

-- Les ventes par segment client (dbt)
SELECT * FROM dbt_marts.mart_daily_sales_by_segment ORDER BY order_date DESC LIMIT 10;

-- Le lignage : quelle exécution a écrit quelles lignes
SELECT run_id, count(*) FROM raw_orders GROUP BY run_id;
```

Dans le menu de gauche, change de **schéma** (`dbt_marts`, `dbt_staging`, `dbt_snapshots`) pour voir ce que dbt a créé.

---

## Étape 5 : simuler une journée d'activité : incrémental et SCD2

Le script suivant modifie les systèmes sources : Bob change d'e-mail, Carol redevient active, trois nouveaux clients s'inscrivent et quatre commandes sont passées aujourd'hui.

```powershell
docker compose exec airflow python /opt/project/scripts/simulate_changes.py
```

Dans Airflow, clique sur **Trigger** (bouton lecture) pour lancer une exécution manuelle. Une fois terminée :

1. Ouvre le nouveau rapport dans `data/output/reports/` (dossier `manual__...`). Section « Extraction » :
   - clients : **incrémental, 5 lignes** (sur 8 dans la source),
   - commandes : **incrémental, 7 lignes** (les 3 du dernier jour déjà traité, relues en entier, plus les 4 nouvelles),
   - produits : **inchangé (ignoré)**.
2. Relance la première requête de l'étape 4 dans Adminer : Bob et Carol ont maintenant **deux versions**, l'ancienne fermée au jour du changement.
3. Observe la jointure « point-in-time » :

```sql
SELECT order_id, order_date, customer_segment_at_order
FROM dbt_marts.fct_orders_enriched
WHERE customer_id = 1003
ORDER BY order_date DESC LIMIT 5;
```

Les anciennes commandes de Carol restent `inactive`, celle d'aujourd'hui est `active`.

4. Compare avec le SCD2 de dbt :

```sql
SELECT customer_id, email, status, dbt_valid_from, dbt_valid_to
FROM dbt_snapshots.snap_customers ORDER BY customer_id, dbt_valid_from;
```

**Question pour toi :** pourquoi la première version de Bob commence-t-elle le 2023-02-20 dans `dim_customers` et le 2024-02-01 dans le snapshot dbt ? (Réponse dans [01_concepts_cles.md](01_concepts_cles.md), section SCD2.)

---

## Étape 6 : fermer la porte qualité

Introduis une adresse e-mail invalide dans la source :

```powershell
docker compose exec postgres psql -U pipeline_user -d analytics -c "UPDATE customers SET email = 'pas-un-email', last_updated = NOW() WHERE customer_id = 1004;"
```

Lance le DAG (**Trigger**). Ce que tu dois observer :

- `validate_data` passe en **rouge** et n'est pas relancée (`retries=0` : des données fausses ne se corrigent pas toutes seules) ;
- toutes les tâches suivantes sont en `upstream_failed` : **rien n'a été chargé** ;
- dans les logs de `validate_data`, le chemin du rapport détaillé (`validation_results.json`) : la règle `email_format_email` est en `FAIL`, avec l'identifiant du client fautif ;
- un fichier d'alerte `*_error.json` apparaît dans `data/output/alerts/`.

Corrige la source, puis relance une **nouvelle** exécution :

```powershell
docker compose exec postgres psql -U pipeline_user -d analytics -c "UPDATE customers SET email = 'david.wilson@email.com', last_updated = NOW() WHERE customer_id = 1004;"
```

Après **Trigger**, tout repasse au vert. Comme le watermark n'avait pas avancé, David est relu ; son empreinte est identique à sa version courante, donc **aucune fausse version** n'est créée (vérifie dans `dim_customers`).

> Pourquoi une nouvelle exécution et pas « Clear » sur `validate_data` ? Les données extraites de l'exécution en échec sont figées dans son checkpoint : les revalider donnerait le même résultat. Il faut les extraire à nouveau.

---

## Étape 7 : rendre l'API instable

Dans le fichier `.env`, ajoute la ligne :

```
MOCK_API_FAIL_FIRST=2
```

Recrée le conteneur de l'API pour qu'il lise ce réglage :

```powershell
docker compose up -d mock-api
```

Lance le DAG. Dans les logs de `extract_orders`, tu vois deux erreurs HTTP 500, des attentes d'environ 1 s puis 2 s, puis le succès : c'est l'**attente exponentielle** dans le code.

**Pour aller plus loin :** mets `MOCK_API_FAIL_FIRST=5`, recrée le conteneur et relance le DAG. Les 3 tentatives du code échouent : la tâche `extract_orders` échoue, Airflow attend environ une minute puis la **relance** (2e niveau de relance), et elle réussit. Dans l'interface, la tâche affiche alors 2 essais.

Remets `MOCK_API_FAIL_FIRST=0` (ou supprime la ligne) puis `docker compose up -d mock-api`.

---

## Étape 8 : planter puis reprendre en ligne de commande

Le pipeline fonctionne aussi sans Airflow, directement depuis ton PC (il utilise la base et l'API de Docker grâce au fichier `.env`). Coupe l'API :

```powershell
docker compose stop mock-api
python pipeline.py
```

Les clients et les produits sont extraits et sauvegardés, puis l'extraction des commandes échoue après 3 tentatives. Le message final te propose `--resume`. Redémarre l'API et reprends :

```powershell
docker compose start mock-api
python pipeline.py --resume
```

Les logs montrent `extract_customers : déjà terminée (checkpoint), on la saute` : seule l'extraction des commandes est refaite, puis le pipeline va au bout. C'est le **Test Case 7** en conditions réelles.

Autres options utiles :

| Commande | Effet |
|---|---|
| `python pipeline.py --run-id ID --from-stage transform_orders` | rejoue une exécution à partir d'une étape |
| `python pipeline.py --engine spark` | calcule les agrégats avec Spark (nécessite PySpark et Java sur le PC) |
| `python pipeline.py --full-refresh` | ignore les watermarks : tout est relu (et les suppressions sont détectées) |

---

## Étape 9 : lancer dbt et Spark à la main

Tous les tests, y compris ceux du DAG (Test Case 8) et la comparaison Spark / pandas, dans le conteneur :

```powershell
docker compose exec -w /opt/project airflow python -m pytest -v
```

Résultat attendu : `48 passed, 1 warning`. L'avertissement vient de PySpark, qui ne garantit pas encore officiellement pandas 3 ; le test de parité confirme justement que Spark et pandas donnent les mêmes résultats.

dbt, à la main (construit et teste tout en une commande) :

```powershell
docker compose exec -w /opt/project/dbt_project airflow /opt/airflow/dbt-venv/bin/dbt build --profiles-dir .
```

Pour forcer la reconstruction complète du modèle incrémental, ajoute `--full-refresh`.

Mesure de performance sur 100 000 enregistrements (sur ton PC) :

```powershell
python scripts/benchmark.py
```

---

## Étape 10 : arrêter et remettre à zéro

| Besoin | Commande |
|---|---|
| Arrêter en gardant les données | `docker compose stop` (puis `docker compose start`) |
| Supprimer les conteneurs, garder la base | `docker compose down` |
| Repartir de zéro (base, checkpoints, rapports, watermarks, commandes ajoutées) | `python scripts/reset_demo.py --docker` |

Après une remise à zéro, `docker compose up -d` recrée la base avec les 5 clients du cours, et la prochaine exécution du DAG refait un chargement complet. Tu peux ensuite fermer Docker Desktop pour libérer la mémoire.

---

## Dépannage

**« failed to connect to the docker API »** : Docker Desktop n'est pas démarré. Lance-le et attends que la baleine soit stable.

**« Activate.ps1 ne peut pas être chargé car l'exécution de scripts est désactivée »** : Windows bloque les scripts PowerShell par défaut. Inutile de changer ce réglage : remplace simplement `python` par le chemin complet, par exemple `C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\python.exe pipeline.py`.

**« port is already allocated »** : un autre programme utilise le port (souvent un PostgreSQL installé sur le PC pour 5432). Change le port de gauche dans `docker-compose.yml`, par exemple `"5433:5432"`, et mets `DB_PORT=5433` dans `.env`.

**Le DAG n'apparaît pas dans Airflow** : attends 30 secondes (délai de détection des nouveaux fichiers), puis vérifie les erreurs d'import :

```powershell
docker compose exec airflow airflow dags list-import-errors
```

**La connexion admin / admin est refusée** : recrée le conteneur avec `docker compose up -d --force-recreate airflow`.

**Une tâche reste longtemps « queued »** : Airflow est peut-être en train de démarrer. Regarde les journaux avec `docker compose logs airflow --tail 50`.

**Le PC ralentit** : `docker stats` montre la mémoire utilisée par chaque conteneur. `docker compose stop` libère tout.

**Où sont les logs ?** Dans l'interface Airflow (bouton **Logs** de chaque tâche) ; hors Airflow, dans la console et dans `logs/pipeline.log`.
