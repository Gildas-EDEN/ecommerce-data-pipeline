# La solution officielle du cours comparée à ce projet

La « Solution Key » fournie par le cours est utile pour comprendre la structure attendue (cinq modules, un DAG, des relances, un rapport de qualité). Mais lue attentivement, et surtout **exécutée**, elle présente des problèmes qui l'empêchent de fonctionner telle quelle ou de passer les cas de test. Savoir les repérer est aussi formateur que d'écrire le code.

Chaque point indique le problème, pourquoi il compte, et ce que fait ce projet à la place.

---

## 1. Ce qui empêche la solution de fonctionner

### 1.1 `pandas` n'est pas importé dans le DAG

Dans `airflow_dag.py`, la fonction `extract_and_validate` utilise `pd.DataFrame(orders_data)`, mais le fichier n'importe jamais pandas. La tâche plante avec une `NameError` dès sa première exécution. L'erreur est invisible à la lecture rapide et n'apparaît qu'à l'exécution.

**Ici :** chaque fonction de tâche importe ce dont elle a besoin, et le test du DAG (Test Case 8) charge réellement le fichier dans Airflow.

### 1.2 Le catalogue produits du cours est illisible

Dans `Product catalog data.csv`, chaque ligne entière est entourée de guillemets. `pd.read_csv` y voit **une seule colonne** nommée `product_id,name,category,price,supplier`. Le `FileExtractor` de la solution lève alors « Missing required columns » : la solution échoue sur le fichier fourni par le cours lui-même.

**Ici :** `FileExtractor` détecte ce format, retire les guillemets ligne par ligne et le signale dans les logs (test `test_course_csv_malformed_format_is_repaired`). Le fichier du cours est utilisé tel quel comme entrée du pipeline.

### 1.3 Le DAG ne se charge pas dans la version actuelle d'Airflow

La solution utilise `schedule_interval` et `context['execution_date']`, supprimés dans Airflow 3. Le support d'Airflow 2 est terminé.

**Ici :** `schedule=` (disponible depuis Airflow 2.4), le `run_id` au lieu de la date d'exécution (qui n'existe pas pour un lancement manuel dans Airflow 3), et des imports compatibles avec les deux versions.

### 1.4 Avec pandas 3, la validation échoue sur toutes les tables contenant du texte

Le contrôle de types compare des noms de types sous forme de texte : `str(data['email'].dtype) in ['object']`. Depuis pandas 3, le texte est stocké dans le type `str`, plus `object`. Le contrôle échoue donc à chaque fois et le pipeline s'arrête sur des données parfaitement valides.

**Ici :** les types sont vérifiés avec `pd.api.types` (`is_integer_dtype`, `is_numeric_dtype`...), qui fonctionne avec pandas 2 comme avec pandas 3. Les tests passent avec pandas 3.0 sur le PC et dans Docker.

---

## 2. Ce qui fait échouer les cas de test

### 2.1 Test Case 5 : les valeurs manquantes ne bloquent jamais

```python
'status': 'PASS' if null_counts.sum() == 0 else 'WARN'
```

Une valeur manquante dans un champ obligatoire ne donne qu'un avertissement. Le cas de test attend un **échec** avec un rapport détaillé.

**Ici :** les colonnes obligatoires sont déclarées dans `config/pipeline.yaml`. Une valeur manquante y produit un `FAIL`, avec le nombre de valeurs manquantes par colonne et des exemples de lignes fautives ; le pipeline s'arrête avant toute transformation.

### 2.2 Le SCD2 n'est jamais appliqué à la vraie dimension

```python
existing_customers_df = pd.DataFrame()  # Placeholder for existing data
```

La dimension existante n'est jamais lue : à chaque exécution, tous les clients sont considérés comme nouveaux. Et les résultats sont écrits dans des fichiers CSV (« In production, this would write to data warehouse tables ») : rien n'arrive dans PostgreSQL. Les tests 3 et 4 peuvent passer sur la fonction isolée, mais le pipeline ne construit aucun historique.

**Ici :** `PostgresLoader.fetch_dimension` lit les versions existantes des clients du lot, et le chargement ferme et insère les versions dans `dim_customers`, en une transaction. L'historique est vérifié en conditions réelles (Bob et Carol à l'étape 5 du guide) et par deux tests dbt.

### 2.3 Test Case 7 : aucun point de reprise, et une double extraction

La solution n'a pas de checkpoint. Pire : la tâche `extract_and_validate` extrait et valide les données, puis la tâche `transform_and_load` appelle `run_data_pipeline()`, qui **extrait tout une seconde fois**. Les données chargées ne sont donc pas celles qui ont été validées : si la source change entre les deux tâches, des données jamais contrôlées arrivent dans l'entrepôt.

**Ici :** chaque étape sauvegarde son résultat ; la validation produit les fichiers `validated_*` et ce sont exactement ceux-là qui sont transformés puis chargés. Une relance repart de l'étape en échec (test `test_tc7_restart_from_transformation_without_reextracting`).

### 2.4 Test Case 6 : un incrémental calculé à partir de l'horloge

```python
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
```

- si le DAG ne tourne pas un jour (panne, maintenance), les changements de ce jour sont **perdus** ;
- s'il tourne deux fois, les mêmes données sont traitées deux fois ;
- le DAG calcule sa fenêtre avec la date d'exécution, `run_data_pipeline` avec l'horloge : deux fenêtres différentes dans la même exécution.

**Ici :** un watermark mémorise la dernière valeur réellement traitée, et n'avance qu'après un chargement réussi. Aucune donnée n'est perdue, et rejouer est sans effet grâce à l'idempotence.

---

## 3. Ce qui poserait problème en production

### 3.1 Injection SQL

```python
query += f" WHERE last_updated > '{last_updated}'"
```

Une valeur malveillante (ou simplement une apostrophe) dans le paramètre modifie la requête. **Ici :** requête paramétrée (`%s`), vérifiée par le test `test_tc1_incremental_query_is_parameterized`.

### 3.2 `catchup` placé au mauvais endroit

`'catchup': False` est dans `default_args`, qui ne s'applique qu'aux tâches : le réglage est ignoré. Dans Airflow 2, `catchup` vaut `True` par défaut ; avec `start_date = 2024-01-01`, activer le DAG lancerait **une exécution pour chaque jour depuis janvier 2024**. **Ici :** `catchup=False` est un paramètre du DAG, vérifié par un test.

### 3.3 Logs configurés au moment de l'import

`pipeline.py` appelle `logging.basicConfig(...)` avec un fichier `pipeline.log` dès son import, et le DAG l'importe en tête de fichier. Comme Airflow relit le DAG régulièrement, la configuration des logs d'Airflow est perturbée et un `pipeline.log` est créé dans un dossier imprévisible. **Ici :** les logs ne sont configurés que dans `main()` (ligne de commande) ; sous Airflow, c'est Airflow qui les gère.

### 3.4 Des relances inutiles, et d'autres absentes

- Toute erreur HTTP est relancée, y compris 401 (mauvaise clé) ou 404 (mauvaise URL) : on attend pour rien avant d'échouer.
- L'erreur 429 (trop de requêtes) et son délai `Retry-After` ne sont pas gérés.
- La pagination ignore `has_more` et `next_page` (pourtant présents dans `Sample_orders.json`) et s'arrête sur une page vide : une requête de trop à chaque extraction.

**Ici :** relance uniquement des erreurs transitoires (réseau, 5xx, 429), respect de `Retry-After`, pagination guidée par l'API, limitation de débit côté client.

### 3.5 Un SCD2 fragile

- Comparaison colonne par colonne avec `!=` : en pandas, `NaN != NaN` vaut `True`. Un client avec une valeur manquante est vu comme « modifié » à chaque exécution et accumule de fausses versions.
- Pas de nettoyage avant comparaison : un espace en trop ou une majuscule crée une nouvelle version.
- Toute nouvelle version commence à la date du traitement : au premier chargement, tous les clients « commencent » aujourd'hui, et aucune commande passée ne peut être rattachée à une version (la jointure point-in-time devient impossible). Si le pipeline est en panne trois jours, les dates de changement sont fausses.
- Aucune gestion des clients supprimés puis réapparus.

**Ici :** empreinte des colonnes après nettoyage, date de changement issue de la source (`last_updated`), première version à la date d'inscription, cas « réactivé » et « supprimé » gérés, 10 tests dédiés.

### 3.6 Performance

La solution parcourt les clients un par un (`iterrows`) et refiltre la dimension à chaque ligne. Mesuré sur ce PC : **6,9 s pour 8 000 clients, contre 0,5 s ici** (13 fois plus lent). Cela reste sous la limite de 2 h pour 100 000 lignes, mais l'écart grandit avec la taille de la dimension. **Ici :** comparaison en une seule jointure (`merge`) ; 100 000 clients en environ 6 s.

### 3.7 Des chemins et tâches incohérents

- Le `FileSensor` attend `/data/input/products.csv`, un chemin écrit en dur, différent de celui de la configuration (`./data/input`).
- La tâche de nettoyage supprime `/tmp/pipeline_*`, des fichiers que rien ne crée.
- Les alertes partent vers `data-team@company.com` sans serveur SMTP configuré : des erreurs d'envoi dans les logs à chaque échec.

**Ici :** chemins issus de la configuration, nettoyage des vrais checkpoints de plus de 7 jours, alertes dans un fichier JSON (et un webhook si configuré), e-mail seulement si une adresse est fournie.

### 3.8 Ni Spark ni dbt

Malgré le titre du cours, la solution n'utilise ni Spark ni dbt. **Ici :** Spark calcule les agrégats (avec un test de parité contre pandas), dbt construit les tables d'analyse, un modèle incrémental, un snapshot SCD2 et 24 tests.

---

## 4. Ce qu'il faut retenir

1. **Exécuter, pas seulement lire.** Les problèmes 1.1, 1.2 et 3.2 n'apparaissent qu'à l'exécution.
2. **Charger exactement ce qui a été validé**, jamais une seconde extraction.
3. **Ne pas déduire l'incrémental de l'horloge** : mémoriser ce qui a réellement été traité.
4. **Les bibliothèques évoluent** (pandas 3, Airflow 3) : préférer les API prévues pour ça (`pd.api.types`) aux comparaisons de chaînes de caractères.
5. **Tester les cas limites** : valeurs manquantes, doublons, formats mal formés, relances. C'est là que se cachent les bugs.
