-- =============================================================================
-- 00_create_airflow_db.sql
-- Base de MÉTADONNÉES d'Airflow (historique des exécutions, état des tâches...).
-- Elle est séparée de la base "analytics" qui contient nos données métier.
-- Exécuté automatiquement au tout premier démarrage du conteneur PostgreSQL.
-- =============================================================================
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
