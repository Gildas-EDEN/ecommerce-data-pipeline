-- =============================================================================
-- 01_init_database.sql
-- Script fourni par le cours (« Database Setup Script »), complété.
-- Exécuté automatiquement au premier démarrage du conteneur PostgreSQL
-- (tout fichier .sql de /docker-entrypoint-initdb.d est lancé une seule fois).
-- Les ajouts par rapport au script d'origine sont signalés par [AJOUT].
-- =============================================================================

-- Create database and user (run as postgres superuser)
CREATE DATABASE analytics;
CREATE USER pipeline_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE analytics TO pipeline_user;

-- Connect to analytics database and create tables
\c analytics;

-- -----------------------------------------------------------------------------
-- SYSTÈME SOURCE : base clients de l'application e-commerce (simulée)
-- -----------------------------------------------------------------------------
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- [AJOUT] index sur la colonne utilisée par l'extraction incrémentale
CREATE INDEX ix_customers_last_updated ON customers (last_updated);

-- -----------------------------------------------------------------------------
-- ENTREPÔT : dimension client historisée (SCD2)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_customers (
    dim_customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,          -- [AJOUT] date à laquelle la version a été fermée
    run_id VARCHAR(255),           -- [AJOUT] lignage : exécution qui a créé la version
    -- [AJOUT] une version ne peut pas finir avant d'avoir commencé
    CONSTRAINT ck_dim_customers_dates CHECK (effective_start_date <= effective_end_date)
);
-- [AJOUT] garde-fou SCD2 au niveau de la base : au plus UNE version courante par client
CREATE UNIQUE INDEX ux_dim_customers_one_current ON dim_customers (customer_id) WHERE is_current;
CREATE INDEX ix_dim_customers_customer ON dim_customers (customer_id, effective_start_date);

-- -----------------------------------------------------------------------------
-- ENTREPÔT : faits journaliers des commandes (grain : jour x client)
-- -----------------------------------------------------------------------------
CREATE TABLE fact_daily_orders (
    order_date DATE,
    customer_id INTEGER,
    total_sales DECIMAL(10,2),
    order_count INTEGER,
    avg_order_value DECIMAL(10,2),
    unique_orders INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,          -- [AJOUT] dernière mise à jour (upsert)
    run_id VARCHAR(255),           -- [AJOUT] lignage
    PRIMARY KEY (order_date, customer_id)
);

-- -----------------------------------------------------------------------------
-- [AJOUT] Tables brutes chargées par le pipeline (copie fidèle des sources)
-- -----------------------------------------------------------------------------
CREATE TABLE raw_products (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    supplier VARCHAR(200) NOT NULL,
    run_id VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    run_id VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ix_raw_orders_loaded_at ON raw_orders (loaded_at);

CREATE TABLE raw_order_items (
    order_id VARCHAR(50) NOT NULL,
    line_number INTEGER NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    run_id VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, line_number)
);

-- [AJOUT] Faits « performance produit » (grain : jour x produit)
CREATE TABLE fact_product_performance (
    order_date DATE NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    units_sold INTEGER NOT NULL,
    revenue NUMERIC(12,2) NOT NULL,
    order_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    run_id VARCHAR(255),
    PRIMARY KEY (order_date, product_id)
);

-- Insert sample customer data
INSERT INTO customers VALUES
(1001, 'Alice Johnson', 'alice.johnson@email.com', '2023-01-15', 'active', '2024-02-01 10:00:00'),
(1002, 'Bob Smith', 'bob.smith@email.com', '2023-02-20', 'active', '2024-02-01 10:00:00'),
(1003, 'Carol Davis', 'carol.davis@email.com', '2023-03-10', 'inactive', '2024-01-28 15:30:00'),
(1004, 'David Wilson', 'david.wilson@email.com', '2023-04-05', 'active', '2024-02-01 10:00:00'),
(1005, 'Eve Brown', 'eve.brown@email.com', '2023-05-12', 'active', '2024-01-30 09:15:00');

-- Grant permissions to pipeline user
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
-- [AJOUT] le chargement remplace les lignes des commandes rechargées
GRANT DELETE ON raw_order_items TO pipeline_user;
