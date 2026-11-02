-- RetailPulse : schéma PostgreSQL (exécuté à la première création du volume).
-- Tables conformes à la section 9 du cahier des charges + invalid_events (traçabilité).

-- Catalogue : alimenté par le Reference Loader (identifiant DummyJSON conservé).
CREATE TABLE IF NOT EXISTS products (
    product_id  INTEGER       PRIMARY KEY,
    title       TEXT          NOT NULL,
    category    TEXT          NOT NULL,
    price       NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    stock       INTEGER       CHECK (stock >= 0),
    brand       TEXT,                       -- absent pour ~la moitié des produits DummyJSON
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Clients : alimentés par le Reference Loader. Les emails DummyJSON sont uniques.
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER     PRIMARY KEY,
    first_name  TEXT        NOT NULL,
    last_name   TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE,
    city        TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Commandes simulées : alimentées par Spark (ORDER_CREATED puis événements de paiement).
CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT          PRIMARY KEY,
    customer_id  INTEGER       NOT NULL REFERENCES customers (customer_id),
    created_at   TIMESTAMPTZ   NOT NULL,
    status       TEXT          NOT NULL DEFAULT 'CREATED'
                               CHECK (status IN ('CREATED', 'PAID', 'FAILED', 'CANCELLED')),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Agrégats par catégorie et fenêtre de 5 minutes : alimentés par Spark (upsert).
-- La clé primaire porte l'unicité (category, window_start, window_end) : un rejeu
-- Spark écrase la ligne au lieu de la dupliquer.
CREATE TABLE IF NOT EXISTS sales_aggregates (
    category       TEXT          NOT NULL,
    window_start   TIMESTAMPTZ   NOT NULL,
    window_end     TIMESTAMPTZ   NOT NULL,
    order_count    INTEGER       NOT NULL CHECK (order_count >= 0),
    total_quantity INTEGER       NOT NULL CHECK (total_quantity >= 0),
    total_revenue  NUMERIC(14,2) NOT NULL CHECK (total_revenue >= 0),
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (category, window_start, window_end),
    CHECK (window_end > window_start)
);

-- Messages rejetés par Spark (JSON invalide, champ manquant, produit inconnu...).
-- Identifiés par leurs coordonnées Kafka : un rejeu ne crée pas de doublon.
CREATE TABLE IF NOT EXISTS invalid_events (
    id              BIGSERIAL   PRIMARY KEY,
    topic           TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    reason          TEXT        NOT NULL,
    event_id        TEXT,
    raw_value       TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, kafka_partition, kafka_offset, reason)
);

-- Index : chacun répond à une requête identifiée.
CREATE INDEX IF NOT EXISTS idx_products_category   ON products (category);            -- enrichissement et filtres par catégorie
CREATE INDEX IF NOT EXISTS idx_orders_customer_id  ON orders (customer_id);          -- jointure vers customers (clé étrangère)
CREATE INDEX IF NOT EXISTS idx_orders_created_at   ON orders (created_at DESC);      -- dernières commandes du tableau de bord
CREATE INDEX IF NOT EXISTS idx_sales_window_start  ON sales_aggregates (window_start); -- évolution du CA dans le temps
