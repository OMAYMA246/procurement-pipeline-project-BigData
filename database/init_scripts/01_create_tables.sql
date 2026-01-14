-- ============================================
-- Script d'initialisation de la base de données
-- Procurement System - Master Data
-- ============================================

-- Table: Suppliers (Fournisseurs)
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_code VARCHAR(50) UNIQUE NOT NULL,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    lead_time_days INTEGER DEFAULT 2,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: Warehouses (Entrepôts)
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(255) NOT NULL,
    warehouse_code VARCHAR(50) UNIQUE NOT NULL,
    city VARCHAR(100),
    capacity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: Products (Produits/SKU)
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(100) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    unit_price DECIMAL(10, 2),
    pack_size INTEGER DEFAULT 1,
    case_size INTEGER DEFAULT 6,
    min_order_quantity INTEGER DEFAULT 1,
    safety_stock INTEGER DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: Stock Levels (Niveaux de stock par entrepôt et produit)
CREATE TABLE IF NOT EXISTS stock_levels (
    stock_id SERIAL PRIMARY KEY,
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    product_id INTEGER REFERENCES products(product_id),
    available_quantity INTEGER DEFAULT 0,
    reserved_quantity INTEGER DEFAULT 0,
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(warehouse_id, product_id, snapshot_date)
);

-- Index pour améliorer les performances des requêtes
CREATE INDEX idx_products_supplier ON products(supplier_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_stock_warehouse ON stock_levels(warehouse_id);
CREATE INDEX idx_stock_product ON stock_levels(product_id);
CREATE INDEX idx_stock_date ON stock_levels(snapshot_date);

-- Commentaires pour documentation
COMMENT ON TABLE suppliers IS 'Fournisseurs et leurs contraintes de livraison';
COMMENT ON TABLE warehouses IS 'Entrepôts de stockage';
COMMENT ON TABLE products IS 'Catalogue produits avec règles d''approvisionnement';
COMMENT ON TABLE stock_levels IS 'Snapshots quotidiens des niveaux de stock';