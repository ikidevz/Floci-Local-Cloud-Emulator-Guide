CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    status      TEXT NOT NULL DEFAULT 'NEW',
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
