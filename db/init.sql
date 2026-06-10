CREATE TABLE IF NOT EXISTS stores (
    store_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_log (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    source TEXT NOT NULL,
    store_id TEXT NOT NULL REFERENCES stores(store_id),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_log_correlation_id
    ON event_log (correlation_id);

CREATE INDEX IF NOT EXISTS idx_event_log_event_type
    ON event_log (event_type);

CREATE INDEX IF NOT EXISTS idx_event_log_store_event
    ON event_log (store_id, event_type);

INSERT INTO stores (store_id, name)
VALUES
    ('store-ueh', 'UEH Campus Store'),
    ('store-d1', 'District 1 Store')
ON CONFLICT (store_id) DO UPDATE
    SET name = EXCLUDED.name,
        active = true;
