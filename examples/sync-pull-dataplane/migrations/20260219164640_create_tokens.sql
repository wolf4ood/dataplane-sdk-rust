CREATE TABLE IF NOT EXISTS tokens (
    flow_id          TEXT PRIMARY KEY,
    endpoint         TEXT NOT NULL,
    token_id         TEXT NOT NULL,
    dataset_id       TEXT NOT NULL,
    UNIQUE (token_id, dataset_id)
)
