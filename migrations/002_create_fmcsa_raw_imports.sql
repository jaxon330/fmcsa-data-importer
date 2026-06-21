CREATE TABLE IF NOT EXISTS fmcsa_raw_imports (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  source TEXT NOT NULL,
  dataset_key TEXT NOT NULL,
  dataset_type TEXT,
  resource_id TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  etag TEXT,
  last_modified TEXT,
  content_length BIGINT,
  sha256 TEXT,
  rows_read BIGINT,
  rows_inserted_or_updated BIGINT,
  rows_failed BIGINT,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_raw_imports_source_identity
  ON fmcsa_raw_imports (
    provider,
    source,
    dataset_key,
    COALESCE(etag, ''),
    COALESCE(last_modified, ''),
    COALESCE(content_length, -1),
    COALESCE(sha256, '')
  );

CREATE INDEX IF NOT EXISTS idx_fmcsa_raw_imports_dataset_imported_at
  ON fmcsa_raw_imports (dataset_key, imported_at DESC);
