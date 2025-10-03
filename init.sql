CREATE TABLE IF NOT EXISTS uploads (
  token TEXT PRIMARY KEY,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);
