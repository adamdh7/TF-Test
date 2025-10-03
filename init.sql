-- 0001_add_file_data.sql
-- 1) verifye kolòn yo (opsyonèl)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'uploads';

-- 2) ajoute kolòn file_data si li pa egziste
ALTER TABLE uploads
ADD COLUMN IF NOT EXISTS file_data BYTEA;

-- 3) asire index sou created_at (si li pa deja egziste)
CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);

-- 4) verifye ankò
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'uploads';
