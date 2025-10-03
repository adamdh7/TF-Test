// server.js - robust DB-migrations + store files in Postgres (BYTEA)
// Replace your existing server.js with this file and redeploy to Render.

require('dotenv').config();

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const MIGRATE_KEY = process.env.MIGRATE_KEY || null;

// Postgres pool
let pool = null;
if (process.env.DATABASE_URL && process.env.DATABASE_URL.trim()) {
  try {
    const { Pool } = require('pg');
    const cfg = { connectionString: process.env.DATABASE_URL };
    if (process.env.DATABASE_SSL === 'true' || process.env.DATABASE_SSL === '1') cfg.ssl = { rejectUnauthorized: false };
    pool = new Pool(cfg);
    pool.on('error', (err) => console.error('Unexpected PG client error', err));
    console.log('Postgres pool created (pg loaded).');
  } catch (err) {
    console.error('pg not installed or failed to init. Install "pg" and set DATABASE_URL to enable DB.', err && err.message);
    pool = null;
  }
} else {
  console.error('No DATABASE_URL configured — this server requires Postgres to store files in DB.');
  pool = null;
}

// in-memory metadata cache
let mappings = {};

// disk metadata fallback (debug)
function loadMappingsFromDisk() {
  try {
    if (fs.existsSync(UPLOAD_JSON)) {
      const txt = fs.readFileSync(UPLOAD_JSON, 'utf8');
      mappings = JSON.parse(txt || '{}');
      console.log('Loaded mappings from', UPLOAD_JSON, Object.keys(mappings).length);
    } else {
      mappings = {};
      console.log('No upload.json found — starting empty mapping.');
    }
  } catch (err) {
    console.warn('Failed loading upload.json:', err && err.message);
    mappings = {};
  }
}
function saveMappingsToDisk() {
  try {
    const tmp = UPLOAD_JSON + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(mappings, null, 2));
    fs.renameSync(tmp, UPLOAD_JSON);
  } catch (err) {
    console.error('Failed saving upload.json', err && err.message);
  }
}

// run migrations: create table + column if not exists
async function runMigrations() {
  if (!pool) {
    console.warn('No DB pool - skipping migrations.');
    return;
  }
  try {
    const createSql = `
      CREATE TABLE IF NOT EXISTS uploads (
        token TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        file_data BYTEA,
        created_at TIMESTAMPTZ DEFAULT now()
      );
    `;
    await pool.query(createSql);
    await pool.query(`ALTER TABLE uploads ADD COLUMN IF NOT EXISTS file_data BYTEA;`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);`);
    console.log('DB migration applied (uploads table + file_data ready).');
  } catch (err) {
    console.error('DB migration failed:', err && err.message);
    throw err;
  }
}

// helpers to interact with DB
async function saveFileToDB(token, entry, buffer) {
  if (!pool) throw new Error('No database pool available');
  const q = `
    INSERT INTO uploads (token, data, file_data, created_at)
    VALUES ($1, $2::jsonb, $3, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, file_data = $3, created_at = NOW();
  `;
  try {
    await pool.query(q, [token, JSON.stringify(entry), buffer]);
  } catch (err) {
    // If column missing, try to run migrations then retry once
    const msg = (err && err.message) || '';
    if (msg.includes('column "file_data"') || msg.includes('does not exist')) {
      console.warn('Save to DB failed because file_data missing. Running migrations and retrying...');
      try {
        await runMigrations();
        await pool.query(q, [token, JSON.stringify(entry), buffer]);
        console.log('Retry saveFileToDB after migration succeeded for token', token);
        return;
      } catch (err2) {
        console.error('Retry after migration failed:', err2 && err2.message);
        throw err2;
      }
    }
    throw err;
  }
}

async function fetchFileFromDB(token) {
  if (!pool) throw new Error('No database pool available');
  const r = await pool.query('SELECT data, file_data FROM uploads WHERE token = $1', [token]);
  if (!r.rowCount) return null;
  return { data: r.rows[0].data, file: r.rows[0].file_data };
}

async function saveMappingMetadataToDB(token, entry) {
  if (!pool) return;
  const q = `
    INSERT INTO uploads (token, data, created_at)
    VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, created_at = NOW();
  `;
  await pool.query(q, [token, JSON.stringify(entry)]);
}

// token + filename helpers
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i = 0; i < len; i++) t += chars[Math.floor(Math.random() * chars.length)];
  return t;
}
function genUniqueToken() {
  let t = genToken(8), tries = 0;
  while (mappings[t] && tries++ < 100) t = genToken(8);
  if (mappings[t]) t = genToken(12);
  return t;
}
function safeFileName(name) {
  const ext = path.extname(name);
  const base = path.basename(name, ext);
  const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
  const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
  return (safeBase + safeExt) || 'file';
}
function truncateMiddle(name, maxLen = 30) {
  if (!name) return name;
  if (name.length <= maxLen) return name;
  const keep = Math.floor((maxLen - 1) / 2);
  const start = name.slice(0, keep);
  const end = name.slice(name.length - keep);
  return `${start}…${end}`;
}

// multer memory storage
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: Number(process.env.MAX_FILE_SIZE || 5368709120) } });

const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// startup: load disk mappings then DB + migrations, then start server
loadMappingsFromDisk();
(async () => {
  try {
    if (!pool) {
      console.error('No DATABASE_URL configured. Server will still run but uploads to DB will fail.');
    } else {
      // run migrations and load metadata
      await runMigrations();
      const res = await pool.query('SELECT token, data FROM uploads');
      const dbm = {};
      (res.rows || []).forEach(r => dbm[r.token] = r.data);
      mappings = Object.assign({}, mappings, dbm);
      // persist metadata-only entries to DB if any local-only exists
      for (const [token, entry] of Object.entries(mappings)) {
        if (!dbm[token]) {
          await saveMappingMetadataToDB(token, entry).catch(()=>{});
        }
      }
      console.log('Loaded mappings from DB:', Object.keys(mappings).length);
    }
  } catch (err) {
    console.error('Startup error (migrations/load):', err && err.message);
  }
})();

// pagination (metadata)
app.get('/uploads', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 200);
  const offset = Math.max(Number(req.query.offset) || 0, 0);
  if (pool) {
    try {
      const q = `SELECT token, data->>'originalName' as name, data->>'safeOriginal' as safeOriginal, created_at
                 FROM uploads ORDER BY created_at DESC LIMIT $1 OFFSET $2`;
      const r = await pool.query(q, [limit, offset]);
      const items = r.rows.map(row => ({
        token: row.token,
        displayName: truncateMiddle(row.name || row.safeOriginal, 30),
        safeOriginal: row.safeOriginal,
        createdAt: row.created_at
      }));
      return res.json({ items });
    } catch (err) {
      console.warn('DB list error', err && err.message);
      return res.status(500).json({ error: 'DB list error' });
    }
  }
  const all = Object.values(mappings).sort((a,b)=> new Date(b.createdAt)-new Date(a.createdAt));
  return res.json({ items: all.slice(offset, offset+limit) });
});

// upload -> store into DB (BYTEA)
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!pool) return res.status(500).json({ error: 'Database not configured. Cannot store files.' });

    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();

    const entry = {
      token,
      originalName,
      safeOriginal,
      size: req.file.size,
      mime: req.file.mimetype,
      createdAt: new Date().toISOString(),
      storage: 'db'
    };

    // Save bytes+metadata to DB, with automatic migration retry if needed
    try {
      await saveFileToDB(token, entry, req.file.buffer);
    } catch (err) {
      console.error('Failed saving file to DB:', err && err.message);
      return res.status(500).json({ error: 'Failed saving file to DB', details: err && err.message });
    }

    mappings[token] = entry;
    saveMappingsToDisk();
    // build url
    const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
    const proto = protoHeader || req.protocol || 'https';
    const host = req.get('host');
    const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = `${origin}${sharePath}`;

    return res.json({ token, url: fileUrl, sharePath, info: entry, displayName: truncateMiddle(originalName, 36) });
  } catch (err) {
    console.error('Upload error', err && err.message);
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// serve file from DB
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!pool) return res.status(500).send('Database not configured');
    const fetched = await fetchFileFromDB(token);
    if (!fetched) return res.status(404).send('Not found');
    const entry = fetched.data || mappings[token];
    const fileBuf = fetched.file;
    if (!fileBuf) {
      console.warn('No file bytes stored in DB for token', token);
      return res.status(410).send('File removed or missing');
    }
    const mime = (entry && entry.mime) || 'application/octet-stream';
    const suggestedName = (entry && (entry.originalName || entry.safeOriginal)) || `file-${token}`;
    res.setHeader('Content-Type', mime);
    res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    return res.send(fileBuf);
  } catch (err) {
    console.error('Error serving file', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// admin: run migrations manually (protected)
app.post('/_admin/run-migrations', async (req, res) => {
  const key = req.headers['x-migrate-key'] || req.query.key;
  if (!MIGRATE_KEY || key !== MIGRATE_KEY) return res.status(403).json({ ok:false, error:'forbidden' });
  try {
    await runMigrations();
    return res.json({ ok:true, message: 'migrations run' });
  } catch (err) {
    return res.status(500).json({ ok:false, error: err && err.message });
  }
});

app.get('/_admin/token/:token', async (req, res) => {
  const token = req.params.token;
  try {
    if (!pool) {
      const entry = mappings[token];
      if (!entry) return res.status(404).json({ ok:false, error:'not found' });
      return res.json({ ok:true, token, entry, hasFileInDB: false });
    }
    const r = await pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token = $1', [token]);
    if (!r.rowCount) return res.status(404).json({ ok:false, error:'not found' });
    return res.json({ ok:true, token, entry: r.rows[0].data, hasFileInDB: r.rows[0].has_file });
  } catch (err) {
    console.error('Admin fetch error', err && err.message);
    return res.status(500).json({ ok:false, error:'server error' });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  if (err) {
    console.error('Unhandled error:', err && err.message);
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
