// server.js - single DATABASE_URL robust uploader (Postgres BYTEA)
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

// Limits and config
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5368709120);             // 5GB default (multer)
const MAX_DB_SAVE_SIZE = Number(process.env.MAX_DB_SAVE_SIZE || 500 * 1024 * 1024); // 500MB default for DB writes
const ALLOWED_MIME = (process.env.ALLOWED_MIME || '').split(',').map(s=>s.trim()).filter(Boolean);
const DB_WRITE_ATTEMPTS = Number(process.env.DB_WRITE_ATTEMPTS || 3);
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 10);
const PG_IDLE_TIMEOUT_MS = Number(process.env.PG_IDLE_TIMEOUT_MS || 30000);
const PG_CONN_TIMEOUT_MS = Number(process.env.PG_CONN_TIMEOUT_MS || 10000);

// Postgres pool (single)
let pool = null;
let poolInfo = null; // { pool, connString, cfg }

function createPoolFromEnv() {
  const conn = (process.env.DATABASE_URL || '').trim();
  if (!conn) {
    console.warn('No DATABASE_URL found in env.');
    pool = null;
    poolInfo = null;
    return;
  }
  try {
    const { Pool } = require('pg');
    const sslVal = process.env.DATABASE_SSL;
    const cfg = {
      connectionString: conn,
      max: PG_POOL_MAX,
      idleTimeoutMillis: PG_IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: PG_CONN_TIMEOUT_MS
    };
    if (sslVal === 'true' || sslVal === '1') cfg.ssl = { rejectUnauthorized: false };
    pool = new Pool(cfg);
    pool.on('error', (err) => console.error('Unexpected PG client error:', err && err.message));
    poolInfo = { pool, connString: conn, cfg };
    console.log('Postgres pool created for DATABASE_URL (max=%d idleMs=%d connTimeoutMs=%d)', PG_POOL_MAX, PG_IDLE_TIMEOUT_MS, PG_CONN_TIMEOUT_MS);
  } catch (err) {
    console.error('pg not installed or failed to init pool:', err && err.message);
    pool = null;
    poolInfo = null;
  }
}

async function closePool() {
  if (pool && typeof pool.end === 'function') {
    try {
      await pool.end();
    } catch (e) {
      console.warn('Error closing pool:', e && e.message);
    }
  }
  pool = null;
  poolInfo = null;
  console.log('Pool closed.');
}

createPoolFromEnv();

// in-memory metadata cache + disk fallback
let mappings = {};

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

// migrations
async function runMigrations() {
  if (!pool) {
    console.warn('No pool - skipping migrations.');
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

// helpers: token, filename
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
  const ext = path.extname(name || '');
  const base = path.basename(name || '', ext);
  const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
  const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
  return (safeBase + safeExt) || 'file';
}
function truncateMiddle(name, maxLen = 30) {
  if (!name) return name;
  if (name.length <= maxLen) return name;
  const keep = Math.floor((maxLen - 1) / 2);
  return `${name.slice(0, keep)}…${name.slice(name.length - keep)}`;
}

// DB interaction with retry + backoff + recreate pool on connection termination
function wait(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function saveMappingMetadataToDB(token, entry) {
  if (!pool) return;
  const q = `
    INSERT INTO uploads (token, data, created_at)
    VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, created_at = NOW();
  `;
  try {
    await pool.query(q, [token, JSON.stringify(entry)]);
    return true;
  } catch (err) {
    const msg = (err && err.message || '').toLowerCase();
    if (msg.includes('column "file_data"') || msg.includes('does not exist')) {
      try {
        await runMigrations();
        await pool.query(q, [token, JSON.stringify(entry)]);
        return true;
      } catch(e){ return false; }
    }
    return false;
  }
}

async function saveFileToDB(token, entry, buffer) {
  if (!pool) throw new Error('No database pool available');
  const q = `
    INSERT INTO uploads (token, data, file_data, created_at)
    VALUES ($1, $2::jsonb, $3, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, file_data = $3, created_at = NOW();
  `;

  let lastErr = null;
  for (let attempt = 1; attempt <= DB_WRITE_ATTEMPTS; attempt++) {
    try {
      await pool.query(q, [token, JSON.stringify(entry), buffer]);
      entry.storage = 'DATABASE_URL';
      console.log(`saveFileToDB: saved token ${token} on DATABASE_URL (attempt ${attempt})`);
      return 'DATABASE_URL';
    } catch (err) {
      lastErr = err;
      const msg = (err && err.message || '').toLowerCase();
      console.warn(`Attempt ${attempt} save failed:`, msg);

      // missing column -> run migrations then retry immediately once
      if (msg.includes('column "file_data"') || msg.includes('does not exist')) {
        try {
          console.log('Running migrations then retrying...');
          await runMigrations();
          await pool.query(q, [token, JSON.stringify(entry), buffer]);
          entry.storage = 'DATABASE_URL';
          console.log(`saveFileToDB: retry after migration succeeded for token ${token}`);
          return 'DATABASE_URL';
        } catch (e2) {
          lastErr = e2;
          console.error('Migration retry failed:', e2 && e2.message);
          // continue to retry logic below
        }
      }

      // payload too large -> throw with code
      if (msg.includes('value too long') || msg.includes('out of memory') || msg.includes('too large')) {
        const wrap = new Error('DB_PAYLOAD_TOO_LARGE: ' + msg);
        wrap.code = 'DB_PAYLOAD_TOO_LARGE';
        throw wrap;
      }

      // connection terminated/reset -> try to recreate pool once (if not last attempt)
      if (msg.includes('connection terminated unexpectedly') || msg.includes('connection reset') || (err && err.code === 'ECONNRESET')) {
        console.warn('Detected connection termination. Will attempt to close and recreate pool (once).');
        try {
          await closePool();
          createPoolFromEnv();
          if (pool) {
            await runMigrations();
            console.log('Recreated pool and ran migrations; retrying now.');
          } else {
            console.warn('Recreate pool failed; will wait then retry if attempts remain.');
          }
        } catch (re) {
          console.warn('Recreate attempt failed:', re && re.message);
        }
      }

      // exponential backoff before next attempt (if any)
      if (attempt < DB_WRITE_ATTEMPTS) {
        const backoff = Math.min(2000 * Math.pow(2, attempt-1), 15000); // 2s,4s,8s cap 15s
        console.log(`Waiting ${backoff}ms before next DB attempt...`);
        await wait(backoff);
        continue;
      } else {
        break;
      }
    }
  }

  const finalMsg = lastErr && lastErr.message ? lastErr.message : 'All DB attempts failed';
  const e = new Error(finalMsg);
  if (lastErr && lastErr.code) e.code = lastErr.code;
  throw e;
}

async function fetchFileFromDB(token) {
  if (!pool) throw new Error('No database pool available');
  try {
    const r = await pool.query('SELECT data, file_data FROM uploads WHERE token = $1', [token]);
    if (!r.rowCount) return null;
    return { data: r.rows[0].data, file: r.rows[0].file_data };
  } catch (err) {
    console.warn('fetchFileFromDB error:', err && err.message);
    throw err;
  }
}

// multer
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: Number(MAX_FILE_SIZE) } });

const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// startup
loadMappingsFromDisk();
(async () => {
  try {
    if (!pool) {
      console.error('No DATABASE_URL configured. Server will still run but uploads to DB will fail.');
    } else {
      await runMigrations();
      // load metadata
      try {
        const res = await pool.query('SELECT token, data, created_at FROM uploads');
        const dbm = {};
        (res.rows || []).forEach(r => {
          dbm[r.token] = r.data;
          dbm[r.token].createdAt = r.created_at || dbm[r.token].createdAt;
        });
        mappings = Object.assign({}, mappings, dbm);
        // persist mapping-only entries
        for (const [token, entry] of Object.entries(mappings)) {
          try { await saveMappingMetadataToDB(token, entry); } catch(e){/*ignore*/ }
        }
        console.log('Loaded mappings from DB:', Object.keys(mappings).length);
        saveMappingsToDisk();
      } catch (err) {
        console.warn('Failed loading mappings from DB on startup:', err && err.message);
      }
    }
  } catch (err) {
    console.error('Startup error (migrations/load):', err && err.message);
  }
})();

// endpoints

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

app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      console.warn('/upload: no file provided. headers:', { 'content-length': req.headers['content-length'], 'content-type': req.headers['content-type'] });
      return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('/upload incoming:', { name: req.file.originalname, size: req.file.size, mime: req.file.mimetype });

    if (ALLOWED_MIME.length && !ALLOWED_MIME.includes(req.file.mimetype)) {
      console.warn('/upload: disallowed mime', req.file.mimetype);
      return res.status(400).json({ error: 'Invalid file type', allowed: ALLOWED_MIME });
    }

    if (req.file.size > MAX_DB_SAVE_SIZE) {
      console.warn('/upload: file too large for DB save', { size: req.file.size, max: MAX_DB_SAVE_SIZE });
      return res.status(413).json({ error: 'File too large for database storage', maxAllowed: MAX_DB_SAVE_SIZE });
    }

    if (!pool) {
      console.error('/upload: no DB pool configured');
      return res.status(503).json({ error: 'Database not configured' });
    }

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
      storage: 'pending'
    };

    try {
      const savedOn = await saveFileToDB(token, entry, req.file.buffer);
      entry.storage = savedOn || entry.storage;
      mappings[token] = entry;
      saveMappingsToDisk();
    } catch (err) {
      const msg = (err && err.message) || '';
      const code = err && err.code;
      console.error('saveFileToDB error for token', token, { code, msg });
      if (code === 'DB_PAYLOAD_TOO_LARGE' || (msg || '').toLowerCase().includes('payload too large')) {
        return res.status(413).json({ error: 'File too large for database storage', details: msg });
      }
      if ((msg || '').toLowerCase().includes('connection') || (code === 'ECONNRESET')) {
        return res.status(502).json({ error: 'Database connection error', details: msg });
      }
      return res.status(500).json({ error: 'Failed saving file to DB', details: msg || 'unknown' });
    }

    const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
    const proto = protoHeader || req.protocol || 'https';
    const host = req.get('host');
    const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = `${origin}${sharePath}`;

    // best-effort persist metadata
    saveMappingMetadataToDB(token, mappings[token]).catch(()=>{});

    console.log('Upload OK', { token, url: fileUrl });
    return res.json({ token, url: fileUrl, sharePath, info: entry, displayName: truncateMiddle(originalName, 36) });

  } catch (err) {
    console.error('/upload unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!pool) return res.status(500).send('Database not configured');
    const fetched = await fetchFileFromDB(token);
    if (!fetched) {
      const m = mappings[token];
      if (m && m.url) return res.redirect(302, m.url);
      return res.status(404).send('Not found');
    }
    const entry = fetched.data || mappings[token];
    const fileBuf = fetched.file;
    if (!fileBuf) {
      console.warn('No file bytes stored in DB for token', token);
      if (entry && entry.url) return res.redirect(302, entry.url);
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
    try {
      const r = await pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token = $1', [token]);
      if (!r.rowCount) return res.status(404).json({ ok:false, error:'not found' });
      return res.json({ ok:true, token, entry: r.rows[0].data, hasFileInDB: r.rows[0].has_file });
    } catch (err) {
      console.warn('Admin token fetch error:', err && err.message);
      const entry = mappings[token];
      if (!entry) return res.status(404).json({ ok:false, error:'not found' });
      return res.json({ ok:true, token, entry, hasFileInDB: false });
    }
  } catch (err) {
    console.error('Admin fetch error', err && err.message);
    return res.status(500).json({ ok:false, error:'server error' });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

// global error handler
app.use((err, req, res, next) => {
  if (err && (err.code === 'LIMIT_FILE_SIZE' || err.code === 'LIMIT_FIELD_VALUE')) {
    return res.status(413).json({ error: 'File too large. Max allowed: ' + MAX_FILE_SIZE });
  }
  const msg = (err && err.message || '').toLowerCase();
  if (msg.includes('value too long') || msg.includes('too large') || msg.includes('out of memory')) {
    return res.status(413).json({ error: 'Payload too large for storage backend', details: err && err.message });
  }
  if (err) {
    console.error('Unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
