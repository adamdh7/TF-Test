// server.js - robust multi-DB uploader with pool-recreate-on-transient-errors
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

// Top-level configurable limits
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5368709120); // multer max (default 5GB)
const MAX_DB_SAVE_SIZE = Number(process.env.MAX_DB_SAVE_SIZE || 500 * 1024 * 1024); // default 500MB for DB writes
const ALLOWED_MIME = (process.env.ALLOWED_MIME || '').split(',').map(s => s.trim()).filter(Boolean);

// Pool tuning (env override)
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 6);
const PG_CONN_TIMEOUT_MS = Number(process.env.PG_CONN_TIMEOUT_MS || 20000);
const PG_IDLE_TIMEOUT_MS = Number(process.env.PG_IDLE_TIMEOUT_MS || 30000);

// --- support multiple DATABASE_URL* entries (normalize spaces in env keys) ---
let poolInfos = []; // array of { name: 'DATABASE_URL1', rawEnvKey: 'DATABASE_URL 1', pool, connString, cfg }

function createPoolsFromEnv() {
  const envKeys = Object.keys(process.env || {});
  // match keys like: DATABASE_URL, DATABASE_URL1, DATABASE_URL 1, DATABASE_URL   2
  const matches = envKeys.filter(k => /^DATABASE_URL(?:\s*\d*)$/i.test(k));
  if (!matches.length) {
    console.warn('No DATABASE_URL* variables found in environment.');
    return;
  }

  // Normalize keys (remove whitespace) and dedupe by normalized name (keep first occurrence)
  const normalized = [];
  const seen = new Set();
  for (const raw of matches) {
    const norm = raw.replace(/\s+/g, ''); // remove all whitespace
    if (seen.has(norm)) {
      console.warn(`Duplicate normalized DATABASE_URL key "${norm}" from raw "${raw}" - ignoring duplicate.`);
      continue;
    }
    seen.add(norm);
    normalized.push({ raw, norm });
  }

  // Sort: DATABASE_URL (no suffix) first, then numeric ascending by suffix
  normalized.sort((a, b) => {
    const getNum = item => {
      const m = item.norm.match(/^DATABASE_URL(?:([0-9]+))?$/i);
      return m && m[1] ? parseInt(m[1], 10) : 0;
    };
    return getNum(a) - getNum(b);
  });

  try {
    const { Pool } = require('pg');
    for (const item of normalized) {
      const rawKey = item.raw;
      const normKey = item.norm; // e.g. DATABASE_URL, DATABASE_URL1
      const conn = (process.env[rawKey] || '').trim();
      if (!conn) continue;

      // determine SSL setting: check multiple possible env names (normalized and raw with spaces)
      const suffix = normKey === 'DATABASE_URL' ? '' : normKey.replace('DATABASE_URL', '');
      const sslKeys = [`DATABASE_SSL${suffix}`, `DATABASE_SSL ${suffix}`, 'DATABASE_SSL'];
      let sslVal = null;
      for (const sKey of sslKeys) {
        if (process.env[sKey] !== undefined) { sslVal = process.env[sKey]; break; }
      }

      // pool config to avoid too many open connections and to use timeouts
      const cfg = {
        connectionString: conn,
        max: PG_POOL_MAX,
        idleTimeoutMillis: PG_IDLE_TIMEOUT_MS,
        connectionTimeoutMillis: PG_CONN_TIMEOUT_MS,
      };
      if (sslVal === 'true' || sslVal === '1') cfg.ssl = { rejectUnauthorized: false };

      try {
        const pool = new Pool(cfg);
        pool.on('error', (err) => console.error(`Unexpected PG client error (${normKey} / ${rawKey}):`, err && err.message));
        poolInfos.push({ name: normKey, rawEnvKey: rawKey, pool, connString: conn, cfg });
        console.log(`Postgres pool created for ${rawKey} (normalized: ${normKey})`);
      } catch (err) {
        console.error(`Failed to create pool for ${rawKey} (normalized: ${normKey}):`, err && err.message);
      }
    }
  } catch (err) {
    console.error('pg not installed or failed to init. Install "pg" and set DATABASE_URL* to enable DB.', err && err.message);
    poolInfos = [];
  }
}

// create pools
createPoolsFromEnv();

// helper to recreate a pool if it's suspected dead
async function recreatePool(pinfo) {
  try {
    console.log(`Recreating pool for ${pinfo.name}...`);
    try { await pinfo.pool.end(); } catch (e) { /* ignore */ }
    const { Pool } = require('pg');
    const newPool = new Pool(pinfo.cfg);
    newPool.on('error', (err) => console.error(`Unexpected PG client error (recreated ${pinfo.name}):`, err && err.message));
    pinfo.pool = newPool;
    console.log(`Recreated pool for ${pinfo.name}`);
    return true;
  } catch (err) {
    console.error(`Failed to recreate pool for ${pinfo.name}:`, err && err.message);
    return false;
  }
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

// run migrations on single pool
async function runMigrationsOnPool(poolInfo) {
  if (!poolInfo || !poolInfo.pool) return;
  try {
    const pool = poolInfo.pool;
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
    console.log(`DB migration applied for ${poolInfo.name}`);
  } catch (err) {
    console.error(`DB migration failed for ${poolInfo.name}:`, err && err.message);
    throw err;
  }
}

// run migrations on all pools (best-effort)
async function runMigrationsAll() {
  for (const pinfo of poolInfos) {
    try {
      await runMigrationsOnPool(pinfo);
    } catch (err) {
      console.warn('Continuing despite migration error on', pinfo.name);
    }
  }
}

// helpers to interact with DBs: try sequentially across pools

async function saveFileToDB(token, entry, buffer) {
  if (!poolInfos.length) throw new Error('No database pools available');
  const q = `
    INSERT INTO uploads (token, data, file_data, created_at)
    VALUES ($1, $2::jsonb, $3, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, file_data = $3, created_at = NOW();
  `;

  let lastErr = null;
  for (const pinfo of poolInfos) {
    try {
      await pinfo.pool.query(q, [token, JSON.stringify(entry), buffer]);
      // record which pool saved it
      entry.storage = pinfo.name;
      console.log(`saveFileToDB: saved token ${token} on ${pinfo.name}`);
      return pinfo.name;
    } catch (err) {
      lastErr = err;
      const msg = (err && err.message) || '';
      console.warn(`Save to ${pinfo.name} failed:`, msg);

      // handle missing column -> migration + retry once
      if (msg.includes('column "file_data"') || msg.includes('does not exist')) {
        try {
          console.log(`Attempting migrations on ${pinfo.name} then retrying save...`);
          await runMigrationsOnPool(pinfo);
          await pinfo.pool.query(q, [token, JSON.stringify(entry), buffer]);
          entry.storage = pinfo.name;
          console.log('Retry saveFileToDB after migration succeeded for token', token, 'on', pinfo.name);
          return pinfo.name;
        } catch (err2) {
          console.error(`Retry after migration failed on ${pinfo.name}:`, err2 && err2.message);
          lastErr = err2;
          continue; // try next pool
        }
      }

      const low = msg.toLowerCase();
      // fast-fail for payload-too-large
      if (low.includes('value too long') || low.includes('out of memory') || low.includes('too large')) {
        const wrap = new Error(`DB storage error on ${pinfo.name}: ${msg}`);
        wrap.code = 'DB_PAYLOAD_TOO_LARGE';
        throw wrap;
      }

      // transient connection termination: try to recreate pool and retry once
      if (low.includes('connection terminated unexpectedly') || low.includes('server closed the connection unexpectedly') || low.includes('terminating connection')) {
        console.warn(`Detected transient connection error on ${pinfo.name} — attempt to recreate pool and retry once.`);
        const ok = await recreatePool(pinfo);
        if (ok) {
          try {
            await pinfo.pool.query(q, [token, JSON.stringify(entry), buffer]);
            entry.storage = pinfo.name;
            console.log(`saveFileToDB: saved token ${token} on ${pinfo.name} after recreate`);
            return pinfo.name;
          } catch (err2) {
            console.error(`Retry after recreate failed on ${pinfo.name}:`, err2 && err2.message);
            lastErr = err2;
            continue; // try next pool
          }
        } else {
          console.warn(`Recreate pool failed for ${pinfo.name}, trying next pool...`);
          continue;
        }
      }

      // Other errors: try next pool (auth/conn issue)
      continue;
    }
  }
  // all pools failed
  const e = new Error('All DB pools failed to save file: ' + (lastErr && lastErr.message ? lastErr.message : 'unknown'));
  if (lastErr && lastErr.code) e.code = lastErr.code;
  throw e;
}

async function fetchFileFromDB(token) {
  if (!poolInfos.length) throw new Error('No database pools available');
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT data, file_data FROM uploads WHERE token = $1', [token]);
      if (!r.rowCount) continue;
      return { data: r.rows[0].data, file: r.rows[0].file_data, pool: pinfo.name };
    } catch (err) {
      console.warn(`Error fetching from ${pinfo.name}:`, err && err.message);
      continue;
    }
  }
  return null;
}

async function saveMappingMetadataToDB(token, entry) {
  if (!poolInfos.length) return;
  const q = `
    INSERT INTO uploads (token, data, created_at)
    VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (token)
    DO UPDATE SET data = $2::jsonb, created_at = NOW();
  `;
  for (const pinfo of poolInfos) {
    try {
      await pinfo.pool.query(q, [token, JSON.stringify(entry)]);
      return pinfo.name;
    } catch (err) {
      const msg = (err && err.message) || '';
      if (msg.includes('column "file_data"') || msg.includes('does not exist')) {
        try {
          await runMigrationsOnPool(pinfo);
          await pinfo.pool.query(q, [token, JSON.stringify(entry)]);
          return pinfo.name;
        } catch (e2) {
          continue;
        }
      }
      continue;
    }
  }
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

// multer memory storage (use MAX_FILE_SIZE)
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: Number(MAX_FILE_SIZE) } });

const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// startup: load disk mappings then DB(s) + migrations, then start server
loadMappingsFromDisk();
(async () => {
  try {
    if (!poolInfos.length) {
      console.error('No DATABASE_URL configured. Server will still run but uploads to DB will fail.');
    } else {
      // run migrations on each pool and load metadata from each DB
      await runMigrationsAll();

      const dbm = {};
      for (const pinfo of poolInfos) {
        try {
          const res = await pinfo.pool.query('SELECT token, data, created_at FROM uploads');
          (res.rows || []).forEach(r => {
            const existing = dbm[r.token];
            if (!existing) {
              dbm[r.token] = r.data;
              dbm[r.token].createdAt = r.created_at || dbm[r.token].createdAt;
            } else {
              const existingDate = new Date(existing.createdAt || 0).getTime();
              const newDate = new Date(r.created_at || 0).getTime();
              if (newDate >= existingDate) {
                dbm[r.token] = r.data;
                dbm[r.token].createdAt = r.created_at || dbm[r.token].createdAt;
              }
            }
          });
        } catch (err) {
          console.warn(`Failed loading mappings from ${pinfo.name}:`, err && err.message);
        }
      }

      // Merge disk mappings and DB mappings (DB overrides disk)
      mappings = Object.assign({}, mappings, dbm);

      // persist metadata-only entries to DB if any local-only exists
      for (const [token, entry] of Object.entries(mappings)) {
        try {
          await saveMappingMetadataToDB(token, entry).catch(()=>{});
        } catch (e) { /* ignore */ }
      }

      console.log('Loaded mappings from DBs:', Object.keys(mappings).length);
      saveMappingsToDisk();
    }
  } catch (err) {
    console.error('Startup error (migrations/load):', err && err.message);
  }
})();

// pagination (metadata)
app.get('/uploads', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 200);
  const offset = Math.max(Number(req.query.offset) || 0, 0);

  if (poolInfos.length) {
    try {
      // collect items from each DB
      let itemsRaw = [];
      for (const pinfo of poolInfos) {
        try {
          const q = `SELECT token, data->>'originalName' as name, data->>'safeOriginal' as safeOriginal, created_at
                     FROM uploads ORDER BY created_at DESC LIMIT $1 OFFSET $2`;
          const r = await pinfo.pool.query(q, [limit, offset]);
          const rows = r.rows.map(row => ({
            token: row.token,
            displayName: truncateMiddle(row.name || row.safeOriginal, 30),
            safeOriginal: row.safeOriginal,
            createdAt: row.created_at,
            source: pinfo.name
          }));
          itemsRaw = itemsRaw.concat(rows);
        } catch (err) {
          console.warn('DB list error for', pinfo.name, err && err.message);
          continue;
        }
      }
      // dedupe by token (keep newest createdAt)
      const map = {};
      for (const it of itemsRaw) {
        if (!map[it.token] || new Date(it.createdAt) > new Date(map[it.token].createdAt)) map[it.token] = it;
      }
      const items = Object.values(map).sort((a,b)=> new Date(b.createdAt) - new Date(a.createdAt)).slice(offset, offset + limit);
      return res.json({ items });
    } catch (err) {
      console.warn('DB combined list error', err && err.message);
      return res.status(500).json({ error: 'DB list error' });
    }
  }

  const all = Object.values(mappings).sort((a,b)=> new Date(b.createdAt)-new Date(a.createdAt));
  return res.json({ items: all.slice(offset, offset+limit) });
});

// upload -> store into DB (BYTEA)
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      console.warn('/upload: no file provided. headers:', {
        'content-length': req.headers['content-length'],
        'content-type': req.headers['content-type']
      });
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // basic logging
    console.log('/upload incoming:', { name: req.file.originalname, size: req.file.size, mime: req.file.mimetype });

    // optional mime allowlist
    if (ALLOWED_MIME.length && !ALLOWED_MIME.includes(req.file.mimetype)) {
      console.warn('/upload: disallowed mime', req.file.mimetype);
      return res.status(400).json({ error: 'Invalid file type', allowed: ALLOWED_MIME });
    }

    // do not attempt to save very large buffers into Postgres
    if (req.file.size > MAX_DB_SAVE_SIZE) {
      console.warn('/upload: file too large for DB save', { size: req.file.size, max: MAX_DB_SAVE_SIZE });
      return res.status(413).json({ error: 'File too large for database storage', maxAllowed: MAX_DB_SAVE_SIZE });
    }

    if (!poolInfos.length) {
      console.error('/upload: no DB pools configured');
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

    // Save bytes+metadata to DB, with automatic migration retry if needed and fallback across pools
    let savedOn = null;
    try {
      savedOn = await saveFileToDB(token, entry, req.file.buffer);
      entry.storage = savedOn || entry.storage;
      mappings[token] = entry;
      saveMappingsToDisk();
    } catch (err) {
      const code = err && err.code;
      const msg = err && err.message;
      console.error('saveFileToDB error for token', token, { code, msg });
      if (code === 'DB_PAYLOAD_TOO_LARGE') {
        return res.status(413).json({ error: 'File too large for database storage', details: msg });
      }
      const lower = (msg || '').toLowerCase();
      if (lower.includes('authentication') || lower.includes('permission denied') || lower.includes('could not connect') || lower.includes('connect')) {
        return res.status(502).json({ error: 'Database connection error', details: msg });
      }
      return res.status(500).json({ error: 'Failed saving file to DB', details: msg || 'unknown' });
    }

    // build url
    const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
    const proto = protoHeader || req.protocol || 'https';
    const host = req.get('host');
    const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = `${origin}${sharePath}`;

    // best-effort metadata persist to at least one DB
    saveMappingMetadataToDB(token, mappings[token]).catch(e => console.warn('saveMappingMetadataToDB failed (non-fatal):', e && e.message));

    console.log('Upload OK', { token, savedOn, url: fileUrl });
    return res.json({ token, url: fileUrl, sharePath, info: entry, displayName: truncateMiddle(originalName, 36) });

  } catch (err) {
    console.error('/upload unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// serve file from DB (or redirect if metadata has external url)
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!poolInfos.length) return res.status(500).send('Database not configured');

    const fetched = await fetchFileFromDB(token);
    if (!fetched) {
      // fallback to mapping (maybe stored externally)
      const m = mappings[token];
      if (m && m.url) return res.redirect(302, m.url);
      return res.status(404).send('Not found');
    }

    const entry = fetched.data || mappings[token];
    const fileBuf = fetched.file;
    if (!fileBuf) {
      console.warn('No file bytes stored in DB for token', token);
      // if metadata contains URL, redirect
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

// admin: run migrations manually (protected)
app.post('/_admin/run-migrations', async (req, res) => {
  const key = req.headers['x-migrate-key'] || req.query.key;
  if (!MIGRATE_KEY || key !== MIGRATE_KEY) return res.status(403).json({ ok:false, error:'forbidden' });
  try {
    await runMigrationsAll();
    return res.json({ ok:true, message: 'migrations run on all pools (best-effort)' });
  } catch (err) {
    return res.status(500).json({ ok:false, error: err && err.message });
  }
});

// admin: fetch token info across pools
app.get('/_admin/token/:token', async (req, res) => {
  const token = req.params.token;
  try {
    if (!poolInfos.length) {
      const entry = mappings[token];
      if (!entry) return res.status(404).json({ ok:false, error:'not found' });
      return res.json({ ok:true, token, entry, hasFileInDB: false });
    }
    for (const pinfo of poolInfos) {
      try {
        const r = await pinfo.pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token = $1', [token]);
        if (!r.rowCount) continue;
        return res.json({ ok:true, token, entry: r.rows[0].data, hasFileInDB: r.rows[0].has_file, storedOn: pinfo.name });
      } catch (err) {
        continue;
      }
    }
    const entry = mappings[token];
    if (!entry) return res.status(404).json({ ok:false, error:'not found' });
    return res.json({ ok:true, token, entry, hasFileInDB: false });
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
