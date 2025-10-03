// server.js - multi-DB fallback: try DATABASE_URL, DATABASE_URL1, DATABASE_URL2 ... -> disk
require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const multer = require('multer');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const PENDING_DIR = path.join(UPLOAD_DIR, 'pending');

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 8 * 1024 * 1024); // default 8MB (larger -> fewer chunks)
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PENDING_RETRY_INTERVAL = Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000; // 30s

try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch(e){}

// -------------------- create pools from env (DATABASE_URL, DATABASE_URL1, ...) --------------------
let poolInfos = []; // array of { name, pool, connString }

function createPoolsFromEnv() {
  const keys = Object.keys(process.env).filter(k => /^DATABASE_URL(?:\d*)$/.test(k));
  if (!keys.length) {
    console.error('No DATABASE_URL* variables found in environment. Exiting.');
    process.exit(1);
  }
  // sort: DATABASE_URL first then numeric ascending
  keys.sort((a,b) => {
    const getNum = k => (k === 'DATABASE_URL' ? 0 : parseInt(k.replace('DATABASE_URL',''),10) || 0);
    return getNum(a) - getNum(b);
  });

  try {
    for (const key of keys) {
      const conn = (process.env[key] || '').trim();
      if (!conn) continue;
      // SSL per-pool: DATABASE_SSL, DATABASE_SSL1, ...
      const suffix = key === 'DATABASE_URL' ? '' : key.replace('DATABASE_URL','');
      const sslEnvKey = `DATABASE_SSL${suffix}`;
      const sslVal = process.env[sslEnvKey] || process.env['DATABASE_SSL'];
      const cfg = { connectionString: conn, max: PG_POOL_MAX};
      if (sslVal === 'true' || sslVal === '1') cfg.ssl = { rejectUnauthorized: false };
      try {
        const pool = new Pool(cfg);
        pool.on('error', (err) => console.error(`Unexpected PG client error (${key}):`, err && err.message));
        poolInfos.push({ name: key, pool, connString: conn });
        console.log(`Postgres pool created for ${key}`);
      } catch (err) {
        console.error(`Failed to create pool for ${key}:`, err && err.message);
      }
    }
  } catch (err) {
    console.error('Failed creating pools from env:', err && err.message);
  }

  if (!poolInfos.length) {
    console.error('No valid DB pools created. Exiting.');
    process.exit(1);
  }
}
createPoolsFromEnv();

// -------------------- mappings (in-memory + disk) --------------------
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
loadMappingsFromDisk();

// -------------------- helpers --------------------
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i = 0; i < len; i++) t += chars[Math.floor(Math.random() * chars.length)];
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

// -------------------- migrations per-pool --------------------
async function runMigrationsOnPool(pinfo) {
  if (!pinfo || !pinfo.pool) return;
  try {
    const pool = pinfo.pool;
    await pool.query(`
      CREATE TABLE IF NOT EXISTS uploads (
        token TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        file_data BYTEA,
        created_at TIMESTAMPTZ DEFAULT now()
      );
    `);
    await pool.query(`ALTER TABLE uploads ADD COLUMN IF NOT EXISTS file_data BYTEA;`);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS file_chunks (
        token TEXT NOT NULL,
        seq INTEGER NOT NULL,
        chunk BYTEA NOT NULL,
        PRIMARY KEY (token, seq)
      );
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_chunks_token_${pinfo.name} ON file_chunks(token);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_uploads_created_at_${pinfo.name} ON uploads(created_at);`);
    console.log(`DB migration applied for ${pinfo.name}`);
  } catch (err) {
    console.error(`DB migration failed for ${pinfo.name}:`, err && err.message);
    throw err;
  }
}
async function runMigrationsAll() {
  for (const pinfo of poolInfos) {
    try {
      await runMigrationsOnPool(pinfo);
    } catch (err) {
      console.warn('Continuing despite migration error on', pinfo.name);
    }
  }
}

// -------------------- DB helpers with multi-pool fallback --------------------

// Try to save all chunks into a pool inside a transaction. If anything fails, rollback and try next pool.
async function saveChunksToDBAcrossPools(token, buffer) {
  if (!poolInfos.length) throw new Error('No DB pools available');
  const totalPieces = Math.ceil(buffer.length / CHUNK_MAX_SIZE);
  let lastErr = null;
  for (const pinfo of poolInfos) {
    const pool = pinfo.pool;
    try {
      await pool.query('BEGIN');
      let seq = 0;
      for (let offset = 0; offset < buffer.length; offset += CHUNK_MAX_SIZE) {
        const piece = buffer.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buffer.length));
        await pool.query('INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)', [token, seq, piece]);
        seq++;
      }
      await pool.query('COMMIT');
      // success: return pool name used
      return pinfo.name;
    } catch (err) {
      lastErr = err;
      try { await pool.query('ROLLBACK'); } catch (e) {}
      console.warn(`Save chunks to ${pinfo.name} failed:`, err && err.message);
      // try next pool
      continue;
    }
  }
  const e = lastErr || new Error('All DB pools failed to save chunks');
  throw e;
}

// Save metadata to first pool that accepts it
async function saveMappingMetadataToDBAcrossPools(token, entry) {
  if (!poolInfos.length) return null;
  let lastErr = null;
  for (const pinfo of poolInfos) {
    try {
      await pinfo.pool.query(`INSERT INTO uploads (token, data, created_at) VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (token) DO UPDATE SET data = $2::jsonb, created_at = NOW();`, [token, JSON.stringify(entry)]);
      return pinfo.name;
    } catch (err) {
      lastErr = err;
      console.warn(`Save mapping metadata to ${pinfo.name} failed:`, err && err.message);
      continue;
    }
  }
  throw lastErr || new Error('All DB pools failed to save metadata');
}

// Fetch upload entry searching across pools (in configured order). Returns { data, hasFile, pool }
async function fetchUploadEntryAcrossPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token=$1', [token]);
      if (r.rowCount) return { data: r.rows[0].data, hasFile: r.rows[0].has_file, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchUploadEntry failed on ${pinfo.name}:`, err && err.message);
      continue;
    }
  }
  return null;
}

// Fetch file_data blob from whichever pool has it
async function fetchFileDataFromPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT file_data FROM uploads WHERE token=$1', [token]);
      if (r.rowCount && r.rows[0].file_data) return { buf: r.rows[0].file_data, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchFileData failed on ${pinfo.name}:`, err && err.message);
      continue;
    }
  }
  return null;
}

// Fetch chunks from whichever pool contains them
async function fetchAllChunksAcrossPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT seq, chunk FROM file_chunks WHERE token=$1 ORDER BY seq ASC', [token]);
      if (r.rowCount) return { rows: r.rows, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchAllChunks failed on ${pinfo.name}:`, err && err.message);
      continue;
    }
  }
  return { rows: [] };
}

// -------------------- pending disk helpers (unchanged) --------------------
function saveBufferToPending(token, entry, buffer) {
  const fn = `pending-${token}-${Date.now()}.bin`;
  const filePath = path.join(PENDING_DIR, fn);
  fs.writeFileSync(filePath, buffer);
  fs.writeFileSync(path.join(PENDING_DIR, fn + '.json'), JSON.stringify({ token, entry, filename: fn, timestamp: Date.now() }));
  console.log('Saved pending file to disk for token', token, filePath);
  return filePath;
}

async function attemptFlushPendingOneToPools(fileBaseName) {
  try {
    const jsonPath = path.join(PENDING_DIR, fileBaseName + '.json');
    const binPath = path.join(PENDING_DIR, fileBaseName);
    if (!fs.existsSync(jsonPath) || !fs.existsSync(binPath)) return false;
    const meta = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    const buffer = fs.readFileSync(binPath);
    try {
      const storedOn = await saveChunksToDBAcrossPools(meta.token, buffer);
      try { await saveMappingMetadataToDBAcrossPools(meta.token, meta.entry); } catch(e){}
      fs.unlinkSync(jsonPath); fs.unlinkSync(binPath);
      console.log('Pending flushed to DB (on ' + storedOn + ') for token', meta.token);
      return true;
    } catch (err) {
      console.warn('Pending flush to DB failed for', fileBaseName, err && err.message);
      return false;
    }
  } catch (err) {
    console.error('Error in attemptFlushPendingOneToPools', err && err.message);
    return false;
  }
}

async function pendingRetryLoop() {
  try {
    const files = fs.readdirSync(PENDING_DIR).filter(n => !n.endsWith('.json'));
    for (const bin of files) {
      await attemptFlushPendingOneToPools(bin);
    }
  } catch (err) {
    console.warn('pendingRetryLoop error', err && err.message);
  } finally {
    setTimeout(pendingRetryLoop, PENDING_RETRY_INTERVAL);
  }
}

// -------------------- startup migrations + load mappings --------------------
(async () => {
  try {
    await runMigrationsAll();
    // load metadata from all pools, preferring newest created_at when tokens duplicate
    const dbm = {};
    for (const pinfo of poolInfos) {
      try {
        const res = await pinfo.pool.query('SELECT token, data, created_at FROM uploads');
        (res.rows || []).forEach(r => {
          const existing = dbm[r.token];
          if (!existing) {
            dbm[r.token] = r.data;
            dbm[r.token].createdAt = r.created_at;
          } else {
            const existingDate = new Date(existing.createdAt || 0).getTime();
            const newDate = new Date(r.created_at || 0).getTime();
            if (newDate >= existingDate) dbm[r.token] = r.data, dbm[r.token].createdAt = r.created_at;
          }
        });
      } catch (err) {
        console.warn('Failed loading mappings from', pinfo.name, err && err.message);
      }
    }
    mappings = Object.assign({}, mappings, dbm);
    // persist local-only entries to DBs best-effort
    for (const [token, entry] of Object.entries(mappings)) {
      try { await saveMappingMetadataToDBAcrossPools(token, entry); } catch(e){ /* ignore */ }
    }
    saveMappingsToDisk();
    console.log('Loaded mappings from DBs:', Object.keys(mappings).length);
    pendingRetryLoop();
  } catch (err) {
    console.error('Startup error (migrations/load):', err && err.message);
  }
})();

// -------------------- express app --------------------
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// multer memory storage (beware for very big files)
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: MAX_FILE_SIZE } });

// upload endpoint
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const token = genToken(10);
    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const entry = {
      token,
      originalName,
      safeOriginal,
      size: req.file.size,
      mime: req.file.mimetype,
      createdAt: new Date().toISOString(),
      storage: 'db' // will be updated with pool name on success
    };

    const buf = req.file.buffer;
    try {
      const storedOn = await saveChunksToDBAcrossPools(token, buf);
      try { await saveMappingMetadataToDBAcrossPools(token, entry); } catch(e){ console.warn('metadata save failed', e && e.message); }
      entry.storage = storedOn;
      mappings[token] = entry;
      saveMappingsToDisk();

      const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
      const proto = protoHeader || req.protocol || 'https';
      const host = req.get('host');
      const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
      const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
      const fileUrl = `${origin}${sharePath}`;

      return res.json({ token, url: fileUrl, sharePath, info: entry });
    } catch (dbErr) {
      console.error('Save to all DB pools failed:', dbErr && dbErr.message);
      // fallback to disk
      try {
        saveBufferToPending(token, entry, buf);
        entry.storage = 'pending_disk';
        mappings[token] = entry;
        saveMappingsToDisk();

        const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
        const proto = protoHeader || req.protocol || 'https';
        const host = req.get('host');
        const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
        const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
        const fileUrl = `${origin}${sharePath}`;
        return res.json({ token, url: fileUrl, sharePath, info: entry, note: 'saved-locally-pending-db' });
      } catch (diskErr) {
        console.error('Disk fallback failed:', diskErr && diskErr.message);
        return res.status(500).json({ error: 'Failed saving file', details: diskErr && diskErr.message });
      }
    }
  } catch (err) {
    console.error('Upload error', err && err.message);
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// serve file: try file_data blob first (fast), then DB chunks, then pending disk
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!token) return res.status(400).send('Bad token');

    // try file_data blob across pools
    try {
      const fileData = await fetchFileDataFromPools(token);
      if (fileData && fileData.buf) {
        const buf = fileData.buf;
        const meta = await fetchUploadEntryAcrossPools(token);
        const mime = (meta && meta.data && meta.data.mime) || (mappings[token] && mappings[token].mime) || 'application/octet-stream';
        // support Range header for streaming
        const range = req.headers.range;
        const fileLen = buf.length;
        res.setHeader('Accept-Ranges', 'bytes');
        if (range) {
          const parts = range.replace(/bytes=/, '').split('-');
          const start = parseInt(parts[0], 10) || 0;
          const end = parts[1] ? parseInt(parts[1], 10) : (fileLen - 1);
          if (start >= fileLen || end >= fileLen) {
            res.status(416).set('Content-Range', `bytes */${fileLen}`).end();
            return;
          }
          const chunk = buf.slice(start, end + 1);
          res.status(206);
          res.set({
            'Content-Range': `bytes ${start}-${end}/${fileLen}`,
            'Content-Length': String(chunk.length),
            'Content-Type': mime,
            'Cache-Control': 'public, max-age=31536000, immutable'
          });
          return res.send(chunk);
        } else {
          res.set({
            'Content-Length': String(fileLen),
            'Content-Type': mime,
            'Cache-Control': 'public, max-age=31536000, immutable'
          });
          return res.send(buf);
        }
      }
    } catch (e) {
      console.warn('fetchFileDataFromPools error (non-fatal):', e && e.message);
    }

    // try DB chunks across pools
    try {
      const { rows } = await fetchAllChunksAcrossPools(token);
      if (rows && rows.length) {
        const mime = (mappings[token] && mappings[token].mime) || 'application/octet-stream';
        res.setHeader('Content-Type', mime);
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        // NB: no Range for chunked DB streaming here (would be more complex). Stream whole file.
        for (const r of rows) res.write(r.chunk);
        return res.end();
      }
    } catch (e) {
      console.warn('fetchAllChunksAcrossPools error (non-fatal):', e && e.message);
    }

    // pending disk
    try {
      const jsonFiles = fs.readdirSync(PENDING_DIR).filter(f => f.endsWith('.json'));
      for (const jf of jsonFiles) {
        try {
          const meta = JSON.parse(fs.readFileSync(path.join(PENDING_DIR, jf), 'utf8'));
          if (meta && meta.token === token) {
            const binName = jf.replace(/\.json$/, '');
            const binPath = path.join(PENDING_DIR, binName);
            if (fs.existsSync(binPath)) {
              res.setHeader('Content-Type', (meta.entry && meta.entry.mime) || 'application/octet-stream');
              res.setHeader('Content-Disposition', `inline; filename="${(meta.entry && meta.entry.safeOriginal) || 'file'}"`);
              res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
              return fs.createReadStream(binPath).pipe(res);
            }
          }
        } catch (e) { /* ignore per-file parse errors */ }
      }
    } catch (e) { /* ignore */ }

    return res.status(404).send('Not found');
  } catch (err) {
    console.error('Serve error', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// admin: run migrations across pools
app.post('/_admin/run-migrations', async (req, res) => {
  try {
    await runMigrationsAll();
    return res.json({ ok: true, message: 'migrations run on all pools (best-effort)' });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max: ' + MAX_FILE_SIZE });
  if (err) {
    console.error('Unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

// start pending retry loop (in case startup didn't)
pendingRetryLoop();

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
