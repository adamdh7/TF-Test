// server.js - Store uploads 100% in Postgres (BYTEA), no local file storage
require('dotenv').config();

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');

// Postgres pool (dynamic require)
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
  console.error('No DATABASE_URL configured — this server requires Postgres to store files.');
  pool = null;
}

// in-memory metadata cache (token -> metadata)
// Metadata does NOT contain file bytes.
let mappings = {};

// disk metadata fallback (optional)
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

// DB migrations and helpers
async function runMigrations() {
  if (!pool) {
    console.warn('No DB pool - skipping migrations.');
    return;
  }
  // Create table with file_data BYTEA column
  const sql = `
    CREATE TABLE IF NOT EXISTS uploads (
      token TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      file_data BYTEA,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);
  `;
  try {
    await pool.query(sql);
    console.log('DB migration applied (uploads table ready).');
  } catch (err) {
    console.error('DB migration failed:', err && err.message);
  }
}

// load metadata (NOT file bytes) from DB into in-memory mappings
async function loadMappingsFromDB() {
  if (!pool) {
    console.log('No DB pool - skipping DB load');
    return {};
  }
  try {
    const res = await pool.query('SELECT token, data FROM uploads');
    const dbm = {};
    (res.rows || []).forEach(r => dbm[r.token] = r.data);
    console.log('Loaded mappings from DB:', Object.keys(dbm).length);
    return dbm;
  } catch (err) {
    console.warn('Failed loading mappings from DB:', err && err.message);
    return {};
  }
}

// Save metadata (JSON) to DB (without file bytes)
async function saveMappingMetadataToDB(token, entry) {
  if (!pool) return;
  try {
    const q = `
      INSERT INTO uploads (token, data, created_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (token)
      DO UPDATE SET data = $2::jsonb, created_at = NOW();
    `;
    await pool.query(q, [token, JSON.stringify(entry)]);
  } catch (err) {
    console.error('Failed saving mapping metadata to DB:', err && err.message);
  }
}

// Save file bytes + metadata to DB (used during upload)
async function saveFileToDB(token, entry, buffer) {
  if (!pool) {
    throw new Error('No database pool available');
  }
  try {
    const q = `
      INSERT INTO uploads (token, data, file_data, created_at)
      VALUES ($1, $2::jsonb, $3, NOW())
      ON CONFLICT (token)
      DO UPDATE SET data = $2::jsonb, file_data = $3, created_at = NOW();
    `;
    await pool.query(q, [token, JSON.stringify(entry), buffer]);
  } catch (err) {
    console.error('Failed saving file to DB:', err && err.message);
    throw err;
  }
}

// Fetch file bytes + metadata from DB when serving
async function fetchFileFromDB(token) {
  if (!pool) throw new Error('No database pool available');
  try {
    const r = await pool.query('SELECT data, file_data FROM uploads WHERE token = $1', [token]);
    if (!r.rowCount) return null;
    return { data: r.rows[0].data, file: r.rows[0].file_data };
  } catch (err) {
    console.error('Failed fetching file from DB:', err && err.message);
    throw err;
  }
}

// utility: token generator and safe filename & truncate
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

// multer in-memory
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: 5368709120 } }); // 5GB

// express
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Startup: load disk then DB metadata
loadMappingsFromDisk();
(async () => {
  if (!pool) {
    console.error('Database not configured. This server requires DATABASE_URL to store files in DB.');
    // keep using disk metadata if present, but uploads will fail.
    return;
  }
  await runMigrations();
  const dbm = await loadMappingsFromDB();
  // prefer DB metadata over disk
  mappings = Object.assign({}, mappings, dbm);
  // persist any disk-only metadata into DB (without file bytes) if needed
  for (const [token, entry] of Object.entries(mappings)) {
    if (!dbm[token]) {
      // save metadata only (no file bytes)
      await saveMappingMetadataToDB(token, entry).catch(()=>{});
    }
  }
  // persist merged mapping to disk for debug (optional)
  saveMappingsToDisk();
})();

// Pagination endpoint (metadata-only)
app.get('/uploads', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const offset = Math.max(Number(req.query.offset) || 0, 0);
  if (pool) {
    try {
      const q = `SELECT token, data->>'originalName' as name, data->>'safeOriginal' as safeOriginal, data->>'storage' as storage, created_at
                 FROM uploads
                 ORDER BY created_at DESC
                 LIMIT $1 OFFSET $2`;
      const r = await pool.query(q, [limit, offset]);
      const items = r.rows.map(row => ({
        token: row.token,
        displayName: truncateMiddle(row.name || row.safOriginal || row.safeoriginal || row.safeOriginal || (row.safeOriginal)),
        safeOriginal: row.safeoriginal || row.safeOriginal || row.safeOriginal,
        storage: row.storage || 'db',
        createdAt: row.created_at
      }));
      return res.json({ items });
    } catch (err) {
      console.warn('DB list error', err && err.message);
      return res.status(500).json({ error: 'DB list error' });
    }
  }
  // fallback to in-memory
  const all = Object.values(mappings).sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt));
  const slice = all.slice(offset, offset + limit).map(e => ({
    token: e.token,
    displayName: truncateMiddle(e.originalName || e.safeOriginal, 30),
    safeOriginal: e.safeOriginal,
    storage: e.storage || 'db',
    createdAt: e.createdAt
  }));
  return res.json({ items: slice, total: all.length });
});

// UPLOAD: store file bytes directly in DB (no local file)
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    if (!pool) return res.status(500).json({ error: 'Database not configured. Cannot store files.' });

    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();

    // prepare metadata entry
    const entry = {
      token,
      originalName,
      safeOriginal,
      size: req.file.size,
      mime: req.file.mimetype,
      createdAt: new Date().toISOString(),
      storage: 'db' // stored in DB
    };

    // save to DB (file bytes + metadata)
    await saveFileToDB(token, entry, req.file.buffer);

    // update in-memory mapping
    mappings[token] = entry;
    // persist metadata to disk for debug (optional)
    saveMappingsToDisk();

    // build URL
    const encodedName = encodeURIComponent(safeOriginal);
    const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
    const proto = protoHeader || req.protocol || 'https';
    const host = req.get('host');
    const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
    const sharePath = `/TF-${token}/${encodedName}`;
    const fileUrl = `${origin}${sharePath}`;

    return res.json({ token, url: fileUrl, sharePath, info: entry, displayName: truncateMiddle(originalName, 36) });
  } catch (err) {
    console.error('Upload error', err && err.message);
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// SERVE: fetch bytes from DB and stream/send
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  const token = req.params.token;
  // check in-memory mapping first for quick metadata; but fetch file bytes from DB
  try {
    if (!pool) return res.status(500).send('Database not configured');

    const fetched = await fetchFileFromDB(token);
    if (!fetched) return res.status(404).send('Not found');
    const entry = fetched.data || mappings[token];
    const fileBuf = fetched.file;

    if (!fileBuf) {
      console.warn('No file bytes stored in DB for token', token);
      return res.status(410).send('File removed or missing');
    }

    // headers
    const mime = (entry && entry.mime) || 'application/octet-stream';
    const suggestedName = (entry && (entry.originalName || entry.safeOriginal)) || `file-${token}`;
    res.setHeader('Content-Type', mime);
    res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
    // cache static uploads long-term (optional)
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');

    // send buffer
    return res.send(fileBuf);
  } catch (err) {
    console.error('Error serving file', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// Admin debug: inspect mapping
app.get('/_admin/token/:token', async (req, res) => {
  const token = req.params.token;
  // prefer metadata from mappings; also show whether DB has bytes
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

// health
app.get('/health', (req, res) => res.json({ ok: true }));

// error handler
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  if (err) {
    console.error(err);
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

// start
app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
