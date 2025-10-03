// server.js - multer-based /upload (fallback to disk if DB fails)
// NOTE: This is a replacement that avoids Busboy constructor issues.
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

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 5 * 1024 * 1024); // 5MB
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB (multer limit - BE CAREFUL)
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PENDING_RETRY_INTERVAL = Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000;

try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch(e){}

if (!process.env.DATABASE_URL) {
  console.error('No DATABASE_URL set in environment. Exiting.');
  process.exit(1);
}
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: PG_POOL_MAX,
  ...(process.env.DATABASE_SSL === 'true' ? { ssl: { rejectUnauthorized: false } } : {})
});
pool.on('error', (err) => console.error('Unexpected PG client error', err && err.message));

// mappings disk load/save
let mappings = {};
function loadMappingsFromDisk() {
  try {
    if (fs.existsSync(UPLOAD_JSON)) {
      mappings = JSON.parse(fs.readFileSync(UPLOAD_JSON, 'utf8') || '{}');
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

// migrations
async function runMigrations() {
  try {
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
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_chunks_token ON file_chunks(token);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);`);
    console.log('DB migrations applied (uploads + file_chunks).');
  } catch (err) {
    console.error('DB migration failed:', err && err.message);
    throw err;
  }
}

// DB helpers
async function saveChunkToDB(token, seq, buffer) {
  const q = `INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)`;
  await pool.query(q, [token, seq, buffer]);
}
async function saveMappingMetadataToDB(token, entry) {
  const q = `INSERT INTO uploads (token, data, created_at) VALUES ($1, $2::jsonb, NOW())
             ON CONFLICT (token) DO UPDATE SET data = $2::jsonb, created_at = NOW();`;
  await pool.query(q, [token, JSON.stringify(entry)]);
}
async function fetchAllChunks(token) {
  const r = await pool.query('SELECT seq, chunk FROM file_chunks WHERE token=$1 ORDER BY seq ASC', [token]);
  return r.rows || [];
}
async function fetchUploadEntry(token) {
  const r = await pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token=$1', [token]);
  if (!r.rowCount) return null;
  return { data: r.rows[0].data, hasFile: r.rows[0].has_file };
}

// pending disk helpers
function saveBufferToPending(token, entry, buffer) {
  const fn = `pending-${token}-${Date.now()}.bin`;
  const filePath = path.join(PENDING_DIR, fn);
  fs.writeFileSync(filePath, buffer);
  fs.writeFileSync(path.join(PENDING_DIR, fn + '.json'), JSON.stringify({ token, entry, filename: fn, timestamp: Date.now() }));
  console.log('Saved pending file to disk for token', token, filePath);
  return filePath;
}

async function attemptFlushPendingOne(fileBaseName) {
  try {
    const jsonPath = path.join(PENDING_DIR, fileBaseName + '.json');
    const binPath = path.join(PENDING_DIR, fileBaseName);
    if (!fs.existsSync(jsonPath) || !fs.existsSync(binPath)) return false;
    const meta = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    const buffer = fs.readFileSync(binPath);
    let seq = 0;
    for (let offset = 0; offset < buffer.length; offset += CHUNK_MAX_SIZE) {
      const piece = buffer.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buffer.length));
      await saveChunkToDB(meta.token, seq, piece);
      seq++;
    }
    try { await saveMappingMetadataToDB(meta.token, meta.entry); } catch (e) {}
    fs.unlinkSync(jsonPath); fs.unlinkSync(binPath);
    console.log('Pending flushed to DB for token', meta.token);
    return true;
  } catch (err) {
    console.warn('Pending flush failed for', fileBaseName, err && err.message);
    return false;
  }
}
async function pendingRetryLoop() {
  try {
    const files = fs.readdirSync(PENDING_DIR).filter(n => !n.endsWith('.json'));
    for (const bin of files) {
      await attemptFlushPendingOne(bin);
    }
  } catch (err) {
    console.warn('pendingRetryLoop error', err && err.message);
  } finally {
    setTimeout(pendingRetryLoop, PENDING_RETRY_INTERVAL);
  }
}

// startup
(async () => {
  try {
    if (RUN_MIGRATIONS_AUTOMATIC) await runMigrations();
    // load metadata
    try {
      const r = await pool.query('SELECT token, data, created_at FROM uploads');
      (r.rows || []).forEach(row => {
        mappings[row.token] = row.data;
        if (!mappings[row.token].createdAt) mappings[row.token].createdAt = row.created_at;
      });
      saveMappingsToDisk();
      console.log('Loaded mappings from DB:', Object.keys(mappings).length);
    } catch (e) {
      console.warn('Could not load mappings from DB (ok if empty):', e && e.message);
    }
    pendingRetryLoop();
  } catch (err) {
    console.error('Startup error:', err && err.message);
  }
})();

const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------- MULTER setup ----------
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: MAX_FILE_SIZE } });

// upload route (uses multer to parse multipart/form-data)
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    // create token & metadata
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
      storage: 'db' // may be changed if fallback
    };

    // slice buffer into chunks and save to DB
    const buf = req.file.buffer;
    try {
      let seq = 0;
      for (let offset = 0; offset < buf.length; offset += CHUNK_MAX_SIZE) {
        const piece = buf.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buf.length));
        await saveChunkToDB(token, seq, piece);
        seq++;
      }
      // save metadata to uploads table
      try { await saveMappingMetadataToDB(token, entry); } catch(e){ console.warn('saveMappingMetadataToDB failed', e && e.message); }
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
      console.error('Save to DB failed:', dbErr && dbErr.message);
      // fallback: save full buffer to pending disk
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

// serve file: try DB chunks then pending
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!token) return res.status(400).send('Bad token');

    // try uploads table file_data
    try {
      const ent = await fetchUploadEntry(token);
      if (ent && ent.hasFile && ent.data) {
        const r = await pool.query('SELECT file_data FROM uploads WHERE token=$1', [token]);
        if (r.rowCount && r.rows[0].file_data) {
          const buf = r.rows[0].file_data;
          res.setHeader('Content-Type', (ent.data && ent.data.mime) || 'application/octet-stream');
          res.setHeader('Content-Length', String(buf.length));
          res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
          return res.send(buf);
        }
      }
    } catch (e) { console.warn('fetchUploadEntry error (non-fatal):', e && e.message); }

    // try DB chunks
    try {
      const rows = await fetchAllChunks(token);
      if (rows && rows.length) {
        res.setHeader('Content-Type', (mappings[token] && mappings[token].mime) || 'application/octet-stream');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        for (const r of rows) res.write(r.chunk);
        return res.end();
      }
    } catch (e) { console.warn('fetchAllChunks error (non-fatal):', e && e.message); }

    // try pending disk
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
        } catch(e){}
      }
    } catch(e){}

    return res.status(404).send('Not found');
  } catch (err) {
    console.error('Serve error', err && err.message);
    return res.status(500).send('Serve error');
  }
});

app.post('/_admin/run-migrations', async (req, res) => {
  try {
    await runMigrations();
    return res.json({ ok: true, message: 'migrations run' });
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

// start pending retry loop
pendingRetryLoop();

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
