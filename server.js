// server.js - corrected, robust Busboy import, chunked uploads, pending-disk fallback
require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const { Pool } = require('pg');

// ---- robust Busboy import (handles CJS/ESM shapes) ----
let Busboy;
try {
  const maybe = require('busboy');
  Busboy = (maybe && (maybe.Busboy || maybe.default)) || maybe;
  if (typeof Busboy !== 'function') {
    console.error('Busboy import unexpected shape:', Object.keys(maybe || {}), '->', Busboy);
    Busboy = null;
  }
} catch (err) {
  console.error('Failed to require busboy module:', err && err.message);
  Busboy = null;
}

// ---- config from .env ----
const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const PENDING_DIR = path.join(UPLOAD_DIR, 'pending');

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 5 * 1024 * 1024); // 5MB
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';

const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PG_CONN_TIMEOUT_MS = Number(process.env.PG_CONN_TIMEOUT_MS || 20000);
const PG_IDLE_TIMEOUT_MS = Number(process.env.PG_IDLE_TIMEOUT_MS || 30000);
const PENDING_RETRY_INTERVAL = Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000; // 30s

// ensure directories
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch (e) {}

// ---- Postgres pool (single DATABASE_URL expected) ----
if (!process.env.DATABASE_URL) {
  console.error('No DATABASE_URL set in environment. Exiting.');
  process.exit(1);
}
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: PG_POOL_MAX,
  idleTimeoutMillis: PG_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: PG_CONN_TIMEOUT_MS,
  ...(process.env.DATABASE_SSL === 'true' ? { ssl: { rejectUnauthorized: false } } : {})
});
pool.on('error', (err) => console.error('Unexpected PG client error', err && err.message));

// in-memory metadata and disk fallback
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

// helpers
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

// ---- DB migrations (uploads + file_chunks) ----
async function runMigrations() {
  try {
    const createUploads = `
      CREATE TABLE IF NOT EXISTS uploads (
        token TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        file_data BYTEA,
        created_at TIMESTAMPTZ DEFAULT now()
      );
    `;
    const createChunks = `
      CREATE TABLE IF NOT EXISTS file_chunks (
        token TEXT NOT NULL,
        seq INTEGER NOT NULL,
        chunk BYTEA NOT NULL,
        PRIMARY KEY (token, seq)
      );
    `;
    await pool.query(createUploads);
    await pool.query(`ALTER TABLE uploads ADD COLUMN IF NOT EXISTS file_data BYTEA;`);
    await pool.query(createChunks);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_chunks_token ON file_chunks(token);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);`);
    console.log('DB migrations applied (uploads + file_chunks).');
  } catch (err) {
    console.error('DB migration failed:', err && err.message);
    throw err;
  }
}

// ---- DB helpers ----
async function saveChunkToDB(token, seq, buffer) {
  const q = `INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)`;
  await pool.query(q, [token, seq, buffer]);
}
async function saveMappingMetadataToDB(token, entry) {
  const q = `INSERT INTO uploads (token, data, created_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (token) DO UPDATE SET data = $2::jsonb, created_at = NOW();`;
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

// ---- pending-disk helpers ----
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
    // slice buffer into CHUNK_MAX_SIZE pieces
    let seq = 0;
    for (let offset = 0; offset < buffer.length; offset += CHUNK_MAX_SIZE) {
      const piece = buffer.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buffer.length));
      await saveChunkToDB(meta.token, seq, piece);
      seq++;
    }
    try { await saveMappingMetadataToDB(meta.token, meta.entry); } catch (e) { /* non-fatal */ }
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

// ---- startup: migrations + load mappings ----
(async () => {
  try {
    if (RUN_MIGRATIONS_AUTOMATIC) await runMigrations();
    // attempt to load metadata from uploads table
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
    // start pending retry loop
    pendingRetryLoop();
  } catch (err) {
    console.error('Startup error:', err && err.message);
  }
})();

// ---- express app ----
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---- /upload endpoint (streaming using Busboy) ----
app.post('/upload', (req, res) => {
  if (!Busboy) {
    console.error('Busboy not available - cannot parse multipart/form-data');
    return res.status(500).json({ error: 'Server missing required module busboy' });
  }

  try {
    const busboy = new Busboy({ headers: req.headers, limits: { fileSize: MAX_FILE_SIZE } });

    const token = genToken(10);
    mappings[token] = { token, originalName: null, safeOriginal: null, size: 0, mime: null, createdAt: new Date().toISOString(), storage: 'chunks_pending' };
    saveMappingsToDisk();

    let gotFile = false;
    let seq = 0;
    let accBuffers = [];
    let accLen = 0;
    let totalBytes = 0;
    let originalName = null;
    let mimeType = null;
    let aborted = false;

    function cleanupAndAbort(code = 500, msg = 'Upload aborted') {
      aborted = true;
      try { req.unpipe && req.unpipe(busboy); } catch (e) {}
      try { busboy.removeAllListeners(); } catch (e) {}
      return res.status(code).json({ error: msg });
    }

    async function flushAccumulator(finalFlush = false) {
      if (accLen === 0) return;
      const tmp = Buffer.concat(accBuffers, accLen);
      let offset = 0;
      while (tmp.length - offset >= CHUNK_MAX_SIZE) {
        const piece = tmp.slice(offset, offset + CHUNK_MAX_SIZE);
        await saveChunkToDB(token, seq, piece);
        seq++;
        offset += CHUNK_MAX_SIZE;
      }
      const remainder = tmp.slice(offset);
      accBuffers = remainder.length ? [remainder] : [];
      accLen = remainder.length;
      if (finalFlush && accLen > 0) {
        await saveChunkToDB(token, seq, Buffer.concat(accBuffers, accLen));
        seq++;
        accBuffers = [];
        accLen = 0;
      }
    }

    busboy.on('file', (fieldname, fileStream, filename, encoding, mimetype) => {
      gotFile = true;
      originalName = filename || 'file';
      mimeType = mimetype || 'application/octet-stream';
      mappings[token].originalName = originalName;
      mappings[token].safeOriginal = safeFileName(originalName);
      mappings[token].mime = mimeType;
      saveMappingsToDisk();

      fileStream.on('data', (chunk) => {
        fileStream.pause();
        (async () => {
          try {
            accBuffers.push(chunk);
            accLen += chunk.length;
            totalBytes += chunk.length;

            if (accLen >= CHUNK_MAX_SIZE) {
              const tmp = Buffer.concat(accBuffers, accLen);
              let offset = 0;
              while (tmp.length - offset >= CHUNK_MAX_SIZE) {
                const piece = tmp.slice(offset, offset + CHUNK_MAX_SIZE);
                try {
                  await saveChunkToDB(token, seq, piece);
                } catch (err) {
                  // DB error while inserting chunk -> fallback to saving remaining whole file to pending disk
                  console.error('DB insert failed during streaming:', err && err.message);
                  try {
                    const remainder = tmp.slice(offset); // remainder not yet flushed
                    // read rest of fileStream into buffer
                    const restParts = [];
                    for await (const more of fileStream) restParts.push(more);
                    const allRemaining = Buffer.concat([remainder, ...restParts]);
                    saveBufferToPending(token, mappings[token], Buffer.concat(accBuffers.length ? accBuffers : [], accLen));
                    mappings[token].size = totalBytes;
                    mappings[token].storage = 'pending_disk';
                    saveMappingsToDisk();
                    return cleanupAndAbort(200, 'Saved to disk pending due to DB error');
                  } catch (diskErr) {
                    console.error('Disk fallback during stream failed:', diskErr && diskErr.message);
                    return cleanupAndAbort(500, 'Server error saving pending');
                  }
                }
                seq++;
                offset += CHUNK_MAX_SIZE;
              }
              const remainder = tmp.slice(offset);
              accBuffers = remainder.length ? [remainder] : [];
              accLen = remainder.length;
            }
            fileStream.resume();
          } catch (err) {
            console.error('Error while processing chunk stream:', err && err.message);
            return cleanupAndAbort(500, 'Stream processing error');
          }
        })();
      });

      fileStream.on('end', async () => {
        try {
          await flushAccumulator(true);
          mappings[token].size = totalBytes;
          mappings[token].storage = 'chunks';
          mappings[token].createdAt = new Date().toISOString();
          saveMappingsToDisk();
          try { await saveMappingMetadataToDB(token, mappings[token]); } catch (e) { /* non-fatal */ }

          const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
          const proto = protoHeader || req.protocol || 'https';
          const host = req.get('host');
          const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
          const sharePath = `/TF-${token}/${encodeURIComponent(mappings[token].safeOriginal)}`;
          const fileUrl = `${origin}${sharePath}`;

          return res.json({ token, url: fileUrl, sharePath, info: mappings[token] });
        } catch (err) {
          console.error('Error finalizing upload:', err && err.message);
          try {
            const pending = accLen ? Buffer.concat(accBuffers, accLen) : Buffer.alloc(0);
            saveBufferToPending(token, mappings[token], pending);
            mappings[token].size = totalBytes;
            mappings[token].storage = 'pending_disk';
            saveMappingsToDisk();
            const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
            const proto = protoHeader || req.protocol || 'https';
            const host = req.get('host');
            const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
            const sharePath = `/TF-${token}/${encodeURIComponent(mappings[token].safeOriginal)}`;
            const fileUrl = `${origin}${sharePath}`;
            return res.json({ token, url: fileUrl, sharePath, info: mappings[token], note: 'saved-locally-pending-db' });
          } catch (diskErr) {
            console.error('Disk fallback failed at finalize:', diskErr && diskErr.message);
            return cleanupAndAbort(500, 'Failed saving file');
          }
        }
      });

      fileStream.on('error', (err) => {
        console.error('fileStream error:', err && err.message);
        return cleanupAndAbort(500, 'Stream error');
      });
    });

    busboy.on('field', () => { /* ignore */ });

    busboy.on('finish', () => {
      if (!gotFile && !aborted) {
        return res.status(400).json({ error: 'No file uploaded' });
      }
      // response handled already when file 'end' emitted
    });

    busboy.on('error', (err) => {
      console.error('busboy error', err && err.message);
      if (!aborted) return res.status(500).json({ error: 'Upload parsing error' });
    });

    req.pipe(busboy);
  } catch (err) {
    console.error('/upload top-level error', err && err.message);
    return res.status(500).json({ error: 'Server error' });
  }
});

// ---- Serve file: try DB chunks, then pending disk ----
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!token) return res.status(400).send('Bad token');

    // try uploads table first for file_data
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
    } catch (e) {
      console.warn('fetchUploadEntry error (non-fatal):', e && e.message);
    }

    // try DB chunks
    try {
      const chunks = await fetchAllChunks(token);
      if (chunks && chunks.length) {
        res.setHeader('Content-Type', (mappings[token] && mappings[token].mime) || 'application/octet-stream');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        for (const row of chunks) res.write(row.chunk);
        return res.end();
      }
    } catch (e) {
      console.warn('fetchAllChunks error (non-fatal):', e && e.message);
    }

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
        } catch (e) { /* ignore per-file parse errors */ }
      }
    } catch (e) { /* ignore overall pending dir errors */ }

    return res.status(404).send('Not found');
  } catch (err) {
    console.error('Serve error', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// ---- admin migrations endpoint (no key) ----
app.post('/_admin/run-migrations', async (req, res) => {
  try {
    await runMigrations();
    return res.json({ ok: true, message: 'migrations run' });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

// global error
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max: ' + MAX_FILE_SIZE });
  if (err) {
    console.error('Unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

// start server
app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
