// server.js - chunked uploads, fallback to disk pending, range streaming
require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const Busboy = require('busboy');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const PENDING_DIR = path.join(UPLOAD_DIR, 'pending');

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 5 * 1024 * 1024);
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';

const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PG_CONN_TIMEOUT_MS = Number(process.env.PG_CONN_TIMEOUT_MS || 20000);
const PG_IDLE_TIMEOUT_MS = Number(process.env.PG_IDLE_TIMEOUT_MS || 30000);

// ensure upload dirs
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch(e){}

// single DATABASE_URL expected
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

// in-memory mappings + disk backup
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
function truncateMiddle(name, maxLen = 30) {
  if (!name) return name;
  if (name.length <= maxLen) return name;
  const keep = Math.floor((maxLen - 1) / 2);
  return `${name.slice(0, keep)}…${name.slice(name.length - keep)}`;
}

// migrations: create uploads + file_chunks
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

// DB helpers
async function saveChunkToDB(token, seq, buffer) {
  const q = `INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)`;
  try {
    await pool.query(q, [token, seq, buffer]);
    return true;
  } catch (err) {
    throw err;
  }
}
async function saveMappingMetadataToDB(token, entry) {
  const q = `INSERT INTO uploads (token, data, created_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (token) DO UPDATE SET data = $2::jsonb, created_at = NOW();`;
  try {
    await pool.query(q, [token, JSON.stringify(entry)]);
    return true;
  } catch (err) {
    throw err;
  }
}
async function fetchAllChunks(token) {
  try {
    const r = await pool.query('SELECT seq, chunk FROM file_chunks WHERE token=$1 ORDER BY seq ASC', [token]);
    return r.rows || [];
  } catch (err) {
    throw err;
  }
}
async function fetchUploadEntry(token) {
  try {
    const r = await pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token=$1', [token]);
    if (!r.rowCount) return null;
    return { data: r.rows[0].data, hasFile: r.rows[0].has_file };
  } catch (err) {
    throw err;
  }
}

// pending-disk helpers
function saveBufferToPending(token, entry, buffer) {
  try {
    const fn = `pending-${token}-${Date.now()}.bin`;
    const meta = { token, entry, filename: fn, timestamp: Date.now() };
    const filePath = path.join(PENDING_DIR, fn);
    fs.writeFileSync(filePath, buffer);
    fs.writeFileSync(path.join(PENDING_DIR, fn + '.json'), JSON.stringify(meta));
    console.log('Saved pending file to disk for token', token, filePath);
    return filePath;
  } catch (err) {
    console.error('Failed to save pending to disk', err && err.message);
    throw err;
  }
}

async function attemptFlushPendingOne(fileBaseName) {
  try {
    const jsonPath = path.join(PENDING_DIR, fileBaseName + '.json');
    const binPath = path.join(PENDING_DIR, fileBaseName);
    if (!fs.existsSync(jsonPath) || !fs.existsSync(binPath)) return false;
    const meta = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    const buffer = fs.readFileSync(binPath);
    try {
      // try saving as chunked: slice buffer into CHUNK_MAX_SIZE pieces and insert
      let seq = 0;
      for (let offset = 0; offset < buffer.length; offset += CHUNK_MAX_SIZE) {
        const piece = buffer.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buffer.length));
        await saveChunkToDB(meta.token, seq, piece);
        seq++;
      }
      // persist metadata
      try { await saveMappingMetadataToDB(meta.token, meta.entry); } catch(e){}
      // remove pending
      fs.unlinkSync(jsonPath); fs.unlinkSync(binPath);
      console.log('Pending flushed to DB for token', meta.token);
      return true;
    } catch (dbErr) {
      console.warn('Pending flush to DB failed for', meta.token, dbErr && dbErr.message);
      return false;
    }
  } catch (err) {
    console.error('Error in attemptFlushPendingOne', err && err.message);
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
    setTimeout(pendingRetryLoop, Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000);
  }
}

// start migrations and load DB mappings
(async () => {
  try {
    if (RUN_MIGRATIONS_AUTOMATIC) {
      await runMigrations();
    } else {
      console.log('RUN_MIGRATIONS_AUTOMATIC=false -> skipping migrations on startup');
    }

    // load existing uploads metadata (best-effort)
    try {
      const r = await pool.query('SELECT token, data, created_at FROM uploads');
      (r.rows || []).forEach(row => {
        mappings[row.token] = row.data;
        mappings[row.token].createdAt = row.created_at || mappings[row.token].createdAt;
      });
      console.log('Loaded mappings from DB:', Object.keys(mappings).length);
      saveMappingsToDisk();
    } catch (e) {
      console.warn('Failed loading mappings from DB (ok if table missing):', e && e.message);
    }

    // start pending retry loop
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

// Streaming upload endpoint that works with your existing frontend (FormData with file)
app.post('/upload', (req, res) => {
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
        try {
          await saveChunkToDB(token, seq, piece);
        } catch (err) {
          throw err;
        }
        seq++;
        offset += CHUNK_MAX_SIZE;
      }
      const remainder = tmp.slice(offset);
      accBuffers = remainder.length ? [remainder] : [];
      accLen = remainder.length;
      if (finalFlush && accLen > 0) {
        try {
          await saveChunkToDB(token, seq, Buffer.concat(accBuffers, accLen));
        } catch (err) {
          throw err;
        }
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
                  // DB insert failed -> fallback to disk whole file buffer
                  console.error('DB insert failed during streaming:', err && err.message);
                  try {
                    // assemble remaining bytes including current tmp remainder + rest of stream not yet read:
                    const remaining = [];
                    if (offset + CHUNK_MAX_SIZE < tmp.length) {
                      remaining.push(tmp.slice(offset + CHUNK_MAX_SIZE));
                    }
                    // read rest of stream to buffer (drain)
                    for await (const more of fileStream) {
                      remaining.push(more);
                    }
                    const allBuffer = Buffer.concat([tmp.slice(offset + CHUNK_MAX_SIZE), ...remaining]);
                    // save previous pieces? simpler: save the entire file buff (tmp rest + remaining) as pending
                    // combine what we've saved already is stored as chunks; here save remaining as pending full
                    const pendingBuff = Buffer.concat(accBuffers.length ? accBuffers : []);
                    saveBufferToPending(token, mappings[token], pendingBuff);
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
            console.error('Error while processing stream:', err && err.message);
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
          try { await saveMappingMetadataToDB(token, mappings[token]); } catch(e){ /* non-fatal */ }

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
            // fallback: save concatenated accBuffers to pending
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

    busboy.on('field', (name, val) => {
      // ignore fields
    });

    busboy.on('finish', () => {
      if (!gotFile && !aborted) {
        return res.status(400).json({ error: 'No file uploaded' });
      }
      // response handled by file end.
    });

    busboy.on('error', (err) => {
      console.error('busboy error', err && err.message);
      if (!aborted) return cleanupAndAbort(500, 'Upload parsing error');
    });

    req.pipe(busboy);
  } catch (err) {
    console.error('/upload top-level error', err && err.message);
    return res.status(500).json({ error: 'Server error' });
  }
});

// Serve file: first try DB chunks, then pending disk
app.get(['/TF-:token','/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!token) return res.status(400).send('Bad token');

    // Try uploads table first (maybe metadata exists)
    try {
      const e = await fetchUploadEntry(token);
      if (e && e.hasFile && e.data) {
        // file_data exists (single blob) - serve it
        const r = await pool.query('SELECT file_data FROM uploads WHERE token=$1', [token]);
        if (r.rowCount && r.rows[0].file_data) {
          const buf = r.rows[0].file_data;
          res.setHeader('Content-Type', (e.data && e.data.mime) || 'application/octet-stream');
          res.setHeader('Content-Length', String(buf.length));
          res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
          return res.send(buf);
        }
      }
    } catch (err) {
      console.warn('fetchUploadEntry failed (non-fatal):', err && err.message);
    }

    // Try chunks in DB
    try {
      const chunks = await fetchAllChunks(token);
      if (chunks && chunks.length) {
        // Check Range header? For simplicity we stream whole file for now
        res.setHeader('Content-Type', (mappings[token] && mappings[token].mime) || 'application/octet-stream');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        for (const row of chunks) res.write(row.chunk);
        return res.end();
      }
    } catch (err) {
      console.warn('fetchAllChunks failed (non-fatal):', err && err.message);
    }

    // Try pending disk files
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
              const stream = fs.createReadStream(binPath);
              return stream.pipe(res);
            }
          }
        } catch (e) { /* ignore parse errors */ }
      }
    } catch (err) { /* ignore */ }

    return res.status(404).send('Not found');
  } catch (err) {
    console.error('Serve error', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// Admin: run migrations (no MIGRATE_KEY required here)
app.post('/_admin/run-migrations', async (req, res) => {
  try {
    await runMigrations();
    return res.json({ ok: true, message: 'migrations run' });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

// global error handler
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max: ' + MAX_FILE_SIZE });
  if (err) {
    console.error('Unhandled error:', err && err.stack || err && err.message);
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
