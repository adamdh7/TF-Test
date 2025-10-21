// server.js - backend only - DB-first chunked uploads, terminal progress, Range serving
require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const { Pool } = require('pg');
const Busboy = require('busboy');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const PENDING_DIR = path.join(UPLOAD_DIR, 'pending');

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 8 * 1024 * 1024); // 8MB
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PENDING_RETRY_INTERVAL = Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000; // ms
const ENABLE_PENDING_FALLBACK = (process.env.ENABLE_PENDING_FALLBACK === 'true');

// ensure dirs (uploads/pending) exist (safe even on ephemeral deploys)
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch(e){}

// -------------------- DB pools --------------------
let poolInfos = [];
function isLikelyConnectionString(s) {
  if (!s || typeof s !== 'string') return false;
  const t = s.trim();
  if (!t) return false;
  if (t.startsWith('postgres://') || t.startsWith('postgresql://')) return true;
  if (t.includes('@') && t.includes('/')) return true;
  return false;
}
function createPoolsFromEnv() {
  const keys = Object.keys(process.env).filter(k => /^DATABASE_URL(?:\d*)$/.test(k));
  keys.sort((a,b)=> {
    const gn = k => (k === 'DATABASE_URL' ? 0 : parseInt(k.replace('DATABASE_URL',''),10) || 0);
    return gn(a) - gn(b);
  });
  for (const key of keys) {
    const raw = process.env[key];
    if (!raw || !raw.trim()) continue;
    const conn = raw.trim();
    if (!isLikelyConnectionString(conn)) continue;
    const suffix = key === 'DATABASE_URL' ? '' : key.replace('DATABASE_URL','');
    const sslVal = process.env[`DATABASE_SSL${suffix}`] || process.env['DATABASE_SSL'];
    const cfg = { connectionString: conn, max: PG_POOL_MAX };
    if (sslVal === 'true' || sslVal === '1') cfg.ssl = { rejectUnauthorized: false };
    try {
      const pool = new Pool(cfg);
      pool.on('error', (err)=> console.error(`Unexpected PG client error (${key}):`, err && err.message));
      poolInfos.push({ name: key, pool, connString: conn });
      console.log(`Postgres pool created for ${key}`);
    } catch (err) {
      console.error(`Failed to create pool for ${key}:`, err && err.message);
    }
  }
  if (!poolInfos.length) console.warn('No DB pools created - server will reject uploads unless fallback enabled.');
}
createPoolsFromEnv();

// -------------------- in-memory mappings (no upload.json) --------------------
let mappings = {}; // token -> metadata (cached from DB on startup)
async function loadMappingsFromDB() {
  if (!poolInfos.length) return;
  const dbm = {};
  for (const pinfo of poolInfos) {
    try {
      const res = await pinfo.pool.query('SELECT token, data, created_at FROM uploads');
      (res.rows || []).forEach(r => {
        const existing = dbm[r.token];
        if (!existing) { dbm[r.token] = r.data; dbm[r.token].createdAt = r.created_at; }
        else {
          const existingDate = new Date(existing.createdAt || 0).getTime();
          const newDate = new Date(r.created_at || 0).getTime();
          if (newDate >= existingDate) { dbm[r.token] = r.data; dbm[r.token].createdAt = r.created_at; }
        }
      });
    } catch (err) {
      console.warn('Failed loading mappings from', pinfo.name, err && err.message);
    }
  }
  mappings = Object.assign({}, dbm, mappings);
}

// -------------------- helpers --------------------
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i=0;i<len;i++) t += chars[Math.floor(Math.random()*chars.length)];
  return t;
}
function safeFileName(name) {
  const ext = path.extname(name || '');
  const base = path.basename(name || '', ext);
  const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0,120);
  const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
  return (safeBase + safeExt) || 'file';
}
function humanBytes(n) {
  if (!n && n !== 0) return '0B';
  if (n === 0) return '0B';
  const units = ['B','KB','MB','GB','TB'];
  let i = 0;
  let v = n;
  while(v >= 1024 && i < units.length-1){ v /= 1024; i++; }
  return `${Math.round(v*10)/10}${units[i]}`;
}
function humanTimeFromSeconds(s) {
  if (typeof s !== 'number' || !isFinite(s)) return null;
  const hrs = Math.floor(s / 3600);
  const mins = Math.floor((s % 3600) / 60);
  const secs = Math.floor(s % 60);
  if (hrs > 0) return `${hrs}h${mins}m${secs}s`;
  if (mins > 0) return `${mins}m${secs}s`;
  return `${secs}s`;
}

// -------------------- migrations --------------------
async function runMigrationsOnPool(pinfo) {
  if (!pinfo || !pinfo.pool) return;
  const client = await pinfo.pool.connect().catch(e => { throw new Error(`connect-failed: ${e && e.message}`); });
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS uploads (
        token TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        file_data BYTEA,
        created_at TIMESTAMPTZ DEFAULT now()
      );
    `);
    await client.query(`ALTER TABLE uploads ADD COLUMN IF NOT EXISTS file_data BYTEA;`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS file_chunks (
        token TEXT NOT NULL,
        seq INTEGER NOT NULL,
        chunk BYTEA NOT NULL,
        PRIMARY KEY (token, seq)
      );
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_file_chunks_token ON file_chunks(token);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_uploads_created_at ON uploads(created_at);`);
    console.log(`DB migration applied for ${pinfo.name}`);
  } finally {
    client.release();
  }
}
async function runMigrationsAll() {
  for (const pinfo of poolInfos) {
    try { await runMigrationsOnPool(pinfo); } catch (e) { console.warn('Continuing despite migration error on', pinfo.name, e && e.message); }
  }
}

// -------------------- DB helpers --------------------
async function saveChunkToPool(pinfo, token, seq, buffer) {
  try {
    await pinfo.pool.query('INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)', [token, seq, buffer]);
    return true;
  } catch (err) {
    console.warn(`chunk insert failed on ${pinfo.name}:`, err && err.message);
    return false;
  }
}
async function saveMappingMetadataToDBAcrossPools(token, entry) {
  if (!poolInfos.length) return null;
  let lastErr = null;
  for (const pinfo of poolInfos) {
    try {
      await pinfo.pool.query(
        `INSERT INTO uploads (token, data, created_at) VALUES ($1,$2::jsonb,NOW())
         ON CONFLICT (token) DO UPDATE SET data = $2::jsonb, created_at = NOW();`,
        [token, JSON.stringify(entry)]
      );
      return pinfo.name;
    } catch (err) {
      lastErr = err;
      console.warn(`Save metadata to ${pinfo.name} failed:`, err && err.message);
      continue;
    }
  }
  throw lastErr || new Error('All DB pools failed to save metadata');
}
async function fetchAllChunksAcrossPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT seq, chunk FROM file_chunks WHERE token=$1 ORDER BY seq ASC', [token]);
      if (r.rowCount) return { rows: r.rows, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchAllChunks failed on ${pinfo.name}:`, err && err.message);
    }
  }
  return { rows: [] };
}
async function fetchUploadEntryAcrossPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT data, (file_data IS NOT NULL) AS has_file FROM uploads WHERE token=$1', [token]);
      if (r.rowCount) return { data: r.rows[0].data, hasFile: r.rows[0].has_file, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchUploadEntry failed on ${pinfo.name}:`, err && err.message);
    }
  }
  return null;
}
async function fetchFileDataFromPools(token) {
  for (const pinfo of poolInfos) {
    try {
      const r = await pinfo.pool.query('SELECT file_data FROM uploads WHERE token=$1', [token]);
      if (r.rowCount && r.rows[0].file_data) return { buf: r.rows[0].file_data, pool: pinfo.name };
    } catch (err) {
      console.warn(`fetchFileData failed on ${pinfo.name}:`, err && err.message);
    }
  }
  return null;
}

// -------------------- pending disk helpers (optional fallback) --------------------
function saveStreamToPendingFileSync(token) {
  const fn = `pending-${token}-${Date.now()}.bin`;
  const filePath = path.join(PENDING_DIR, fn);
  const ws = fs.createWriteStream(filePath);
  return { path: filePath, ws, filename: fn };
}
function savePendingMetaFileSync(token, entry, pendingBinName) {
  try {
    fs.writeFileSync(path.join(PENDING_DIR, pendingBinName + '.json'), JSON.stringify({ token, entry, filename: pendingBinName, timestamp: Date.now() }));
  } catch(e){ console.warn('savePendingMetaFileSync failed', e && e.message); }
}
async function attemptFlushPendingOneToPools(fileBaseName) {
  try {
    const jsonPath = path.join(PENDING_DIR, fileBaseName + '.json');
    const binPath = path.join(PENDING_DIR, fileBaseName);
    if (!fs.existsSync(jsonPath) || !fs.existsSync(binPath)) return false;
    const meta = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    const stream = fs.createReadStream(binPath, { highWaterMark: CHUNK_MAX_SIZE });
    if (!poolInfos.length) return false;
    for (const pinfo of poolInfos) {
      try {
        let seq = 0;
        for await (const chunk of stream) {
          await pinfo.pool.query('INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)', [meta.token, seq, chunk]);
          seq++;
        }
        try { await saveMappingMetadataToDBAcrossPools(meta.token, meta.entry); } catch(e){}
        fs.unlinkSync(jsonPath); fs.unlinkSync(binPath);
        console.log('Flushed pending to DB for token', meta.token);
        return true;
      } catch (err) {
        console.warn('Pending flush to DB failed for', fileBaseName, err && err.message);
        stream.destroy();
      }
    }
    return false;
  } catch (err) {
    console.error('attemptFlushPendingOneToPools error', err && err.message);
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

// -------------------- startup --------------------
(async () => {
  try {
    if (poolInfos.length && RUN_MIGRATIONS_AUTOMATIC) await runMigrationsAll();
    await loadMappingsFromDB();
    if (ENABLE_PENDING_FALLBACK) pendingRetryLoop();
    console.log('Startup complete. mappings loaded:', Object.keys(mappings).length);
  } catch (err) {
    console.error('Startup error:', err && err.message);
  }
})();

// -------------------- express app --------------------
const app = express();
app.use(cors());
app.use(express.json());
app.use(compression({
  filter: (req, res) => {
    try { if (req && req.path && req.path.startsWith('/TF-')) return false; } catch(e){}
    return compression.filter(req, res);
  }
}));
app.use(express.static(path.join(__dirname, 'public'), { index: 'index.html' }));

// admin mapping inspector
app.get('/_admin/mapping/:token', async (req, res) => {
  const token = req.params.token;
  const out = { token, memory: mappings[token] || null, db: null, chunks: null, pending_found: false };
  try {
    for (const pinfo of poolInfos) {
      try {
        const r = await pinfo.pool.query('SELECT token, data, octet_length(file_data) AS file_data_len, created_at FROM uploads WHERE token=$1', [token]);
        if (r.rowCount) { out.db = out.db || []; out.db.push({ pool: pinfo.name, row: r.rows[0] }); out.file_data_len = r.rows[0].file_data_len; }
        const cr = await pinfo.pool.query('SELECT count(*)::int AS cnt FROM file_chunks WHERE token=$1', [token]);
        if (cr && cr.rows && cr.rows[0]) { out.chunks = out.chunks || []; out.chunks.push({ pool: pinfo.name, count: cr.rows[0].cnt }); }
      } catch(err) {
        out.db = out.db || []; out.db.push({ pool: pinfo.name, error: err.message });
      }
    }
  } catch(e){}
  try {
    const files = fs.readdirSync(PENDING_DIR).filter(f => f.endsWith('.json'));
    for (const jf of files) {
      try {
        const meta = JSON.parse(fs.readFileSync(path.join(PENDING_DIR, jf), 'utf8'));
        if (meta && meta.token === token) { out.pending_found = true; break; }
      } catch(e){}
    }
  } catch(e){}
  return res.json(out);
});
app.get('/_admin/mappings', (req, res) => res.json({ count: Object.keys(mappings).length, tokens: Object.keys(mappings).slice(0,50) }));

// -------------------- upload (streaming with Busboy) --------------------
app.post('/upload', (req, res) => {
  try {
    if (!poolInfos.length && !ENABLE_PENDING_FALLBACK) {
      return res.status(503).json({ error: 'No DB available. Enable pending fallback or provide DB.' });
    }

    const bb = new Busboy({ headers: req.headers, limits: { fileSize: MAX_FILE_SIZE } });
    const token = 'TF-' + genToken(7);
    let fileName = null;
    let safeName = null;
    let mime = null;
    let receivedBytes = 0;
    let seq = 0;
    let storingToDB = poolInfos.length > 0;
    let primaryPool = poolInfos.length ? poolInfos[0] : null;
    let pendingWriter = null;
    let pendingBinName = null;
    let entry = { token, originalName: null, safeOriginal: null, size: null, mime: null, createdAt: new Date().toISOString(), storage: storingToDB ? 'db' : 'pending_disk' };
    let totalBytesExpected = Number(req.headers['content-length'] || 0);

    function logProgress() {
      const humanGot = humanBytes(receivedBytes);
      const humanTotal = totalBytesExpected ? humanBytes(totalBytesExpected) : 'unknown';
      const pct = totalBytesExpected ? Math.round((receivedBytes / totalBytesExpected) * 100) : null;
      const pctStr = pct !== null ? ` (${pct}%)` : '';
      process.stdout.write(`\r${token} ${humanGot}/${humanTotal}${pctStr}`);
    }

    bb.on('file', (fieldname, file, filename, encoding, mimetype) => {
      fileName = filename || 'file';
      safeName = safeFileName(fileName);
      mime = mimetype || 'application/octet-stream';
      entry.originalName = fileName;
      entry.safeOriginal = safeName;
      entry.mime = mime;

      // prepare pending writer only if fallback enabled
      if (!storingToDB && ENABLE_PENDING_FALLBACK) {
        const pending = saveStreamToPendingFileSync(token);
        pendingWriter = pending.ws;
        pendingBinName = pending.filename;
      }

      file.on('data', async (data) => {
        // break into CHUNK_MAX_SIZE pieces
        for (let off=0; off < data.length; off += CHUNK_MAX_SIZE) {
          const piece = data.slice(off, Math.min(off + CHUNK_MAX_SIZE, data.length));
          // try DB insert if chosen
          if (storingToDB && primaryPool) {
            try {
              await primaryPool.pool.query('INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)', [token, seq, piece]);
            } catch (err) {
              console.warn('DB insert chunk failed mid-upload:', err && err.message);
              storingToDB = false;
              entry.storage = 'pending_disk';
              if (ENABLE_PENDING_FALLBACK) {
                if (!pendingWriter) {
                  const pend = saveStreamToPendingFileSync(token);
                  pendingWriter = pend.ws;
                  pendingBinName = pend.filename;
                }
                // write this piece to pending
                try { pendingWriter.write(piece); } catch(e){ console.error('Pending write failed', e && e.message); }
              } else {
                // Stop upload and respond error (DB full / not accepting)
                try { file.unpipe(); } catch(e){}
                console.error('Upload aborted: DB failure and pending fallback disabled.');
                return res.status(507).json({ error: 'Storage insufficient on DB; upload aborted.' });
              }
            }
          } else {
            // write to pending disk
            if (!ENABLE_PENDING_FALLBACK) {
              console.error('Pending fallback disabled but DB not available - aborting.');
              try { file.unpipe(); } catch(e){}
              return res.status(507).json({ error: 'Storage insufficient; pending fallback disabled.' });
            }
            try { pendingWriter.write(piece); } catch(e){ console.error('Failed writing pending', e && e.message); }
          }
          seq++;
          receivedBytes += piece.length;
        }
        logProgress();
      });

      file.on('end', async () => {
        entry.size = receivedBytes;
        // save metadata
        try {
          if (storingToDB) {
            await saveMappingMetadataToDBAcrossPools(token, entry);
            mappings[token] = entry;
          } else {
            // pending writer close + meta file
            if (pendingWriter) { pendingWriter.end(); savePendingMetaFileSync(token, entry, pendingBinName); }
            mappings[token] = entry;
          }
        } catch (err) {
          console.warn('Post-upload metadata save failed:', err && err.message);
          // if metadata failed and we used DB chunks, leave chunks there — admin can inspect later
          if (!storingToDB && pendingWriter) pendingWriter.end();
        }

        process.stdout.write(`\r${token} ${humanBytes(receivedBytes)}/${(totalBytesExpected?humanBytes(totalBytesExpected):humanBytes(receivedBytes))}\n`);

        const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
        const proto = protoHeader || req.protocol || 'https';
        const host = req.get('host');
        const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
        const sharePath = `/${token}/${encodeURIComponent(safeName)}`;
        const fileUrl = `${origin}${sharePath}`;

        return res.json({ token, url: fileUrl, sharePath, info: entry });
      });

      file.on('error', (err) => {
        console.error('Upload file stream error', err && err.message);
      });
    });

    bb.on('field', (name, val) => {
      // optional extra fields (durationSeconds)
      if (name === 'durationSeconds') {
        try { entry.durationSeconds = Number(val); } catch(e){}
      }
    });

    bb.on('finish', () => {
      // nothing to do here; response already sent in 'end' handler
    });

    bb.on('error', (err) => {
      console.error('Busboy error', err && err.message);
      try { res.status(500).json({ error: 'Upload parse failed', details: err && err.message }); } catch(e){}
    });

    req.pipe(bb);
  } catch (err) {
    console.error('Upload handler error', err && err.message);
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// -------------------- parse Range (fixed) --------------------
function parseRange(rangeHeader, size) {
  if (!rangeHeader) return null;
  const m = /bytes=(\d*)-(\d*)/.exec(rangeHeader.trim());
  if (!m) return null;
  const startStr = m[1];
  const endStr = m[2];

  if (startStr === '' && endStr !== '') {
    const lastN = parseInt(endStr, 10);
    if (isNaN(lastN) || lastN <= 0) return null;
    const start = Math.max(0, size - lastN);
    const end = size - 1;
    if (start > end) return null;
    return { start, end };
  }
  if (startStr !== '' && endStr === '') {
    const start = parseInt(startStr, 10);
    if (isNaN(start) || start < 0 || start >= size) return null;
    const end = size - 1;
    return { start, end };
  }
  if (startStr !== '' && endStr !== '') {
    let start = parseInt(startStr, 10);
    let end = parseInt(endStr, 10);
    if (isNaN(start) || isNaN(end) || start < 0 || end < 0 || start > end) return null;
    if (start >= size) return null;
    return { start: start, end: Math.min(end, size - 1) };
  }
  return null;
}

// -------------------- infer mime --------------------
function inferMimeFromName(name, fallback) {
  if (!name) return fallback || 'application/octet-stream';
  const ext = path.extname(name || '').toLowerCase();
  const map = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/mp4',
    '.webm': 'video/webm',
    '.ogg': 'video/ogg',
    '.ogv': 'video/ogg',
    '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav',
    '.mov': 'video/quicktime'
  };
  return map[ext] || fallback || 'application/octet-stream';
}

// -------------------- serve TF token (Range aware for chunks/pending) --------------------
app.get(['/TF-:token', '/TF-:token/:name', '/:token/:name'], async (req, res) => {
  try {
    const rawToken = req.params.token;
    const token = rawToken && rawToken.startsWith('TF-') ? rawToken : ('TF-' + rawToken);
    if (!token) return res.status(400).send('Bad token');

    // try DB file_data (rare)
    try {
      for (const pinfo of poolInfos) {
        try {
          const r = await pinfo.pool.query('SELECT data, (file_data IS NOT NULL) AS has_file, octet_length(file_data) AS file_len FROM uploads WHERE token=$1', [token]);
          if (r.rowCount && r.rows[0] && r.rows[0].has_file && r.rows[0].file_len) {
            const meta = r.rows[0].data || mappings[token] || {};
            const r2 = await pinfo.pool.query('SELECT file_data FROM uploads WHERE token=$1', [token]);
            const buf = r2.rows[0].file_data;
            const mime = inferMimeFromName(req.params.name || meta.safeOriginal, meta.mime);
            const fileLen = buf.length;
            const range = parseRange(req.headers.range, fileLen);
            res.setHeader('Accept-Ranges', 'bytes');
            res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
            res.setHeader('Content-Disposition', 'inline');
            res.setHeader('Content-Type', mime);

            if (range) {
              const { start, end } = range;
              if (start >= fileLen || end >= fileLen) { res.status(416).set('Content-Range', `bytes */${fileLen}`).end(); return; }
              const chunk = buf.slice(start, end + 1);
              res.status(206).set({
                'Content-Range': `bytes ${start}-${end}/${fileLen}`,
                'Content-Length': String(chunk.length)
              });
              // helpful headers for UI
              res.setHeader('X-TF-PLAYED-BYTES', String(start));
              res.setHeader('X-TF-TOTAL-BYTES', String(fileLen));
              if (meta && meta.durationSeconds) {
                const playedSec = Math.round((start / fileLen) * meta.durationSeconds);
                res.setHeader('X-TF-PLAYED-SEC', String(playedSec));
                res.setHeader('X-TF-TOTAL-SEC', String(meta.durationSeconds));
              }
              return res.send(chunk);
            } else {
              res.setHeader('Content-Length', String(fileLen));
              res.setHeader('X-TF-PLAYED-BYTES', '0');
              res.setHeader('X-TF-TOTAL-BYTES', String(fileLen));
              return res.send(buf);
            }
          }
        } catch(e){ continue; }
      }
    } catch(e){}

    // try DB chunks
    try {
      const fetched = await fetchAllChunksAcrossPools(token);
      const rows = fetched.rows || [];
      if (rows && rows.length) {
        const chunks = rows.map(r => Buffer.from(r.chunk));
        const chunkLens = chunks.map(b => b.length);
        const total = chunkLens.reduce((a,b)=>a+b,0);
        const meta = mappings[token] || null;
        const mime = inferMimeFromName(req.params.name || (meta && meta.safeOriginal), meta && meta.mime);

        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        res.setHeader('Content-Disposition', 'inline');
        res.setHeader('Content-Type', mime);

        const range = parseRange(req.headers.range, total);
        const startByte = range ? range.start : 0;
        res.setHeader('X-TF-PLAYED-BYTES', String(startByte));
        res.setHeader('X-TF-TOTAL-BYTES', String(total));
        if (meta && meta.durationSeconds) {
          const playedSec = Math.round((startByte / total) * meta.durationSeconds);
          res.setHeader('X-TF-PLAYED-SEC', String(playedSec));
          res.setHeader('X-TF-TOTAL-SEC', String(meta.durationSeconds));
        }

        if (!range) {
          res.setHeader('Content-Length', String(total));
          for (const b of chunks) res.write(b);
          return res.end();
        } else {
          const { start, end } = range;
          if (start >= total || end >= total) { res.status(416).set('Content-Range', `bytes */${total}`).end(); return; }
          const sendLen = end - start + 1;
          res.status(206).set({
            'Content-Range': `bytes ${start}-${end}/${total}`,
            'Content-Length': String(sendLen)
          });
          let remainingStart = start;
          let remainingToSend = sendLen;
          for (let i=0;i<chunks.length && remainingToSend>0;i++) {
            const cl = chunkLens[i];
            if (remainingStart >= cl) { remainingStart -= cl; continue; }
            const sliceStart = remainingStart;
            const sliceEnd = Math.min(cl - 1, sliceStart + remainingToSend - 1);
            const slice = chunks[i].slice(sliceStart, sliceEnd + 1);
            res.write(slice);
            remainingToSend -= (sliceEnd - sliceStart + 1);
            remainingStart = 0;
          }
          return res.end();
        }
      }
    } catch(e){}

    // pending disk (fallback)
    try {
      const jsonFiles = fs.readdirSync(PENDING_DIR).filter(f => f.endsWith('.json'));
      for (const jf of jsonFiles) {
        try {
          const meta = JSON.parse(fs.readFileSync(path.join(PENDING_DIR, jf), 'utf8'));
          if (meta && meta.token === token) {
            const binName = jf.replace(/\.json$/, '');
            const binPath = path.join(PENDING_DIR, binName);
            if (fs.existsSync(binPath)) {
              const stat = fs.statSync(binPath);
              const size = stat.size;
              let mime = (meta.entry && meta.entry.mime) || null;
              mime = inferMimeFromName(req.params.name || (meta.entry && meta.entry.safeOriginal), mime);
              const range = parseRange(req.headers.range, size);
              res.setHeader('Accept-Ranges', 'bytes');
              res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
              res.setHeader('Content-Type', mime);
              res.setHeader('Content-Disposition', 'inline');

              const startByte = range ? range.start : 0;
              res.setHeader('X-TF-PLAYED-BYTES', String(startByte));
              res.setHeader('X-TF-TOTAL-BYTES', String(size));
              if (meta.entry && meta.entry.durationSeconds) {
                const playedSec = Math.round((startByte / size) * meta.entry.durationSeconds);
                res.setHeader('X-TF-PLAYED-SEC', String(playedSec));
                res.setHeader('X-TF-TOTAL-SEC', String(meta.entry.durationSeconds));
              }

              if (range) {
                const { start, end } = range;
                if (start >= size || end >= size) { res.status(416).set('Content-Range', `bytes */${size}`).end(); return; }
                res.status(206).set({
                  'Content-Range': `bytes ${start}-${end}/${size}`,
                  'Content-Length': String(end - start + 1)
                });
                fs.createReadStream(binPath, { start, end }).pipe(res);
                return;
              } else {
                res.setHeader('Content-Length', String(size));
                return fs.createReadStream(binPath).pipe(res);
              }
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

// meta endpoint
app.get(['/TF-:token/meta','/:token/meta'], async (req, res) => {
  const rawToken = req.params.token;
  const token = rawToken && rawToken.startsWith('TF-') ? rawToken : ('TF-' + rawToken);
  const meta = mappings[token] || null;
  let dbInfo = null;
  if (poolInfos.length) {
    for (const pinfo of poolInfos) {
      try {
        const r = await pinfo.pool.query('SELECT data, (file_data IS NOT NULL) AS has_file, octet_length(file_data) AS file_len FROM uploads WHERE token=$1', [token]);
        if (r.rowCount) { dbInfo = r.rows[0]; break; }
      } catch(e){}
    }
  }
  if (!meta && !dbInfo) return res.status(404).json({ error: 'Not found' });
  const out = Object.assign({}, meta || (dbInfo && dbInfo.data) || {});
  if (dbInfo && dbInfo.file_len) out.file_data_len = dbInfo.file_len;
  if (out.durationSeconds) out.durationHuman = humanTimeFromSeconds(out.durationSeconds);
  return res.json(out);
});

// admin run migrations
app.post('/_admin/run-migrations', async (req, res) => {
  try {
    await runMigrationsAll();
    return res.json({ ok:true, message: 'migrations run' });
  } catch (err) {
    return res.status(500).json({ ok:false, error: err && err.message });
  }
});

// health
app.get('/health', (req,res) => res.json({ ok: true }));

// SPA fallback
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/TF-') || req.path.startsWith('/upload') || req.path.startsWith('/_admin') || req.path.startsWith('/health')) return next();
  const indexPath = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(indexPath)) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.sendFile(indexPath);
  }
  return res.status(404).send('Not found');
});

// error handler
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max: ' + MAX_FILE_SIZE });
  if (err) {
    console.error('Unhandled error:', err && (err.stack || err.message));
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

process.on('SIGINT', () => { console.log('SIGINT, exiting'); process.exit(0); });
process.on('SIGTERM', () => { console.log('SIGTERM, exiting'); process.exit(0); });

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
