// server.js - multi-DB + Range support for blobs, chunks, pending disk + SPA-safe static serving
require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const multer = require('multer');
const { Pool } = require('pg');
const { spawnSync } = require('child_process');

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const PENDING_DIR = path.join(UPLOAD_DIR, 'pending');

const CHUNK_MAX_SIZE = Number(process.env.CHUNK_MAX_SIZE || 8 * 1024 * 1024); // 8MB default
const MAX_FILE_SIZE = Number(process.env.MAX_FILE_SIZE || 5 * 1024 * 1024 * 1024); // 5GB
const RUN_MIGRATIONS_AUTOMATIC = (process.env.RUN_MIGRATIONS_AUTOMATIC || 'true').toLowerCase() === 'true';
const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || 2);
const PENDING_RETRY_INTERVAL = Number(process.env.PENDING_RETRY_INTERVAL || 30) * 1000; // seconds -> ms

// ensure dirs
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); fs.mkdirSync(PENDING_DIR, { recursive: true }); } catch(e){}

// -------------------- pools creation (DATABASE_URL*) --------------------
let poolInfos = []; // array { name, pool, connString }
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
    if (!raw || typeof raw !== 'string' || !raw.trim()) {
      console.warn(`${key} is empty — skipping.`);
      continue;
    }
    const conn = raw.trim();
    if (!isLikelyConnectionString(conn)) {
      console.warn(`${key} doesn't look like a connection string — skipping.`);
      continue;
    }
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
  if (!poolInfos.length) console.warn('No DB pools created - DB operations will fallback to disk.');
}
createPoolsFromEnv();

// -------------------- mappings load/save --------------------
let mappings = {};
function loadMappingsFromDisk() {
  try {
    if (fs.existsSync(UPLOAD_JSON)) {
      // load whatever is on disk as a fallback/seed; it'll be migrated to DB on startup if DBs exist
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
  // IMPORTANT: do NOT persist upload.json when we have working DB pools. Keep upload.json only as a fallback for when no DB.
  if (poolInfos && poolInfos.length) {
    // noop when DBs available
    return;
  }
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

// New helper: get client ip reliably (x-forwarded-for first)
function getClientIp(req) {
  try {
    const xf = req.headers['x-forwarded-for'];
    if (xf && typeof xf === 'string' && xf.length) {
      return xf.split(',')[0].trim();
    }
    if (req.ip) return req.ip;
    if (req.connection && req.connection.remoteAddress) return req.connection.remoteAddress;
  } catch(e){}
  return 'unknown';
}

// -------------------- logging & format helpers --------------------
function fmtBytesLower(bytes) {
  if (!bytes || bytes <= 0) return '0ko';
  const KB = 1024;
  const MB = KB * 1024;
  if (bytes >= MB) {
    const v = Math.round(bytes / MB);
    return `${v}mo`;
  }
  const v = Math.round(bytes / KB);
  return `${v}ko`;
}
function formatTimeHMS(sec) {
  if (!sec || sec <= 0) return '0s';
  sec = Math.round(sec);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  let out = '';
  if (h) out += `${h}h`;
  if (m || (h && !m)) out += `${m}mn`;
  out += `${s}s`;
  return out;
}

// in-memory watched stats: { token: { userKey: secondsWatched } }
const watchedStats = {};

// updated log helper for viewing progress and accumulate per-user
function logViewingProgress({ req, token, bytesSent, totalBytes, durationSeconds }) {
  try {
    const userKey = getClientIp(req) || (req.ip || req.headers['x-forwarded-for'] || 'unknown').toString();
    // compute percent
    const percent = totalBytes ? Math.round((bytesSent / totalBytes) * 100) : 0;
    let seenSeconds = 0;
    if (durationSeconds && totalBytes) {
      seenSeconds = Math.round(durationSeconds * (totalBytes ? (bytesSent / totalBytes) : 0));
    }
    // store per-user
    watchedStats[token] = watchedStats[token] || {};
    watchedStats[token][userKey] = watchedStats[token][userKey] || 0;
    // accumulate only if we have seenSeconds > 0
    if (seenSeconds) watchedStats[token][userKey] = Math.max(watchedStats[token][userKey], seenSeconds);

    // get totals
    const totalSeenForUser = watchedStats[token][userKey];
    const totalDurationStr = durationSeconds ? formatTimeHMS(durationSeconds) : 'unknown';
    const seenStr = durationSeconds ? `${formatTimeHMS(seenSeconds)}/${totalDurationStr}` : `${fmtBytesLower(bytesSent)}/${fmtBytesLower(totalBytes)}`;

    // one-line terminal friendly output with percent
    console.log(`[TF-STREAM] token=${token} ip=${userKey} sent=${fmtBytesLower(bytesSent)}/${fmtBytesLower(totalBytes)}    ${percent}% viewed=${seenStr} userTotal=${formatTimeHMS(totalSeenForUser)}`);
  } catch (e) {
    console.warn('logViewingProgress error', e && e.message);
  }
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
    // use simple index names to avoid invalid characters
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

// -------------------- DB helpers (multi-pool fallback) --------------------
async function saveChunksToDBAcrossPools(token, buffer) {
  if (!poolInfos.length) throw new Error('No DB pools available');
  let lastErr = null;
  for (const pinfo of poolInfos) {
    try {
      const client = await pinfo.pool.connect();
      try {
        await client.query('BEGIN');
        let seq = 0;
        for (let offset=0; offset<buffer.length; offset += CHUNK_MAX_SIZE) {
          const piece = buffer.slice(offset, Math.min(offset + CHUNK_MAX_SIZE, buffer.length));
          await client.query('INSERT INTO file_chunks (token, seq, chunk) VALUES ($1,$2,$3)', [token, seq, piece]);
          seq++;
        }
        await client.query('COMMIT');
        client.release();
        return pinfo.name;
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch(e){/*ignore*/ }
        client.release();
        lastErr = err;
        console.warn(`Save chunks to ${pinfo.name} failed:`, err && err.message);
        continue;
      }
    } catch (err) {
      lastErr = err;
      console.warn(`Could not connect to ${pinfo.name}:`, err && err.message);
      continue;
    }
  }
  throw lastErr || new Error('All DB pools failed to save chunks');
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

// -------------------- pending disk helpers --------------------
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

// -------------------- startup --------------------
(async () => {
  try {
    if (poolInfos.length && RUN_MIGRATIONS_AUTOMATIC) await runMigrationsAll();
    // load mappings from DBs (merge newest)
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
    // merge DB mappings into memory; DB is authoritative if present
    mappings = Object.assign({}, dbm, mappings);
    // best-effort persist any remaining local-only entries (from upload.json) into DBs
    if (poolInfos.length) {
      for (const [token, entry] of Object.entries(mappings)) {
        try { await saveMappingMetadataToDBAcrossPools(token, entry); } catch(e){ console.warn('persist local->DB failed for', token, e && e.message); }
      }
      try {
        if (fs.existsSync(UPLOAD_JSON)) {
          fs.unlinkSync(UPLOAD_JSON);
          console.log('Removed local upload.json after migrating to DBs');
        }
      } catch(e) {
        console.warn('Could not remove upload.json after migration:', e && e.message);
      }
    } else {
      // no DBs: keep using disk-backed mappings
      saveMappingsToDisk();
    }
    // start pending retries only once
    pendingRetryLoop();
    console.log('Startup complete. mappings:', Object.keys(mappings).length);
  } catch (err) {
    console.error('Startup error:', err && err.message);
  }
})();

// -------------------- express app --------------------
const app = express();
app.use(cors());

// --- DEBUG / admin helpers (use for troubleshooting) ---
function safeLog(...args) {
  try { console.log(...args); } catch(e){}
}
// quick mapping inspector
app.get('/_admin/mapping/:token', async (req, res) => {
  const token = req.params.token;
  const out = { token, memory: mappings[token] || null, db: null, chunks: null, file_data_len: null, pending_found: false };
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
app.get('/_admin/mappings', (req, res) => {
  return res.json({ count: Object.keys(mappings).length, tokens: Object.keys(mappings).slice(0,50) });
});
app.use((req, res, next) => {
  if (req.path && req.path.startsWith('/TF-')) {
    safeLog('[TF-REQUEST] path=', req.path, 'ip=', getClientIp(req), 'range=', req.headers.range || 'none');
  }
  next();
});

// IMPORTANT CHANGE: do not compress TF- endpoints (video streaming). Use compression.filter but skip paths starting with /TF-
app.use(compression({
  filter: (req, res) => {
    try {
      if (req && req.path && req.path.startsWith('/TF-')) return false;
    } catch(e){}
    return compression.filter(req, res);
  }
}));

app.use(express.json());

// serve static first (index.html is in public)
app.use(express.static(path.join(__dirname, 'public'), { index: 'index.html' }));

// multer (memory)
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage, limits: { fileSize: MAX_FILE_SIZE } });

// -------------------- upload progress middleware --------------------
function uploadProgressMiddleware(req, res, next) {
  const total = parseInt(req.headers['content-length'] || '0', 10) || 0;
  let received = 0;
  let lastLogTime = 0;
  let lastLoggedBytes = 0;

  function maybeLog() {
    const now = Date.now();
    // throttle logs to ~200ms and only if bytes changed enough
    if (now - lastLogTime < 200 && Math.abs(received - lastLoggedBytes) < 16*1024) return;
    lastLogTime = now;
    lastLoggedBytes = received;
    const totalStr = total ? fmtBytesLower(total) : 'unknown';
    const pct = total ? Math.round((received / total) * 100) : 0;
    console.log(`[UPLOAD] ${fmtBytesLower(received)}/${totalStr}    ${pct}%`);
  }

  req.on('data', (chunk) => {
    received += chunk.length;
    maybeLog();
  });
  req.on('end', () => {
    const totalStr = total ? fmtBytesLower(total) : 'unknown';
    console.log(`[UPLOAD] done ${fmtBytesLower(received)}/${totalStr}    100%`);
  });
  next();
}

// -------------------- helper: parse Range header --------------------
function parseRange(rangeHeader, size) {
  if (!rangeHeader) return null;
  const m = /bytes=(\d*)-(\d*)/.exec(rangeHeader);
  if (!m) return null;
  const start = m[1] === '' ? null : parseInt(m[1], 10);
  const end = m[2] === '' ? null : parseInt(m[2], 10);
  if (start === null && end === null) return null;
  const s = start !== null ? start : (size - (end + 1));
  const e = end !== null ? end : (size - 1);
  if (isNaN(s) || isNaN(e) || s > e || s < 0) return null;
  return { start: s, end: e };
}

// -------------------- helper: infer mime from filename --------------------
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

// -------------------- try to probe duration using ffprobe (if available) --------------------
function probeDurationFromBuffer(buf) {
  const tmp = path.join(PENDING_DIR, `probe-${Date.now()}-${genToken(6)}.tmp`);
  try {
    fs.writeFileSync(tmp, buf);
    const out = spawnSync('ffprobe', ['-v','quiet','-print_format','json','-show_format', tmp], { encoding:'utf8', timeout: 7000 });
    if (out && out.status === 0 && out.stdout) {
      const j = JSON.parse(out.stdout);
      const dur = j && j.format && parseFloat(j.format.duration);
      if (!isNaN(dur) && dur > 0) return dur;
    }
  } catch (e) {
    // ffprobe might not be available — ignore
  } finally {
    try { fs.unlinkSync(tmp); } catch(e){}
  }
  return null;
}

// ---------- upload ----------
app.post('/upload', uploadProgressMiddleware, upload.single('file'), async (req, res) => {
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
      storage: 'db'
    };

    const buf = req.file.buffer;

    // try probe duration (best-effort)
    try {
      const dur = probeDurationFromBuffer(buf);
      if (dur && !isNaN(dur)) {
        entry.duration = Math.round(dur); // seconds
        console.log(`[UPLOAD] probed duration: ${formatTimeHMS(entry.duration)} for token ${token}`);
      }
    } catch (e) {
      console.warn('[UPLOAD] ffprobe probe failed', e && e.message);
    }

    try {
      const storedOn = await saveChunksToDBAcrossPools(token, buf);
      try { await saveMappingMetadataToDBAcrossPools(token, entry); } catch(e){ console.warn('metadata save failed', e && e.message); }
      entry.storage = storedOn;
      mappings[token] = entry;
      if (!poolInfos.length) saveMappingsToDisk();

      const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
      const proto = protoHeader || req.protocol || 'https';
      const host = req.get('host');
      const origin = (process.env.BASE_URL && process.env.BASE_URL.replace(/\/+$/, '')) || `${proto}://${host}`;
      const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
      const fileUrl = `${origin}${sharePath}`;

      return res.json({ token, url: fileUrl, sharePath, info: entry });
    } catch (dbErr) {
      console.error('Save to all DB pools failed:', dbErr && dbErr.message);
      try {
        saveBufferToPending(token, entry, buf);
        entry.storage = 'pending_disk';
        mappings[token] = entry;
        if (!poolInfos.length) saveMappingsToDisk();
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

// -------------------- serve TF token (Range aware for blobs, chunks, pending) --------------------
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  try {
    const token = req.params.token;
    if (!token) return res.status(400).send('Bad token');

    // helper to obtain durationSeconds from mappings or DB metadata
    async function getDurationSeconds() {
      try {
        if (mappings[token] && mappings[token].duration) return mappings[token].duration;
        const meta = await fetchUploadEntryAcrossPools(token);
        if (meta && meta.data && meta.data.duration) return meta.data.duration;
      } catch (e){}
      return null;
    }

    // 1) try file_data blob across pools
    try {
      const fileData = await fetchFileDataFromPools(token);
      if (fileData && fileData.buf) {
        const buf = fileData.buf;
        const meta = await fetchUploadEntryAcrossPools(token);
        let mime = (meta && meta.data && meta.data.mime) || (mappings[token] && mappings[token].mime) || null;
        mime = inferMimeFromName(req.params.name || (meta && meta.data && meta.data.safeOriginal) || (mappings[token] && mappings[token].safeOriginal), mime);
        const fileLen = buf.length;
        const range = parseRange(req.headers.range, fileLen);

        // common headers
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        res.setHeader('Content-Disposition', 'inline'); // <- ensure inline, not attachment
        res.setHeader('Content-Type', mime);

        const durationSeconds = await getDurationSeconds();

        if (range) {
          const { start, end } = range;
          if (start >= fileLen || end >= fileLen) {
            res.status(416).set('Content-Range', `bytes */${fileLen}`).end();
            return;
          }
          const chunk = buf.slice(start, end + 1);
          const chunkLen = chunk.length;
          res.status(206).set({
            'Content-Range': `bytes ${start}-${end}/${fileLen}`,
            'Content-Length': String(chunkLen)
          });
          // write chunk and log bytes sent (count relative to whole file)
          res.write(chunk);
          res.end();
          // bytesSent here should represent cumulative bytes delivered in this request.
          // For range requests we only sent chunkLen — but percent computed vs fileLen is chunkLen/fileLen.
          logViewingProgress({ req, token, bytesSent: end + 1, totalBytes: fileLen, durationSeconds });
          return;
        } else {
          res.set({
            'Content-Length': String(fileLen)
          });
          // send whole buffer; count and log
          res.write(buf);
          res.end();
          logViewingProgress({ req, token, bytesSent: fileLen, totalBytes: fileLen, durationSeconds });
          return;
        }
      }
    } catch (e) {
      console.warn('file_data fetch error (non-fatal):', e && e.message);
    }

    // 2) try DB chunks across pools
    try {
      const { rows } = await fetchAllChunksAcrossPools(token);
      if (rows && rows.length) {
        const chunks = rows.map(r => Buffer.from(r.chunk));
        const chunkLens = chunks.map(b => b.length);
        const total = chunkLens.reduce((a,b)=>a+b,0);
        let mime = (mappings[token] && mappings[token].mime) || null;
        mime = inferMimeFromName(req.params.name || (mappings[token] && mappings[token].safeOriginal), mime);

        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        res.setHeader('Content-Disposition', 'inline');
        res.setHeader('Content-Type', mime);

        const range = parseRange(req.headers.range, total);
        const durationSeconds = await getDurationSeconds();

        if (!range) {
          // stream all sequentially with counting
          res.setHeader('Content-Length', String(total));
          let sent = 0;
          for (const b of chunks) {
            res.write(b);
            sent += b.length;
          }
          res.end();
          logViewingProgress({ req, token, bytesSent: sent, totalBytes: total, durationSeconds });
          return;
        } else {
          const { start, end } = range;
          if (start >= total || end >= total) {
            res.status(416).set('Content-Range', `bytes */${total}`).end();
            return;
          }
          const sendLen = end - start + 1;
          res.status(206).set({
            'Content-Range': `bytes ${start}-${end}/${total}`,
            'Content-Length': String(sendLen)
          });
          // find which chunks and offsets to send, count as we go
          let remainingStart = start;
          let remainingToSend = sendLen;
          let sent = 0;
          for (let i=0;i<chunks.length && remainingToSend>0;i++) {
            const cl = chunkLens[i];
            if (remainingStart >= cl) {
              remainingStart -= cl;
              continue;
            }
            const sliceStart = remainingStart;
            const sliceEnd = Math.min(cl - 1, sliceStart + remainingToSend - 1);
            const slice = chunks[i].slice(sliceStart, sliceEnd + 1);
            res.write(slice);
            sent += slice.length;
            remainingToSend -= (sliceEnd - sliceStart + 1);
            remainingStart = 0;
          }
          res.end();
          // bytesSent is start + sent to indicate cumulative position relative to file
          logViewingProgress({ req, token, bytesSent: start + sent, totalBytes: total, durationSeconds });
          return;
        }
      }
    } catch (e) {
      console.warn('chunks fetch error (non-fatal):', e && e.message);
    }

    // 3) pending disk
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

              const durationSeconds = (meta.entry && meta.entry.duration) || await getDurationSeconds();

              if (range) {
                const { start, end } = range;
                if (start >= size || end >= size) { res.status(416).set('Content-Range', `bytes */${size}`).end(); return; }
                res.status(206).set({
                  'Content-Range': `bytes ${start}-${end}/${size}`,
                  'Content-Length': String(end - start + 1)
                });
                // count bytes from stream 'data' events
                let sent = 0;
                const rs = fs.createReadStream(binPath, { start, end });
                rs.on('data', (chunk) => { sent += chunk.length; });
                rs.on('end', () => {
                  logViewingProgress({ req, token, bytesSent: start + sent, totalBytes: size, durationSeconds });
                });
                rs.pipe(res);
                return;
              } else {
                res.setHeader('Content-Length', String(size));
                let sent = 0;
                const rs = fs.createReadStream(binPath);
                rs.on('data', (chunk) => { sent += chunk.length; });
                rs.on('end', () => {
                  logViewingProgress({ req, token, bytesSent: sent, totalBytes: size, durationSeconds });
                });
                return rs.pipe(res);
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

// ---------- admin migrations ----------
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

// serve SPA index fallback for non-API GETs (so deep links still load your UI)
// IMPORTANT: place after API routes to avoid overriding them
app.get('*', (req, res, next) => {
  // don't override TF- or API calls
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

// start pending retry loop (already started on startup; keep it safe here)
pendingRetryLoop();

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
