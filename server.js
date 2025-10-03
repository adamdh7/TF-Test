require('dotenv').config();

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const MAPPINGS_KEY = process.env.MAPPINGS_KEY || 'mappings.json';
let BUCKET = process.env.S3_BUCKET || process.env.BUCKET || null;
const S3_REGION = process.env.S3_REGION || null;

// Normalize BASE_URL (optional)
let BASE_URL = process.env.BASE_URL || null;
if (BASE_URL) {
  if (!/^https?:\/\//i.test(BASE_URL)) BASE_URL = 'https://' + BASE_URL;
  BASE_URL = BASE_URL.replace(/\/+$/, '');
}

// Helpers
function looksLikePlaceholder(val) {
  if (!val || typeof val !== 'string') return true;
  const low = val.toLowerCase();
  return low.includes('your-') || low.includes('example') || low.includes('replace') || low.includes('yourbucket') || low.includes('s3.your-region') || low.includes('db_host');
}

// Disable S3 if placeholders present
if (looksLikePlaceholder(BUCKET) || looksLikePlaceholder(S3_REGION) ||
    looksLikePlaceholder(process.env.S3_ACCESS_KEY_ID) || looksLikePlaceholder(process.env.S3_SECRET_ACCESS_KEY)
) {
  console.warn('S3 config looks like placeholders or missing — S3 disabled.');
  BUCKET = null;
}

// Create S3 client only if BUCKET defined
let s3Client = null;
if (BUCKET) {
  s3Client = new S3Client({
    region: process.env.S3_REGION || 'auto',
    endpoint: process.env.S3_ENDPOINT || undefined,
    forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true' || false,
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY
    }
  });
  console.log('S3 client created for bucket:', BUCKET);
} else {
  console.log('S3 disabled or not configured.');
}

// Postgres pool dynamic
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
    console.warn('pg not installed or failed to init. DB disabled. Install "pg" and set DATABASE_URL to enable.', err && err.message);
    pool = null;
  }
} else {
  console.log('No DATABASE_URL configured — DB disabled (using local upload.json).');
}

// Ensure upload dir
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// In-memory mappings
let mappings = {};

// Disk persistence
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

// DB helpers
async function runMigrations() {
  if (!pool) return;
  const sql = `
    CREATE TABLE IF NOT EXISTS uploads (
      token TEXT PRIMARY KEY,
      data JSONB NOT NULL,
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

async function loadMappingsFromDB() {
  if (!pool) return {};
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

async function saveMappingToDB(token, entry) {
  if (!pool) return;
  try {
    const q = `
      INSERT INTO uploads(token, data, created_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (token)
      DO UPDATE SET data = $2::jsonb, created_at = NOW();
    `;
    await pool.query(q, [token, JSON.stringify(entry)]);
  } catch (err) {
    console.error('Failed saving mapping to DB:', err && err.message);
  }
}

// S3 mappings (optional)
async function loadMappingsFromS3() {
  if (!BUCKET || !s3Client) { console.log('Skipping S3 load'); return; }
  try {
    const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY });
    const res = await s3Client.send(cmd);
    const streamToString = (stream) => new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', c => chunks.push(c));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
    const text = await streamToString(res.Body);
    const s3m = JSON.parse(text || '{}');
    mappings = Object.assign({}, s3m, mappings);
    console.log('Merged mappings from S3:', Object.keys(s3m).length);
  } catch (err) {
    console.warn('Could not load mappings from S3:', err && err.message);
  }
}

async function saveMappingsToS3() {
  if (!BUCKET || !s3Client) return;
  try {
    const body = JSON.stringify(mappings, null, 2);
    const put = new PutObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY, Body: body, ContentType: 'application/json' });
    await s3Client.send(put);
    console.log('Saved mappings to S3');
  } catch (err) {
    console.error('Failed saving mappings to S3', err && err.message);
  }
}

// Token + filename helpers
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
// Truncate filename to show start...end (e.g. first 12 + '…' + last 12) with max total length
function truncateMiddle(name, maxLen = 30) {
  if (!name) return name;
  if (name.length <= maxLen) return name;
  const keep = Math.floor((maxLen - 1) / 2);
  const start = name.slice(0, keep);
  const end = name.slice(name.length - keep);
  return `${start}…${end}`;
}

function safeFileName(name) {
  const ext = path.extname(name);
  const base = path.basename(name, ext);
  const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
  const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
  return (safeBase + safeExt) || 'file';
}

// Multer memory storage
const storage = multer.memoryStorage();
const upload = multer({ storage, limits: { fileSize: 5368709120 } }); // 5GB

// Express app
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
// optional: serve uploads folder directly (comment out if you don't want public access)
// app.use('/uploads', express.static(UPLOAD_DIR));

// Startup: load disk, DB, S3
loadMappingsFromDisk();
(async () => {
  if (pool) {
    await runMigrations();
    const dbm = await loadMappingsFromDB();
    // prefer DB mappings over disk (DB overrides)
    mappings = Object.assign({}, mappings, dbm);
    // persist local-only to DB
    for (const [token, entry] of Object.entries(mappings)) {
      if (!dbm[token]) {
        await saveMappingToDB(token, entry).catch(()=>{});
      }
    }
  }
  await loadMappingsFromS3();
  saveMappingsToDisk();
  if (BUCKET) await saveMappingsToS3();
})();

// Pagination endpoint for UI (fast)
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
        storage: row.storage,
        createdAt: row.created_at
      }));
      return res.json({ items });
    } catch (err) {
      console.warn('DB list error', err && err.message);
    }
  }
  // fallback to disk mapping
  const all = Object.values(mappings).sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt));
  const slice = all.slice(offset, offset + limit).map(e => ({
    token: e.token,
    displayName: truncateMiddle(e.originalName || e.safeOriginal, 30),
    safeOriginal: e.safeOriginal,
    storage: e.storage,
    createdAt: e.createdAt
  }));
  return res.json({ items: slice, total: all.length });
});

// Upload endpoint - respond fast: save local and return URL; S3 upload happens async
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();
    const s3Key = `uploads/TF-${token}/${Date.now()}_${safeOriginal}`;

    // ALWAYS save a local cache copy for resilience (absolute path)
    const localFilename = `${Date.now()}_${token}_${safeOriginal}`;
    const localPath = path.resolve(UPLOAD_DIR, localFilename);
    fs.writeFileSync(localPath, req.file.buffer);

    // prepare mapping entry
    const entry = {
      token,
      s3Key: null,
      originalName,
      safeOriginal,
      size: req.file.size,
      mime: req.file.mimetype,
      createdAt: new Date().toISOString(),
      storage: 'local',
      localPath
    };

    // Save mapping locally & DB quickly
    mappings[token] = entry;
    saveMappingsToDisk();
    if (pool) {
      saveMappingToDB(token, entry).catch(e => console.warn('saveMappingToDB error', e && e.message));
    }

    // Async S3 upload (fire-and-forget) to speed up response
    if (BUCKET && s3Client) {
      (async () => {
        try {
          const put = new PutObjectCommand({
            Bucket: BUCKET,
            Key: s3Key,
            Body: req.file.buffer,
            ContentType: req.file.mimetype
          });
          await s3Client.send(put);
          // update mapping to s3
          entry.s3Key = s3Key;
          entry.storage = 's3';
          mappings[token] = entry;
          saveMappingsToDisk();
          if (pool) await saveMappingToDB(token, entry);
          // optionally save mappings to S3 (less important)
          if (BUCKET) await saveMappingsToS3();
          console.log('Async S3 upload complete for token', token);
        } catch (err) {
          console.warn('Async S3 upload failed for token', token, err && err.message);
        }
      })().catch(err => console.warn('Async upload fire error', err && err.message));
    }

    // Construct sharePath & URL robustly
    const encodedName = encodeURIComponent(safeOriginal);
    const sharePath = `/TF-${token}/${encodedName}`;

    const protoHeader = (req.headers['x-forwarded-proto'] || '').split(',')[0];
    const proto = protoHeader || req.protocol || 'https';
    const host = req.get('host');
    const origin = BASE_URL || `${proto}://${host}`;
    const fileUrl = `${origin}${sharePath}`;

    // Return truncated display name too (for UI)
    return res.json({
      token,
      url: fileUrl,
      sharePath,
      info: entry,
      displayName: truncateMiddle(originalName, 36)
    });
  } catch (err) {
    console.error('Upload error', err && err.message);
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
    return res.status(500).json({ error: 'Upload failed', details: err && err.message });
  }
});

// Serve file: prefer S3 signed URL if available, else local file (absolute path)
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).send('Not found');

  try {
    // If S3 available and entry has s3Key, issue presigned URL
    if (entry.s3Key && BUCKET && s3Client) {
      try {
        const expiresSec = Number(process.env.PRESIGN_EXPIRES || 3600);
        const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key });
        const signedUrl = await getSignedUrl(s3Client, getCmd, { expiresIn: expiresSec });
        // Short cache for presigned redirect
        res.setHeader('Cache-Control', 'public, max-age=60, s-maxage=60');
        return res.redirect(302, signedUrl);
      } catch (err) {
        console.warn('Presign failed, falling back to local for token', token, err && err.message);
      }
    }

    // Serve local file if exists
    let filePath = entry.localPath || path.join(UPLOAD_DIR, entry.filename || '');
    if (!path.isAbsolute(filePath)) filePath = path.resolve(__dirname, filePath);
    if (!filePath || !fs.existsSync(filePath)) {
      console.warn('Local file missing for token', token, 'path:', filePath);
      return res.status(410).send('File removed or missing');
    }

    // set long cache for stable uploaded files (safe if files are immutable)
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    const suggestedName = entry.originalName || entry.safeOriginal || path.basename(filePath);
    res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
    return res.sendFile(filePath);
  } catch (err) {
    console.error('Error serving file', err && err.message);
    return res.status(500).send('Serve error');
  }
});

// Admin debug: inspect mapping (remove in prod if you want)
app.get('/_admin/token/:token', (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).json({ ok: false, error: 'not found' });
  return res.json({ ok: true, token, entry, displayName: truncateMiddle(entry.originalName || entry.safeOriginal, 36) });
});

// Health
app.get('/health', (req, res) => res.json({ ok: true }));

// Error handler
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  if (err) {
    console.error('Unhandled error:', err && err.message);
    return res.status(500).json({ error: 'Server error', details: err && err.message });
  }
  next();
});

// Start server
app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
