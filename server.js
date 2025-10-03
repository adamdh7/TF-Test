// server.js — defensive, improved error handling, admin inspect endpoint
require('dotenv').config();

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json');
const MAPPINGS_KEY = process.env.MAPPINGS_KEY || 'mappings.json';
let BUCKET = process.env.S3_BUCKET || process.env.BUCKET || null;
const S3_REGION = process.env.S3_REGION || null;
const BASE_URL = process.env.BASE_URL || null;

// utility: detect placeholder values
function looksLikePlaceholder(val) {
  if (!val || typeof val !== 'string') return true;
  const low = val.toLowerCase();
  return low.includes('your-') || low.includes('example') || low.includes('replace') || low.includes('yourbucket') || low.includes('s3.your-region') || low.includes('db_host');
}

// Disable S3 if placeholders present
if (looksLikePlaceholder(BUCKET) || looksLikePlaceholder(S3_REGION) ||
    looksLikePlaceholder(process.env.S3_ACCESS_KEY_ID) || looksLikePlaceholder(process.env.S3_SECRET_ACCESS_KEY)) {
  console.warn('S3 config looks like placeholders or missing — S3 disabled. Set S3_BUCKET, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY to enable.');
  BUCKET = null;
}

// create S3 client only if BUCKET set
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

// Postgres pool (dynamic require)
let pool = null;
if (process.env.DATABASE_URL && typeof process.env.DATABASE_URL === 'string' && process.env.DATABASE_URL.trim() !== '') {
  try {
    // accept both 'postgresql://' and 'postgres://'
    const { Pool } = require('pg');
    const cfg = { connectionString: process.env.DATABASE_URL };
    if (process.env.DATABASE_SSL === 'true' || process.env.DATABASE_SSL === '1') {
      cfg.ssl = { rejectUnauthorized: false };
    }
    pool = new Pool(cfg);
    pool.on('error', (err) => console.error('Unexpected PG client error', err));
    console.log('Postgres pool created (pg loaded).');
  } catch (err) {
    console.warn('pg module not installed or failed to initialize. DB disabled. Install "pg" and set DATABASE_URL to enable DB.', err.message || String(err));
    pool = null;
  }
} else {
  console.log('No DATABASE_URL configured — DB disabled (using local upload.json).');
}

// ensure uploads dir
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// in-memory mappings
let mappings = {};

// disk functions
function loadMappingsFromDisk() {
  try {
    if (fs.existsSync(UPLOAD_JSON)) {
      const text = fs.readFileSync(UPLOAD_JSON, 'utf8');
      mappings = JSON.parse(text || '{}');
      console.log('Loaded mappings from disk:', Object.keys(mappings).length);
    } else {
      mappings = {};
      console.log('No upload.json found — starting with empty mappings.');
    }
  } catch (err) {
    console.warn('Failed to load upload.json:', err.message || String(err));
    mappings = {};
  }
}
function saveMappingsToDisk() {
  try {
    const tmp = UPLOAD_JSON + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(mappings, null, 2));
    fs.renameSync(tmp, UPLOAD_JSON);
  } catch (err) {
    console.error('Failed to save upload.json:', err.message || String(err));
  }
}

// DB functions (safe)
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
    console.error('DB migration failed:', err.message || String(err));
  }
}
async function loadMappingsFromDB() {
  if (!pool) { console.log('No DB pool - skip DB load'); return {}; }
  try {
    const res = await pool.query('SELECT token, data FROM uploads');
    const dbm = {};
    if (res && res.rows) {
      res.rows.forEach(r => { dbm[r.token] = r.data; });
    }
    console.log('Loaded mappings from DB:', Object.keys(dbm).length);
    return dbm;
  } catch (err) {
    console.warn('Failed loading mappings from DB:', err.message || String(err));
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
    console.error('Failed saving mapping to DB:', err.message || String(err));
  }
}

// S3 functions
async function loadMappingsFromS3() {
  if (!BUCKET || !s3Client) { console.log('Skipping S3 load (no bucket/client)'); return; }
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
    const s3Mappings = JSON.parse(text || '{}');
    mappings = Object.assign({}, s3Mappings, mappings);
    console.log('Merged mappings from S3:', Object.keys(s3Mappings).length);
  } catch (err) {
    console.warn('Could not load mappings from S3 (maybe first run):', err.message || String(err));
  }
}
async function saveMappingsToS3() {
  if (!BUCKET || !s3Client) return;
  try {
    const body = JSON.stringify(mappings, null, 2);
    const put = new PutObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY, Body: body, ContentType: 'application/json' });
    await s3Client.send(put);
    console.log('Saved mappings to S3.');
  } catch (err) {
    console.error('Failed saving mappings to S3:', err.message || String(err));
  }
}

// utils
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i = 0; i < len; i++) t += chars[Math.floor(Math.random() * chars.length)];
  return t;
}
function genUniqueToken() {
  let t = genToken(8), tries = 0;
  while (mappings[t] && tries < 100) { t = genToken(8); tries++; }
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

// multer
const storage = multer.memoryStorage();
const upload = multer({ storage, limits: { fileSize: 5368709120 } }); // 5GB

// express
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// startup: load disk -> migrate DB -> load DB -> load S3 -> persist merged
loadMappingsFromDisk();
(async () => {
  if (pool) await runMigrations();
  const dbm = await loadMappingsFromDB();
  mappings = Object.assign({}, mappings, dbm);
  if (pool) {
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

// upload endpoint
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();
    const s3Key = `uploads/TF-${token}/${Date.now()}_${safeOriginal}`;

    // always write local cache
    const localFilename = `${Date.now()}_${token}_${safeOriginal}`;
    const localPath = path.join(UPLOAD_DIR, localFilename);
    fs.writeFileSync(localPath, req.file.buffer);

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

    if (BUCKET && s3Client) {
      try {
        const put = new PutObjectCommand({
          Bucket: BUCKET,
          Key: s3Key,
          Body: req.file.buffer,
          ContentType: req.file.mimetype,
        });
        await s3Client.send(put);
        entry.s3Key = s3Key;
        entry.storage = 's3';
      } catch (err) {
        console.warn('S3 upload failed, keeping local cache:', err.message || String(err));
        entry.storage = 'local';
      }
    }

    mappings[token] = entry;
    saveMappingsToDisk();
    await saveMappingToDB(token, entry).catch(()=>{});
    await saveMappingsToS3().catch(()=>{});

    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = BASE_URL ? `${BASE_URL.replace(/\/+$/, '')}${sharePath}` : `${req.protocol}://${req.get('host')}${sharePath}`;

    return res.json({ token, url: fileUrl, sharePath, info: entry });
  } catch (err) {
    console.error('Upload error:', err);
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
    return res.status(500).json({ error: 'Upload failed', details: err.message || String(err) });
  }
});

// serve endpoint — safer handling
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).send('Not found');

  try {
    // if storage local or s3 client missing, try localPath first
    if (!entry.s3Key || entry.storage === 'local' || !s3Client) {
      const filePath = entry.localPath || path.join(UPLOAD_DIR, entry.filename || '');
      if (!filePath || !fs.existsSync(filePath)) {
        console.warn('Local file missing for token', token, 'path:', filePath);
        return res.status(410).send('File removed or missing');
      }
      const suggestedName = entry.originalName || entry.safeOriginal || path.basename(filePath);
      res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
      return res.sendFile(filePath);
    }

    // otherwise attempt S3 presign -> redirect
    const expiresSec = Number(process.env.PRESIGN_EXPIRES || 3600);
    try {
      const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key });
      const signedUrl = await getSignedUrl(s3Client, getCmd, { expiresIn: expiresSec });
      return res.redirect(302, signedUrl);
    } catch (err) {
      console.warn('S3 presign failed for token', token, err.message || String(err));
      // fallback to localPath if exists
      const filePath = entry.localPath;
      if (filePath && fs.existsSync(filePath)) {
        const suggestedName = entry.originalName || entry.safeOriginal || path.basename(filePath);
        res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
        return res.sendFile(filePath);
      }
      // no local fallback: return 410 with log
      console.error('No local fallback and S3 presign failed for token', token);
      return res.status(410).send('File unavailable');
    }
  } catch (err) {
    console.error('Error serving file for token', token, err);
    return res.status(500).send('Serve error');
  }
});

// admin debug endpoint — remove if you want no public debug
app.get('/_admin/token/:token', (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).json({ ok: false, error: 'not found' });
  return res.json({ ok: true, token, entry });
});

app.get('/health', (req, res) => res.json({ ok: true }));

// global error handler
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  if (err) {
    console.error('Unhandled error:', err);
    return res.status(500).json({ error: 'Server error', details: err.message || String(err) });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
