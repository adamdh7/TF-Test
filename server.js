// server.js (modified to persist to upload.json + local cache fallback)
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const UPLOAD_JSON = process.env.UPLOAD_JSON || path.join(__dirname, 'upload.json'); // <-- file the user wanted
const MAPPINGS_KEY = process.env.MAPPINGS_KEY || 'mappings.json';
const BUCKET = process.env.S3_BUCKET || process.env.BUCKET; // optional
const BASE_URL = process.env.BASE_URL || null;

// S3 client
const s3Client = new S3Client({
  region: process.env.S3_REGION || 'auto',
  endpoint: process.env.S3_ENDPOINT || undefined,
  forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true' || false,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY
  }
});

// ensure local upload dir exists
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// in-memory mappings: token -> info
let mappings = {};

// --- Disk persistence functions (upload.json) ---
function loadMappingsFromDisk() {
  try {
    if (fs.existsSync(UPLOAD_JSON)) {
      const text = fs.readFileSync(UPLOAD_JSON, 'utf8');
      mappings = JSON.parse(text || '{}');
      console.log('Loaded mappings from', UPLOAD_JSON, Object.keys(mappings).length);
    } else {
      mappings = {};
    }
  } catch (err) {
    console.warn('Failed loading upload.json:', err.message || err.toString());
    mappings = {};
  }
}
function saveMappingsToDisk() {
  try {
    // atomic-ish write
    const tmp = UPLOAD_JSON + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(mappings, null, 2));
    fs.renameSync(tmp, UPLOAD_JSON);
    // console.log('Saved mappings to', UPLOAD_JSON);
  } catch (err) {
    console.error('Failed saving upload.json', err);
  }
}

// --- S3 persistence (optional) ---
async function loadMappingsFromS3() {
  if (!BUCKET) {
    console.warn('No S3 bucket configured — skipping S3 mappings load.');
    return;
  }
  try {
    const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY });
    const res = await s3Client.send(cmd);
    const streamToString = (stream) => new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
    const text = await streamToString(res.Body);
    const s3Mappings = JSON.parse(text || '{}');
    // merge: prefer local disk mappings (so manual edits or results since last S3 save stay)
    mappings = Object.assign({}, s3Mappings, mappings);
    console.log('Merged mappings from S3:', Object.keys(s3Mappings).length);
  } catch (err) {
    console.warn('Could not load mappings from S3 (maybe first run).', err.message || err.toString());
  }
}
async function saveMappingsToS3() {
  if (!BUCKET) {
    // console.warn('No S3 bucket configured — not saving mappings to S3.');
    return;
  }
  try {
    const body = JSON.stringify(mappings, null, 2);
    const put = new PutObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY, Body: body, ContentType: 'application/json' });
    await s3Client.send(put);
    // console.log('Saved mappings.json to S3');
  } catch (err) {
    console.error('Failed saving mappings to S3', err);
  }
}

// token utils
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i = 0; i < len; i++) t += chars[Math.floor(Math.random() * chars.length)];
  return t;
}
function genUniqueToken() {
  let t = genToken(8);
  let tries = 0;
  while (mappings[t] && tries < 100) {
    t = genToken(8);
    tries++;
  }
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

// Multer memory storage
const storage = multer.memoryStorage();
const upload = multer({ storage, limits: { fileSize: 5368709120 } }); // 5GB

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Startup: load disk mappings first, then attempt to merge with S3 mappings
loadMappingsFromDisk();
(async () => {
  await loadMappingsFromS3();
  // ensure we persist merged result locally
  saveMappingsToDisk();
})();

// Upload endpoint -> store in S3 + local cache + update upload.json
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const originalName = req.file.originalname || 'file';
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();
    const s3Key = `uploads/TF-${token}/${Date.now()}_${safeOriginal}`;

    // ALWAYS save a local cache copy for resilience
    const localFilename = `${Date.now()}_${token}_${safeOriginal}`;
    const localPath = path.join(UPLOAD_DIR, localFilename);
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
      storage: 'local', // may be updated to 's3' if upload succeeds
      localPath // absolute path on server
    };

    if (BUCKET) {
      try {
        const put = new PutObjectCommand({
          Bucket: BUCKET,
          Key: s3Key,
          Body: req.file.buffer,
          ContentType: req.file.mimetype,
          ACL: undefined // don't set public by default; control access via server route
        });
        await s3Client.send(put);
        entry.s3Key = s3Key;
        entry.storage = 's3';
      } catch (err) {
        console.warn('S3 upload failed, keeping local cache only:', err.message || err.toString());
        entry.storage = 'local';
      }
    } else {
      entry.storage = 'local';
    }

    // store mapping and persist to disk (and S3 as optional durable copy)
    mappings[token] = entry;
    saveMappingsToDisk();
    await saveMappingsToS3().catch(e => {/* ignore s3 save errors */});

    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = BASE_URL
      ? `${BASE_URL.replace(/\/+$/, '')}${sharePath}`
      : `${req.protocol}://${req.get('host')}${sharePath}`;

    return res.json({ token, url: fileUrl, sharePath, info: entry });
  } catch (err) {
    console.error('Upload error', err);
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
    return res.status(500).json({ error: 'Upload failed', details: err.message || String(err) });
  }
});

// Serve file by token: try S3 (signed URL) -> fallback to local cache
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).send('Not found');

  try {
    // If it's only local storage or local file exists, serve it
    if (!entry.s3Key || entry.storage === 'local') {
      const filePath = entry.localPath || path.join(UPLOAD_DIR, entry.filename || '');
      if (!filePath || !fs.existsSync(filePath)) return res.status(410).send('File removed or missing');
      const suggestedName = entry.originalName || entry.safeOriginal || path.basename(filePath);
      res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
      return res.sendFile(filePath);
    }

    // entry has s3Key - try to generate signed URL and redirect
    const expiresSec = Number(process.env.PRESIGN_EXPIRES || 3600); // seconds
    try {
      const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key });
      const signedUrl = await getSignedUrl(s3Client, getCmd, { expiresIn: expiresSec });
      return res.redirect(302, signedUrl);
    } catch (err) {
      // fallback to local cached copy if S3 presign or fetch fails
      console.warn('Presign/redirect failed, falling back to local cache for token', token, err.message || err.toString());
      const filePath = entry.localPath;
      if (filePath && fs.existsSync(filePath)) {
        const suggestedName = entry.originalName || entry.safeOriginal || path.basename(filePath);
        res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
        return res.sendFile(filePath);
      }
      // if no local copy, attempt to proxy from S3 (last resort)
      try {
        const s3Stream = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key }));
        const suggestedName = entry.originalName || entry.safeOriginal;
        res.setHeader('Content-Type', entry.mime || 'application/octet-stream');
        res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g,'')}"`);
        return s3Stream.Body.pipe(res);
      } catch (err2) {
        console.error('Error serving file from S3 and no local cache', err2);
        return res.status(500).send('Serve error');
      }
    }
  } catch (err) {
    console.error('Error serving file', err);
    return res.status(500).send('Serve error');
  }
});

// health
app.get('/health', (req, res) => res.json({ ok: true }));

// Error handler (multer size limit)
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  }
  if (err) {
    console.error(err);
    return res.status(500).json({ error: 'Server error', details: err.message || String(err) });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
