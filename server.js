// server.js
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand, GetObjectAclCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads'); // still used for local temp if needed
const MAPPINGS_KEY = process.env.MAPPINGS_KEY || 'mappings.json';
const BUCKET = process.env.S3_BUCKET || process.env.BUCKET; // required for external storage
const BASE_URL = process.env.BASE_URL || null; // optional for building visible links

// S3 / S3-compatible client config via env
// Required env: S3_BUCKET, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
const s3Client = new S3Client({
  region: process.env.S3_REGION || 'auto',
  endpoint: process.env.S3_ENDPOINT || undefined, // for R2 / Spaces / B2 provide custom endpoint
  forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true' || false,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY
  }
});

// Ensure local upload dir exists (still ok to keep for fallback)
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// mappings in memory: token -> info
let mappings = {};

// helper persist/load mappings to/from S3
async function loadMappingsFromS3() {
  if (!BUCKET) {
    console.warn('No S3 bucket configured — running in local-only mode.');
    return;
  }
  try {
    // try to get mappings.json
    const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: MAPPINGS_KEY });
    const res = await s3Client.send(cmd);
    const streamToString = (stream) => new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
    const text = await streamToString(res.Body);
    mappings = JSON.parse(text || '{}');
    console.log('Loaded mappings from S3:', Object.keys(mappings).length);
  } catch (err) {
    console.warn('Could not load mappings from S3 (maybe first run). Starting with empty mappings.', err.message || err.toString());
    mappings = {};
  }
}

async function saveMappingsToS3() {
  if (!BUCKET) {
    console.warn('No S3 bucket configured — not saving mappings to S3.');
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

// Multer memory storage (we will upload directly to S3)
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  limits: { fileSize: 5368709120 } // 5GB
});

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Startup: load mappings from S3 so files remain available after restarts
(async () => {
  await loadMappingsFromS3();
})();

// Upload endpoint -> store file in S3
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const originalName = req.file.originalname;
    const safeOriginal = safeFileName(originalName);
    const token = genUniqueToken();
    const s3Key = `uploads/TF-${token}/${Date.now()}_${safeOriginal}`;

    // put object to S3
    if (!BUCKET) {
      // fallback: store locally (not persistent on Render free)
      const filename = `${Date.now()}_${safeOriginal}`;
      const outPath = path.join(UPLOAD_DIR, filename);
      fs.writeFileSync(outPath, req.file.buffer);
      mappings[token] = {
        filename,
        originalName,
        safeOriginal,
        size: req.file.size,
        mime: req.file.mimetype,
        createdAt: new Date().toISOString(),
        storage: 'local'
      };
      // save local mappings file
      try { fs.writeFileSync(path.join(UPLOAD_DIR, 'mappings.json'), JSON.stringify(mappings, null, 2)); } catch(e){}
    } else {
      const put = new PutObjectCommand({
        Bucket: BUCKET,
        Key: s3Key,
        Body: req.file.buffer,
        ContentType: req.file.mimetype,
        ACL: undefined // do not make public by default
      });
      await s3Client.send(put);

      mappings[token] = {
        s3Key,
        originalName,
        safeOriginal,
        size: req.file.size,
        mime: req.file.mimetype,
        createdAt: new Date().toISOString(),
        storage: 's3'
      };

      // persist mappings to S3
      await saveMappingsToS3();
    }

    const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
    const fileUrl = BASE_URL
      ? `${BASE_URL.replace(/\/+$/, '')}${sharePath}`
      : `${req.protocol}://${req.get('host')}${sharePath}`;

    return res.json({
      token,
      url: fileUrl,
      sharePath,
      info: mappings[token]
    });
  } catch (err) {
    console.error('Upload error', err);
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
    return res.status(500).json({ error: 'Upload failed', details: err.message || String(err) });
  }
});

// Serve file by token: either stream from S3 or redirect to signed URL
app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).send('Not found');

  try {
    if (entry.storage === 'local') {
      const filePath = path.join(UPLOAD_DIR, entry.filename);
      if (!fs.existsSync(filePath)) return res.status(410).send('File removed');
      const suggestedName = entry.originalName || entry.safeOriginal || entry.filename;
      res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);
      return res.sendFile(filePath);
    }

    // entry.storage === 's3' -> generate signed URL and redirect (fast, offloads traffic to S3)
    const expiresSec = Number(process.env.PRESIGN_EXPIRES || 3600); // seconds
    const getCmd = new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key });
    const signedUrl = await getSignedUrl(s3Client, getCmd, { expiresIn: expiresSec });

    // Option A (recommended): redirect client to the signed URL
    return res.redirect(302, signedUrl);

    // Option B: proxy stream through server (uncomment if you prefer server to stream)
    /*
    const s3Stream = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: entry.s3Key }));
    const suggestedName = entry.originalName || entry.safeOriginal;
    res.setHeader('Content-Type', entry.mime || 'application/octet-stream');
    res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g,'')}"`);
    s3Stream.Body.pipe(res);
    */
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
