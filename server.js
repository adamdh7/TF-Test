// server.js
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const cors = require('cors');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
const MAPPINGS_FILE = path.join(UPLOAD_DIR, 'mappings.json');
const BASE_URL = process.env.BASE_URL || null; // eg: https://example.com

// ensure upload dir exists
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// load existing mappings or create empty
let mappings = {};
try {
  if (fs.existsSync(MAPPINGS_FILE)) {
    mappings = JSON.parse(fs.readFileSync(MAPPINGS_FILE, 'utf8') || '{}');
  }
} catch (err) {
  console.warn('Could not read mappings file, starting fresh.', err);
  mappings = {};
}

// helper: persist mappings (simple read-modify-write)
function saveMappings() {
  try {
    fs.writeFileSync(MAPPINGS_FILE, JSON.stringify(mappings, null, 2), 'utf8');
  } catch (err) {
    console.error('Failed to write mappings file', err);
  }
}

// helper: generate random 8-char token (alphanumeric uppercase)
function genToken(len = 8) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let t = '';
  for (let i = 0; i < len; i++) t += chars[Math.floor(Math.random() * chars.length)];
  return t;
}

function genUniqueToken() {
  let t = genToken(8);
  let tries = 0;
  while (mappings[t] && tries < 50) {
    t = genToken(8);
    tries++;
  }
  // if collision after many tries (extremely unlikely), extend token
  if (mappings[t]) {
    t = genToken(12);
  }
  return t;
}

// sanitize a filename to be URL-safe & filesystem-safe
function safeFileName(name) {
  // preserve extension, replace bad chars with _
  const ext = path.extname(name);
  const base = path.basename(name, ext);
  // remove path separators, non-ascii, etc.
  const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
  const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
  return (safeBase + safeExt) || 'file';
}

// multer storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const safeName = safeFileName(file.originalname);
    const filename = `${Date.now()}_${safeName}`;
    cb(null, filename);
  }
});

// limit 5 GB
const upload = multer({
  storage,
  limits: { fileSize: 5368709120 } // 5 * 1024 * 1024 * 1024 = 5368709120 bytes
});

const app = express();
app.use(cors());
app.use(express.json());

// serve static client (public folder)
app.use(express.static(path.join(__dirname, 'public')));

// serve raw uploads as fallback under /files (not the main share URL)
app.use('/files', express.static(UPLOAD_DIR, {
  // optionally set cacheControl or headers here
}));

// Upload endpoint
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  const filename = req.file.filename; // actual stored filename on disk
  const originalName = req.file.originalname;
  const safeOriginal = safeFileName(originalName); // used in URL
  const info = {
    filename,            // stored name on disk
    originalName,        // original upload name
    safeOriginal,        // sanitized name used in public URL
    size: req.file.size,
    mime: req.file.mimetype,
    createdAt: new Date().toISOString()
  };

  // generate token and map it
  const token = genUniqueToken();
  // store under token
  mappings[token] = info;
  saveMappings();

  // build share URL: /TF-<TOKEN>/<safeOriginalName>
  const sharePath = `/TF-${token}/${encodeURIComponent(info.safeOriginal)}`;
  const fileUrl = BASE_URL
    ? `${BASE_URL.replace(/\/+$/, '')}${sharePath}`
    : `${req.protocol}://${req.get('host')}${sharePath}`;

  // return helpful info including domain + path + filename
  res.json({
    filename: info.filename,
    originalName: info.originalName,
    token,
    url: fileUrl,
    sharePath
  });
});

// Serve file by token: GET /TF-:token OR GET /TF-:token/:name
// Accepts both /TF-ABCDEFGH and /TF-ABCDEFGH/filename.ext
app.get(['/TF-:token', '/TF-:token/:name'], (req, res) => {
  const token = req.params.token;
  const entry = mappings[token];
  if (!entry) return res.status(404).send('Not found');

  const filePath = path.join(UPLOAD_DIR, entry.filename);
  if (!fs.existsSync(filePath)) return res.status(410).send('File removed');

  // set Content-Disposition with original filename to suggest proper name on download
  const suggestedName = entry.originalName || entry.safeOriginal || entry.filename;
  res.setHeader('Content-Disposition', `inline; filename="${suggestedName.replace(/"/g, '')}"`);

  res.sendFile(filePath, (err) => {
    if (err) {
      console.error('Error sending file', err);
      if (!res.headersSent) res.status(500).send('Server error');
    }
  });
});

// health
app.get('/health', (req, res) => res.json({ ok: true }));

// multer error handler (size limit etc.)
app.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'File too large. Max 5 GB allowed.' });
  }
  // other multer errors
  if (err) {
    console.error('Upload error', err);
    return res.status(500).json({ error: 'Upload failed', details: err.message || err.toString() });
  }
  next();
});

app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
