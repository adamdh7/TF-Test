import express from 'express';
import fs from 'fs';
import path from 'path';
import cors from 'cors';

const app = express();
const DATA_FILE = path.resolve('./data.json');

app.use(express.json());
app.use(cors());

let db = { users: [], chats: {} };
if (fs.existsSync(DATA_FILE)) {
  db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
}

function save() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(db, null, 2));
}

app.post('/api/login', (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email requis' });
  if (!db.users.includes(email)) {
    db.users.push(email);
    db.chats[email] = [];
    save();
  }
  res.json({ ok: true });
});

app.get('/api/users', (req, res) => {
  const self = req.query.self;
  res.json(db.users.filter(u => u !== self));
});

app.get('/api/chats/:user', (req, res) => {
  const { user } = req.params;
  res.json(db.chats[user] || []);
});

app.post('/api/chats/:user', (req, res) => {
  const { user } = req.params;
  const { from, text, reply } = req.body;
  if (!from || !text) return res.status(400).json({ error: 'Données manquantes' });

  db.chats[user] = db.chats[user] || [];
  db.chats[user].push({ from, text, reply: reply || null });
  save();
  res.json({ ok: true });
});

// export comme serverless function
export default app;
