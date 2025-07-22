let messages = [];

export default function handler(req, res) {
  if (req.method === 'GET') {
    res.status(200).json(messages);
  } else if (req.method === 'POST') {
    const { pseudo, text } = req.body;
    if (pseudo && text) {
      messages.push({ pseudo, text });
      if (messages.length > 100) messages.shift(); // Limite mémoire
    }
    res.status(200).json({ success: true });
  } else {
    res.status(405).end(); // Method Not Allowed
  }
}
