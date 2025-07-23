export async function menu({ sock, message }) {
  const txt = '*Menu:* D→menu Tg→tagall Sipli→kick Sip→del Sipyo→kickall Tm→hidetag Qr→generate qr Sticker→sticker Purge→sim group deleted';
  await sock.sendMessage(message.key.remoteJid, { text: txt });
}
