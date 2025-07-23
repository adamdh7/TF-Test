export async function getqr({ sock, message }) {
  await sock.sendMessage(message.key.remoteJid, { text: 'Je génère un nouveau QR…' });
  await sock.logout();  // déclenche la régénération dans initSocket
}
