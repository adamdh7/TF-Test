export async function getcode({ sock, message }) {
  await sock.sendMessage(message.key.remoteJid, { text: 'Je récupère un nouveau code 8…' });
  await sock.logout();  // déclenche la génération du code 8
}
