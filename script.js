const chatDiv = document.getElementById("chat");
const input = document.getElementById("messageInput");
const sendBtn = document.getElementById("sendBtn");

const pseudo = prompt("Entrez votre pseudo (admin ou user)"); // admin or user

function loadMessages() {
  fetch("/api/messages")
    .then(res => res.json())
    .then(data => {
      chatDiv.innerHTML = "";
      data.forEach(msg => {
        const div = document.createElement("div");
        div.textContent = `[${msg.pseudo}] ${msg.text}`;
        chatDiv.appendChild(div);
      });
      chatDiv.scrollTop = chatDiv.scrollHeight;
    });
}

sendBtn.onclick = () => {
  const text = input.value.trim();
  if (!text) return;

  fetch("/api/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ pseudo, text })
  }).then(() => {
    input.value = "";
    loadMessages();
  });
};

setInterval(loadMessages, 1000); // Refresh every 1s
loadMessages();
