import commands from '../commands';
// …
const intent = await analyzeIntent(text);
if (intent.command && commands[intent.command]) {
  return commands[intent.command]({ sock: conn, message: m, args: intent.args });
}
