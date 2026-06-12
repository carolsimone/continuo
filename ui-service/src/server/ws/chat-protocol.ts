export type ServerMessage =
  | { type: 'session'; sessionId: string }
  | { type: 'tool'; command: string }
  | { type: 'text'; text: string }
  | { type: 'final'; text: string }
  | { type: 'error'; code: string; message: string };

export type ClientMessage =
  | { type: 'user_message'; text: string }
  | { type: 'new_chat' };

// Intended read-only command surface. In headless `claude -p` mode the allowlist
// does NOT act as a default-deny (tool calls are auto-approved), so this documents
// intent and front-runs any future stricter matching — it is not the enforcement
// boundary. Comma-separated because each pattern contains spaces.
export const ALLOWED_TOOLS = [
  'Bash(continuo schedule status:*)',
  'Bash(continuo schedule list:*)',
  'Bash(continuo schedule graph:*)',
  'Bash(continuo describe:*)',
].join(',');

// The actual enforcement boundary: a deny-list. In headless mode `--disallowedTools`
// is honored (a matching command is denied), so mutating verbs are blocked here.
// This is best-effort confinement for the local skeleton — it does not sandbox
// arbitrary shell, which is why the bridge is gated off outside local development.
export const DISALLOWED_TOOLS = ['Bash(continuo schedule trigger:*)'].join(',');

export const SYSTEM_PROMPT = [
  'You answer questions about continuo schedules for an end user.',
  'Discover the available commands by running "continuo describe".',
  'Use only the continuo CLI to gather facts.',
  'Reply concisely in plain language; never show raw JSON.',
  'Do not run mutating commands such as "continuo schedule trigger".',
].join(' ');

export function classifyClaudeLine(line: string): ServerMessage[] {
  const trimmed = line.trim();
  if (!trimmed) return [];
  let obj: any;
  try {
    obj = JSON.parse(trimmed);
  } catch {
    return [];
  }
  if (!obj || typeof obj !== 'object') return [];

  switch (obj.type) {
    case 'system':
      if (obj.subtype === 'init' && typeof obj.session_id === 'string') {
        return [{ type: 'session', sessionId: obj.session_id }];
      }
      return [];
    case 'assistant': {
      const content = obj.message?.content;
      if (!Array.isArray(content)) return [];
      const out: ServerMessage[] = [];
      for (const block of content) {
        if (block?.type === 'text' && typeof block.text === 'string') {
          out.push({ type: 'text', text: block.text });
        } else if (block?.type === 'tool_use' && typeof block?.input?.command === 'string') {
          out.push({ type: 'tool', command: block.input.command });
        }
      }
      return out;
    }
    case 'result':
      if (obj.is_error) {
        return [{ type: 'error', code: 'agent_failed', message: String(obj.result ?? 'agent error') }];
      }
      return [{ type: 'final', text: obj.result != null ? String(obj.result) : '' }];
    default:
      return [];
  }
}

export function encodeUserTurn(text: string): string {
  return (
    JSON.stringify({ type: 'user', message: { role: 'user', content: [{ type: 'text', text }] } }) + '\n'
  );
}
