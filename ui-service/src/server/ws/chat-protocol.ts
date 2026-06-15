export type ServerMessage =
  | { type: 'thread'; threadId: string }
  | { type: 'history'; messages: { role: string; text?: string; command?: string }[] }
  | { type: 'tool'; command: string }
  | { type: 'text'; text: string }
  | { type: 'final'; text: string }
  | { type: 'confirm_request'; actionId: string; summary: string }
  | { type: 'error'; code: string; message: string };

export type ClientMessage =
  | { type: 'user_message'; text: string }
  | { type: 'new_chat' }
  | { type: 'confirm_response'; actionId: string; approved: boolean }
  | { type: 'interrupt' };

// toClientEvent maps a ws JSON frame to a gRPC ClientEvent (null = unknown frame).
export function toClientEvent(msg: ClientMessage): object | null {
  switch (msg.type) {
    case 'user_message':
      return typeof msg.text === 'string' ? { user_message: { text: msg.text } } : null;
    case 'new_chat':
      return { new_chat: {} };
    case 'confirm_response':
      return typeof msg.actionId === 'string'
        ? { confirm_response: { action_id: msg.actionId, approved: Boolean(msg.approved) } }
        : null;
    case 'interrupt':
      return { interrupt: {} };
    default:
      return null;
  }
}

// fromServerEvent maps a gRPC ServerEvent to the ws JSON frame (null = skip).
export function fromServerEvent(ev: any): ServerMessage | null {
  if (ev.thread) return { type: 'thread', threadId: ev.thread.thread_id ?? '' };
  if (ev.history) {
    const messages = (ev.history.messages ?? []).map((m: any) => ({
      role: m.role,
      ...(m.text != null ? { text: m.text ?? '' } : {}),
      ...(m.command != null ? { command: m.command ?? '' } : {}),
    }));
    return { type: 'history', messages };
  }
  if (ev.tool) return { type: 'tool', command: ev.tool.command ?? '' };
  if (ev.text) return { type: 'text', text: ev.text.text ?? '' };
  if (ev.final) return { type: 'final', text: ev.final.text ?? '' };
  if (ev.confirm_request)
    return { type: 'confirm_request', actionId: ev.confirm_request.action_id ?? '', summary: ev.confirm_request.summary ?? '' };
  if (ev.error) return { type: 'error', code: ev.error.code, message: ev.error.message };
  return null;
}
