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
      // approved must be a real boolean: Boolean("false") is true, so coercing a
      // malformed frame would silently approve a mutating action. Drop anything
      // that isn't a literal boolean (the pending action then safely expires).
      return typeof msg.actionId === 'string' && typeof msg.approved === 'boolean'
        ? { confirm_response: { action_id: msg.actionId, approved: msg.approved } }
        : null;
    case 'interrupt':
      return { interrupt: {} };
    default:
      return null;
  }
}

// fromServerEvent maps a gRPC ServerEvent to the ws JSON frame (null = skip).
//
// Dispatch is driven by the proto-loader oneof discriminator (`ev.event`, the
// name of the set field — the loader is configured with `oneofs: true`) rather
// than by field presence. A new ServerEvent variant added to agentchat.proto
// therefore lands in the default branch and is logged, instead of being
// silently dropped with no compile error or test failure.
export function fromServerEvent(ev: any): ServerMessage | null {
  switch (ev?.event) {
    case 'thread':
      return { type: 'thread', threadId: ev.thread.thread_id ?? '' };
    case 'history': {
      const messages = (ev.history.messages ?? []).map((m: any) => ({
        role: m.role,
        ...(m.text != null ? { text: m.text ?? '' } : {}),
        ...(m.command != null ? { command: m.command ?? '' } : {}),
      }));
      return { type: 'history', messages };
    }
    case 'tool':
      return { type: 'tool', command: ev.tool.command ?? '' };
    case 'text':
      return { type: 'text', text: ev.text.text ?? '' };
    case 'final':
      return { type: 'final', text: ev.final.text ?? '' };
    case 'confirm_request':
      return { type: 'confirm_request', actionId: ev.confirm_request.action_id ?? '', summary: ev.confirm_request.summary ?? '' };
    case 'error':
      return { type: 'error', code: ev.error.code, message: ev.error.message };
    default:
      // An absent oneof (event === undefined) is a no-op; a set-but-unhandled
      // variant means agentchat.proto added a ServerEvent the relay does not map
      // yet — log it so the gap is visible rather than silently swallowed.
      if (ev?.event) console.warn(`fromServerEvent: unhandled ServerEvent variant "${ev.event}"`);
      return null;
  }
}
