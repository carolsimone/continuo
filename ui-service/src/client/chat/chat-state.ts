import type { ServerMessage } from './chat-protocol';

export type ChatItem =
  | { kind: 'user'; text: string }
  | { kind: 'assistant'; text: string; done: boolean }
  | { kind: 'tool'; command: string }
  | { kind: 'error'; message: string };

export interface ChatState {
  items: ChatItem[];
  sessionId: string | null;
}

export const initialChatState: ChatState = { items: [], sessionId: null };

export function appendUserText(state: ChatState, text: string): ChatState {
  return { ...state, items: [...state.items, { kind: 'user', text }] };
}

export function applyServerMessage(state: ChatState, msg: ServerMessage): ChatState {
  switch (msg.type) {
    case 'session':
      return { ...state, sessionId: msg.sessionId };
    case 'tool':
      return { ...state, items: [...state.items, { kind: 'tool', command: msg.command }] };
    case 'text': {
      const items = [...state.items];
      const last = items[items.length - 1];
      if (last && last.kind === 'assistant' && !last.done) {
        items[items.length - 1] = { ...last, text: last.text + msg.text };
      } else {
        items.push({ kind: 'assistant', text: msg.text, done: false });
      }
      return { ...state, items };
    }
    case 'final': {
      const items = [...state.items];
      const last = items[items.length - 1];
      if (last && last.kind === 'assistant' && !last.done) {
        items[items.length - 1] = { kind: 'assistant', text: msg.text, done: true };
      } else {
        items.push({ kind: 'assistant', text: msg.text, done: true });
      }
      return { ...state, items };
    }
    case 'error':
      return { ...state, items: [...state.items, { kind: 'error', message: msg.message }] };
    default:
      return state;
  }
}
