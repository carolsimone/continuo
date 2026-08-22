import type { ServerMessage } from './chat-protocol';

export type ChatItem =
  | { kind: 'user'; text: string }
  | { kind: 'assistant'; text: string; done: boolean }
  | { kind: 'tool'; command: string }
  | { kind: 'confirm'; actionId: string; summary: string; resolved: boolean | null }
  | { kind: 'error'; message: string };

export interface ChatState {
  items: ChatItem[];
  threadId: string | null;
  pendingConfirm: string | null;
  streaming: boolean;
}

export const initialChatState: ChatState = { items: [], threadId: null, pendingConfirm: null, streaming: false };

export function appendUserText(state: ChatState, text: string): ChatState {
  return { ...state, streaming: true, items: [...state.items, { kind: 'user', text }] };
}

export function resolveConfirm(state: ChatState, actionId: string, approved: boolean): ChatState {
  return {
    ...state,
    pendingConfirm: null,
    items: state.items.map((it) =>
      it.kind === 'confirm' && it.actionId === actionId ? { ...it, resolved: approved } : it,
    ),
  };
}

export function applyServerMessage(state: ChatState, msg: ServerMessage): ChatState {
  switch (msg.type) {
    case 'thread':
      return { ...state, threadId: msg.threadId };
    case 'history': {
      const items: ChatItem[] = msg.messages.map((m) =>
        m.role === 'user'
          ? { kind: 'user', text: m.text ?? '' }
          : m.role === 'tool'
            ? { kind: 'tool', command: m.command ?? '' }
            : { kind: 'assistant', text: m.text ?? '', done: true },
      );
      return { ...state, items, streaming: false, pendingConfirm: null };
    }
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
      return { ...state, streaming: true, items };
    }
    case 'final': {
      const items = [...state.items];
      const last = items[items.length - 1];
      if (last && last.kind === 'assistant' && !last.done) {
        items[items.length - 1] = { kind: 'assistant', text: msg.text || last.text, done: true };
      } else if (msg.text) {
        items.push({ kind: 'assistant', text: msg.text, done: true });
      }
      return { ...state, streaming: false, items };
    }
    case 'confirm_request':
      return {
        ...state,
        pendingConfirm: msg.actionId,
        items: [...state.items, { kind: 'confirm', actionId: msg.actionId, summary: msg.summary, resolved: null }],
      };
    case 'error': {
      // thread_not_found means the resumed thread is gone or not ours; drop the
      // threadId so the next connection opens a fresh thread instead of retrying
      // the missing one.
      const threadId = msg.code === 'thread_not_found' ? null : state.threadId;
      return { ...state, threadId, streaming: false, pendingConfirm: null, items: [...state.items, { kind: 'error', message: msg.message }] };
    }
    default:
      return state;
  }
}
