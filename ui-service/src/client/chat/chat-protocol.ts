export type ServerMessage =
  | { type: 'session'; sessionId: string }
  | { type: 'tool'; command: string }
  | { type: 'text'; text: string }
  | { type: 'final'; text: string }
  | { type: 'error'; code: string; message: string };

export type ClientMessage =
  | { type: 'user_message'; text: string }
  | { type: 'new_chat' };
