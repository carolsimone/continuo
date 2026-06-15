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
