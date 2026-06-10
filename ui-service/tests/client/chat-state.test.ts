import { describe, it, expect } from 'vitest';
import { initialChatState, appendUserText, applyServerMessage } from '../../src/client/chat/chat-state';

describe('chat-state reducer', () => {
  it('captures the session id', () => {
    const s = applyServerMessage(initialChatState, { type: 'session', sessionId: 'abc' });
    expect(s.sessionId).toBe('abc');
  });

  it('builds a user → tool → assistant transcript and replaces partials on final', () => {
    let s = appendUserText(initialChatState, 'how is daily?');
    s = applyServerMessage(s, { type: 'tool', command: 'continuo schedule status daily' });
    s = applyServerMessage(s, { type: 'text', text: 'partial' });
    s = applyServerMessage(s, { type: 'final', text: 'All 40 tasks green.' });
    expect(s.items).toEqual([
      { kind: 'user', text: 'how is daily?' },
      { kind: 'tool', command: 'continuo schedule status daily' },
      { kind: 'assistant', text: 'All 40 tasks green.', done: true },
    ]);
  });

  it('opens a fresh assistant bubble per turn', () => {
    let s = appendUserText(initialChatState, 'q1');
    s = applyServerMessage(s, { type: 'final', text: 'a1' });
    s = appendUserText(s, 'q2');
    s = applyServerMessage(s, { type: 'final', text: 'a2' });
    expect(s.items.filter((i) => i.kind === 'assistant')).toEqual([
      { kind: 'assistant', text: 'a1', done: true },
      { kind: 'assistant', text: 'a2', done: true },
    ]);
  });

  it('renders an error item', () => {
    const s = applyServerMessage(initialChatState, { type: 'error', code: 'agent_failed', message: 'boom' });
    expect(s.items).toContainEqual({ kind: 'error', message: 'boom' });
  });
});
