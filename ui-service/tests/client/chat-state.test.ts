import { describe, it, expect } from 'vitest';
import { applyServerMessage, initialChatState, appendUserText, resolveConfirm } from '../../src/client/chat/chat-state';

describe('chat-state reducer', () => {
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

  it('opens a second assistant bubble when text follows a tool, final replacing only the last', () => {
    let s = appendUserText(initialChatState, 'q');
    s = applyServerMessage(s, { type: 'text', text: 'Looking' });
    s = applyServerMessage(s, { type: 'tool', command: 'continuo schedule list' });
    s = applyServerMessage(s, { type: 'text', text: 'Working' });
    s = applyServerMessage(s, { type: 'final', text: 'Done' });
    expect(s.items).toEqual([
      { kind: 'user', text: 'q' },
      { kind: 'assistant', text: 'Looking', done: false },
      { kind: 'tool', command: 'continuo schedule list' },
      { kind: 'assistant', text: 'Done', done: true },
    ]);
  });
});

describe('confirm flow state', () => {
  it('appends a confirm item and blocks input while pending', () => {
    const s = applyServerMessage(initialChatState, {
      type: 'confirm_request', actionId: 'a1', summary: 'Run `continuo schedule trigger daily`?',
    });
    const last = s.items[s.items.length - 1];
    expect(last).toEqual({ kind: 'confirm', actionId: 'a1', summary: 'Run `continuo schedule trigger daily`?', resolved: null });
    expect(s.pendingConfirm).toBe('a1');
  });

  it('resolveConfirm marks the item and unblocks input', () => {
    let s = applyServerMessage(initialChatState, { type: 'confirm_request', actionId: 'a1', summary: 'x' });
    s = resolveConfirm(s, 'a1', true);
    const last = s.items[s.items.length - 1] as any;
    expect(last.resolved).toBe(true);
    expect(s.pendingConfirm).toBeNull();
  });
});

describe('history + thread', () => {
  it('thread replaces session id semantics', () => {
    const s = applyServerMessage(initialChatState, { type: 'thread', threadId: 't-9' });
    expect(s.threadId).toBe('t-9');
  });

  it('history replaces items with replayed transcript', () => {
    const s = applyServerMessage(initialChatState, {
      type: 'history',
      messages: [
        { role: 'user', text: 'q' },
        { role: 'tool', command: 'schedule_status' },
        { role: 'assistant', text: 'a' },
      ],
    });
    expect(s.items).toEqual([
      { kind: 'user', text: 'q' },
      { kind: 'tool', command: 'schedule_status' },
      { kind: 'assistant', text: 'a', done: true },
    ]);
  });
});
