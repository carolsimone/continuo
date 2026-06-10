import { describe, it, expect } from 'vitest';
import { classifyClaudeLine, encodeUserTurn } from '../../src/server/ws/chat-protocol';

describe('classifyClaudeLine', () => {
  it('maps the init system line to a session message', () => {
    const line = JSON.stringify({ type: 'system', subtype: 'init', session_id: 'abc123' });
    expect(classifyClaudeLine(line)).toEqual([{ type: 'session', sessionId: 'abc123' }]);
  });

  it('maps an assistant tool_use block to a tool message', () => {
    const line = JSON.stringify({
      type: 'assistant',
      message: { content: [{ type: 'tool_use', name: 'Bash', input: { command: 'continuo schedule status daily' } }] },
    });
    expect(classifyClaudeLine(line)).toEqual([{ type: 'tool', command: 'continuo schedule status daily' }]);
  });

  it('maps an assistant text block to a text message', () => {
    const line = JSON.stringify({ type: 'assistant', message: { content: [{ type: 'text', text: 'Looking into it.' }] } });
    expect(classifyClaudeLine(line)).toEqual([{ type: 'text', text: 'Looking into it.' }]);
  });

  it('maps a success result line to a final message', () => {
    const line = JSON.stringify({ type: 'result', subtype: 'success', result: 'All 40 tasks green.', is_error: false });
    expect(classifyClaudeLine(line)).toEqual([{ type: 'final', text: 'All 40 tasks green.' }]);
  });

  it('maps an error result to an error message', () => {
    const line = JSON.stringify({ type: 'result', is_error: true, result: 'boom' });
    expect(classifyClaudeLine(line)).toEqual([{ type: 'error', code: 'agent_failed', message: 'boom' }]);
  });

  it('ignores empty, non-JSON, and unknown lines', () => {
    expect(classifyClaudeLine('')).toEqual([]);
    expect(classifyClaudeLine('not json')).toEqual([]);
    expect(classifyClaudeLine(JSON.stringify({ type: 'user', message: {} }))).toEqual([]);
  });
});

describe('encodeUserTurn', () => {
  it('produces a newline-terminated stream-json user turn', () => {
    const out = encodeUserTurn('hello');
    expect(out.endsWith('\n')).toBe(true);
    expect(JSON.parse(out)).toEqual({
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: 'hello' }] },
    });
  });
});
