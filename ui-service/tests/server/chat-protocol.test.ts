import { describe, it, expect } from 'vitest';
import { toClientEvent, fromServerEvent } from '../../src/server/ws/chat-protocol';

describe('toClientEvent', () => {
  it('forwards a user_message with string text', () => {
    expect(toClientEvent({ type: 'user_message', text: 'hi' })).toEqual({ user_message: { text: 'hi' } });
  });

  it('maps new_chat and interrupt', () => {
    expect(toClientEvent({ type: 'new_chat' })).toEqual({ new_chat: {} });
    expect(toClientEvent({ type: 'interrupt' })).toEqual({ interrupt: {} });
  });

  it('forwards a confirm_response only with a real boolean approved', () => {
    expect(toClientEvent({ type: 'confirm_response', actionId: 'a1', approved: true })).toEqual({
      confirm_response: { action_id: 'a1', approved: true },
    });
    expect(toClientEvent({ type: 'confirm_response', actionId: 'a1', approved: false })).toEqual({
      confirm_response: { action_id: 'a1', approved: false },
    });
  });

  it('drops a confirm_response whose approved is not a boolean (no silent approval)', () => {
    // Boolean("false") === true — coercion would auto-approve a mutating action.
    expect(toClientEvent({ type: 'confirm_response', actionId: 'a1', approved: 'false' as any })).toBeNull();
    expect(toClientEvent({ type: 'confirm_response', actionId: 'a1', approved: 1 as any })).toBeNull();
    expect(toClientEvent({ type: 'confirm_response', actionId: 'a1' } as any)).toBeNull();
  });
});

describe('fromServerEvent', () => {
  it('maps each server event to its ws frame with empty-string safety', () => {
    expect(fromServerEvent({ thread: { thread_id: 't1' } })).toEqual({ type: 'thread', threadId: 't1' });
    expect(fromServerEvent({ tool: { command: 'continuo schedule status daily' } })).toEqual({
      type: 'tool',
      command: 'continuo schedule status daily',
    });
    expect(fromServerEvent({ text: { text: 'hi' } })).toEqual({ type: 'text', text: 'hi' });
    expect(fromServerEvent({ final: {} })).toEqual({ type: 'final', text: '' });
    expect(fromServerEvent({ confirm_request: { action_id: 'a1', summary: 'Run x?' } })).toEqual({
      type: 'confirm_request',
      actionId: 'a1',
      summary: 'Run x?',
    });
    expect(fromServerEvent({ error: { code: 'agent_failed', message: 'boom' } })).toEqual({
      type: 'error',
      code: 'agent_failed',
      message: 'boom',
    });
  });

  it('returns null for an unrecognized event', () => {
    expect(fromServerEvent({})).toBeNull();
  });
});
