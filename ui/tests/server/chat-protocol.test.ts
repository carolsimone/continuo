import { describe, it, expect, vi } from 'vitest';
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
  // Fakes carry the proto-loader oneof discriminator (`event`) alongside the set
  // field, mirroring what @grpc/proto-loader produces with `oneofs: true`.
  it('maps each server event to its ws frame with empty-string safety', () => {
    expect(fromServerEvent({ event: 'thread', thread: { thread_id: 't1' } })).toEqual({ type: 'thread', threadId: 't1' });
    expect(fromServerEvent({ event: 'tool', tool: { command: 'continuo schedule status daily' } })).toEqual({
      type: 'tool',
      command: 'continuo schedule status daily',
    });
    expect(fromServerEvent({ event: 'text', text: { text: 'hi' } })).toEqual({ type: 'text', text: 'hi' });
    expect(fromServerEvent({ event: 'final', final: {} })).toEqual({ type: 'final', text: '' });
    expect(fromServerEvent({ event: 'confirm_request', confirm_request: { action_id: 'a1', summary: 'Run x?' } })).toEqual({
      type: 'confirm_request',
      actionId: 'a1',
      summary: 'Run x?',
    });
    expect(fromServerEvent({ event: 'error', error: { code: 'agent_failed', message: 'boom' } })).toEqual({
      type: 'error',
      code: 'agent_failed',
      message: 'boom',
    });
  });

  it('maps a history event with mixed message kinds', () => {
    expect(
      fromServerEvent({
        event: 'history',
        history: { messages: [{ role: 'user', text: 'q' }, { role: 'tool', command: 'continuo schedule list' }] },
      }),
    ).toEqual({
      type: 'history',
      messages: [{ role: 'user', text: 'q' }, { role: 'tool', command: 'continuo schedule list' }],
    });
  });

  it('dispatches on the oneof discriminator, not field presence', () => {
    // A malformed event carrying two field blobs must resolve by its discriminator.
    // Field-presence dispatch would have returned the wrong (thread) variant.
    expect(
      fromServerEvent({ event: 'text', text: { text: 'hi' }, thread: { thread_id: 'stale' } }),
    ).toEqual({ type: 'text', text: 'hi' });
  });

  it('returns null and warns for an unhandled (future) oneof variant', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    expect(fromServerEvent({ event: 'brand_new', brand_new: {} })).toBeNull();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('brand_new'));
    warn.mockRestore();
  });

  it('returns null for an event with no oneof set', () => {
    expect(fromServerEvent({})).toBeNull();
  });
});
