// @vitest-environment jsdom
import { StrictMode, createElement, type ReactNode } from 'react';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { newRequestId, settlesRequest, useActionRequestId } from '../../src/client/request-id';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('requestId', () => {
  it('one per action, reused by a retry inside the same dialog', () => {
    const { result, rerender } = renderHook(() => useActionRequestId());
    const first = result.current.id;
    expect(first).toMatch(UUID);

    // A retry re-renders the dialog (submitting, then the error) without a
    // new press: the id it sends again is the one it sent the first time.
    rerender();
    rerender();
    expect(result.current.id).toBe(first);
  });

  it('keeps one id across the double invocation of strict mode', () => {
    const wrapper = ({ children }: { children: ReactNode }) => createElement(StrictMode, null, children);
    const { result, rerender } = renderHook(() => useActionRequestId(), { wrapper });
    const first = result.current.id;
    rerender();
    expect(result.current.id).toBe(first);
  });

  it('a fresh click mints a new one', () => {
    const { result } = renderHook(() => useActionRequestId());
    const first = result.current.id;

    act(() => result.current.renew());

    expect(result.current.id).toMatch(UUID);
    expect(result.current.id).not.toBe(first);
  });

  it('two actions mint two different ids', () => {
    const a = renderHook(() => useActionRequestId());
    const b = renderHook(() => useActionRequestId());
    expect(a.result.current.id).not.toBe(b.result.current.id);
  });

  it('renew is a stable function, so a handler that holds it never goes stale', () => {
    const { result } = renderHook(() => useActionRequestId());
    const renew = result.current.renew;
    act(() => renew());
    expect(result.current.renew).toBe(renew);
  });

  describe('take', () => {
    it('requestId: an unanswered press repeated however long after, with nothing new observed, is a retry with the same id', () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily', 'run-1');

      vi.advanceTimersByTime(120_001);
      expect(result.current.take('daily', 'run-1')).toBe(first);

      vi.advanceTimersByTime(6 * 60 * 60 * 1000);
      expect(result.current.take('daily', 'run-1')).toBe(first);
    });

    it('requestId: a press after the page observes a newer run starts a new action with a new id', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily', 'run-1');

      const later = result.current.take('daily', 'run-2');

      expect(later).toMatch(UUID);
      expect(later).not.toBe(first);
      // The new id is itself retryable from then on.
      expect(result.current.take('daily', 'run-2')).toBe(later);
    });

    it('requestId: a page that has observed no run yet observes one by its first run', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily', null);

      expect(result.current.take('daily', undefined)).toBe(first);
      expect(result.current.take('daily', 'run-1')).not.toBe(first);
    });

    it('requestId: losing sight of the observed run (a failed read, a run gone from the window) is not a newer run', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily', 'r1');

      expect(result.current.take('daily', '')).toBe(first);
      expect(result.current.take('daily', null)).toBe(first);
      expect(result.current.take('daily', undefined)).toBe(first);
      expect(result.current.take('daily', 'r1')).toBe(first);

      const later = result.current.take('daily', 'r2');
      expect(later).not.toBe(first);
    });

    it('requestId: a press made before the page knows what it shows adopts the first run it then reads', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily', undefined);

      // The read lands: what it shows may be the run the press made, so it
      // is not a newer run than the press saw.
      expect(result.current.take('daily', 'run-1')).toBe(first);
      expect(result.current.take('daily', 'run-2')).not.toBe(first);
    });

    it('requestId: a press that asks for something else gets a new id', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('run');

      expect(result.current.take('build')).not.toBe(first);
    });

    it('requestId: the minted id is the one the first press sends, and a press after renew gets a new one', () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { result } = renderHook(() => useActionRequestId());
      const minted = result.current.id;

      vi.advanceTimersByTime(6 * 60 * 60 * 1000);
      expect(result.current.take('a', 'run-1')).toBe(minted);

      // The server's final answer renews the id: the next press, even one with
      // the same parameters and the same observed run, is another action.
      act(() => result.current.renew());
      const renewed = result.current.take('a', 'run-1');
      expect(renewed).not.toBe(minted);
      expect(result.current.id).toBe(renewed);
    });
  });

  it('settlesRequest: a success and a refusal are final, a 5xx leaves the request open', () => {
    expect(settlesRequest({ ok: true, status: 200 })).toBe(true);
    expect(settlesRequest({ ok: false, status: 400 })).toBe(true);
    expect(settlesRequest({ ok: false, status: 409 })).toBe(true);
    expect(settlesRequest({ ok: false, status: 500 })).toBe(false);
    expect(settlesRequest({ ok: false, status: 504 })).toBe(false);
  });

  it('mints a version 4 UUID where crypto.randomUUID is unavailable (a page served over plain http)', () => {
    vi.stubGlobal('crypto', { getRandomValues: globalThis.crypto.getRandomValues.bind(globalThis.crypto) });
    const id = newRequestId();
    expect(id).toMatch(UUID);
    expect(id[14]).toBe('4');
    expect('89ab').toContain(id[19]);
    expect(newRequestId()).not.toBe(id);
  });
});
