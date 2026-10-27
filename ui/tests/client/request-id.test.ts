// @vitest-environment jsdom
import { StrictMode, createElement, type ReactNode } from 'react';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { REQUEST_ID_IN_DOUBT_MS, newRequestId, settlesRequest, useActionRequestId } from '../../src/client/request-id';

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
    it('requestId: a quick press after an unanswered one is a retry and gets the same id', () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily');

      vi.advanceTimersByTime(REQUEST_ID_IN_DOUBT_MS - 1);

      expect(result.current.take('daily')).toBe(first);
    });

    it('requestId: a press after the in-doubt window starts a new action with a new id', () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('daily');

      vi.advanceTimersByTime(REQUEST_ID_IN_DOUBT_MS + 1);
      const later = result.current.take('daily');

      expect(later).toMatch(UUID);
      expect(later).not.toBe(first);
      // The new id is itself retryable from then on.
      expect(result.current.take('daily')).toBe(later);
    });

    it('requestId: a press that asks for something else gets a new id', () => {
      const { result } = renderHook(() => useActionRequestId());
      const first = result.current.take('run');

      expect(result.current.take('build')).not.toBe(first);
    });

    it('requestId: an id that was never sent does not expire, and renew restarts the window', () => {
      vi.useFakeTimers({ toFake: ['Date'] });
      const { result } = renderHook(() => useActionRequestId());
      const minted = result.current.id;

      vi.advanceTimersByTime(10 * REQUEST_ID_IN_DOUBT_MS);
      expect(result.current.take('a')).toBe(minted);

      act(() => result.current.renew());
      const renewed = result.current.take('a');
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
