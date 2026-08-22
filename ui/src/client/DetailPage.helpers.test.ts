import { describe, it, expect } from 'vitest';
import { isTerminalStatus, isRerunnableStatus, pillClass } from './DetailPage';

describe('isTerminalStatus', () => {
  it('treats skipped as terminal', () => {
    expect(isTerminalStatus('skipped')).toBe(true);
  });

  it('treats running as non-terminal', () => {
    expect(isTerminalStatus('running')).toBe(false);
  });
});

describe('pillClass', () => {
  it('maps skipped to its own pill class', () => {
    expect(pillClass('skipped')).toBe('pill--skipped');
  });
});

describe('isRerunnableStatus', () => {
  it('treats failed as rerunnable', () => {
    expect(isRerunnableStatus('failed')).toBe(true);
  });

  it('treats cancelled as rerunnable', () => {
    expect(isRerunnableStatus('cancelled')).toBe(true);
  });

  it('treats skipped as not rerunnable', () => {
    expect(isRerunnableStatus('skipped')).toBe(false);
  });

  it('treats succeeded as not rerunnable', () => {
    expect(isRerunnableStatus('succeeded')).toBe(false);
  });
});
