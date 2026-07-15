import { describe, it, expect } from 'vitest';
import { isTerminalStatus, pillClass } from './DetailPage';

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
