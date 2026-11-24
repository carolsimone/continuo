import { describe, it, expect } from 'vitest';
import { isTerminalStatus, isRerunnableStatus, pillClass, runActions } from './detail-page-helpers';

describe('isTerminalStatus', () => {
  it('treats skipped as terminal', () => {
    expect(isTerminalStatus('skipped')).toBe(true);
  });

  it('treats running as non-terminal', () => {
    expect(isTerminalStatus('running')).toBe(false);
  });

  it('treats a cancelling run as non-terminal: it is still being stopped', () => {
    expect(isTerminalStatus('cancelling')).toBe(false);
    expect(isTerminalStatus('cancelled')).toBe(true);
  });
});

describe('pillClass', () => {
  it('maps skipped to its own pill class', () => {
    expect(pillClass('skipped')).toBe('pill--skipped');
  });

  it('maps cancelling to its own pill class, apart from cancelled', () => {
    expect(pillClass('cancelling')).toBe('pill--cancelling');
    expect(pillClass('cancelled')).toBe('pill--cancelled');
  });
});

describe('isRerunnableStatus', () => {
  it('treats failed as rerunnable', () => {
    expect(isRerunnableStatus('failed')).toBe(true);
  });

  it('treats cancelled as rerunnable', () => {
    expect(isRerunnableStatus('cancelled')).toBe(true);
  });

  it('treats a cancelling run as not rerunnable: it has not ended', () => {
    expect(isRerunnableStatus('cancelling')).toBe(false);
  });

  it('treats skipped as not rerunnable', () => {
    expect(isRerunnableStatus('skipped')).toBe(false);
  });

  it('treats succeeded as not rerunnable', () => {
    expect(isRerunnableStatus('succeeded')).toBe(false);
  });
});

describe('runActions', () => {
  const failed = { run_id: 'r2', status: 'failed' as const };
  const base = { run: failed, lastRunId: 'r2', liveRunExists: false, driftState: 'stale' as const };

  it('enables both for the latest run that failed on an older release', () => {
    expect(runActions(base)).toEqual({
      rerun: { enabled: true, reason: null },
      rebase: { enabled: true, reason: null },
    });
  });

  it('enables both for the latest run that was cancelled', () => {
    const a = runActions({ ...base, run: { run_id: 'r2', status: 'cancelled' } });
    expect(a.rerun.enabled).toBe(true);
    expect(a.rebase.enabled).toBe(true);
  });

  it('enables Rebase when the run\'s release is unknown', () => {
    expect(runActions({ ...base, driftState: 'unknown' }).rebase).toEqual({ enabled: true, reason: null });
  });

  it('greys out Rebase only, for a run already on the live release', () => {
    const a = runActions({ ...base, driftState: 'fresh' });
    expect(a.rerun).toEqual({ enabled: true, reason: null });
    expect(a.rebase).toEqual({ enabled: false, reason: 'This run is already on the live release' });
  });

  it('greys out both for a run where every node succeeded', () => {
    const a = runActions({ ...base, run: { run_id: 'r2', status: 'succeeded' } });
    expect(a.rerun).toEqual({ enabled: false, reason: 'Nothing to rerun: every node of this run succeeded' });
    expect(a.rebase).toEqual({ enabled: false, reason: 'Nothing to rebase: every node of this run succeeded' });
  });

  it('greys out both for a skipped run', () => {
    const a = runActions({ ...base, run: { run_id: 'r2', status: 'skipped' } });
    expect(a.rerun).toEqual({ enabled: false, reason: 'Nothing to rerun: this run was skipped' });
    expect(a.rebase).toEqual({ enabled: false, reason: 'Nothing to rebase: this run was skipped' });
  });

  it('greys out both while a run is active, whatever run is on screen', () => {
    const a = runActions({ ...base, run: { run_id: 'r1', status: 'failed' }, liveRunExists: true });
    expect(a.rerun).toEqual({ enabled: false, reason: 'A run is already active' });
    expect(a.rebase).toEqual({ enabled: false, reason: 'A run is already active' });
  });

  it('greys out both for a failed run a newer run came after', () => {
    const a = runActions({ ...base, run: { run_id: 'r1', status: 'failed' } });
    expect(a.rerun).toEqual({ enabled: false, reason: 'A newer run has ended. Rerun only the latest run' });
    expect(a.rebase).toEqual({ enabled: false, reason: 'A newer run has ended. Rebase only the latest run' });
  });

  it('greys out both for a schedule that never ran', () => {
    const a = runActions({ ...base, run: null, lastRunId: null });
    expect(a.rerun).toEqual({ enabled: false, reason: 'This schedule has not run yet' });
    expect(a.rebase).toEqual({ enabled: false, reason: 'This schedule has not run yet' });
  });

  it('greys out both with no reason while the latest run is still being found', () => {
    const a = runActions({ ...base, run: null, lastRunId: undefined });
    expect(a.rerun).toEqual({ enabled: false, reason: null });
    expect(a.rebase).toEqual({ enabled: false, reason: null });
  });

  it('greys out both with no reason while the run is still being read', () => {
    const a = runActions({ ...base, run: { run_id: 'r2', status: null } });
    expect(a.rerun).toEqual({ enabled: false, reason: null });
    expect(a.rebase).toEqual({ enabled: false, reason: null });
  });
});
