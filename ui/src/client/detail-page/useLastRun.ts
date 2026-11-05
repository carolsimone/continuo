import { useCallback, useEffect, useRef, useState, type Dispatch, type SetStateAction } from 'react';
import { isTerminalStatus } from '../detail-page-helpers';
import { readJson } from '../read-json';
import type { RunGraph, ScheduleSummary, Scheduler } from '../types';
import { useRetriedRead } from './useRetriedRead';

const FOLLOW_INTERVAL_MS = 5000;

// The id a dashboard card passes as navigation state: the schedule's last run
// (null when it never ran), or undefined when the page was opened without one.
function hintedLastRunId(locationState: unknown): string | null | undefined {
  if (locationState == null) return undefined;
  return ((locationState as { last_run_id?: string | null } | null)?.last_run_id ?? null);
}

// The schedule's last run, for one schedule's page: its id (undefined until it
// is known, null when the schedule never ran), its status and its graph. The
// dashboard card passes the id as navigation state so the lookup can be
// skipped; without it the id is read from the schedule list. The state is read
// once, when the page arrives at the schedule. A read that fails leaves its
// value as it was and is made again until it lands.
//
// The schedule list is read again every 5 s, and on refreshLastRun, so the page
// moves to a run started after it arrived, from this page or from anywhere
// else. A list that names no run leaves the run as it is. pastRunsVersion rises
// each time a run the page holds starts or ends after its first status, which
// is when the schedule's past runs may have changed.
export function useLastRun(
  name: string | undefined,
  locationState: unknown,
): {
  lastRunId: string | null | undefined;
  scheduler: Scheduler | null;
  setScheduler: Dispatch<SetStateAction<Scheduler | null>>;
  liveRunGraph: RunGraph | null;
  readFailed: boolean;
  refreshLastRun: () => Promise<void>;
  pastRunsVersion: number;
} {
  const [hinted] = useState(() => hintedLastRunId(locationState));
  const [lastRunId, setLastRunId] = useState<string | null | undefined>(hinted);
  const [scheduler, setScheduler] = useState<Scheduler | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);
  const [followFailed, setFollowFailed] = useState(false);
  const [pastRunsVersion, setPastRunsVersion] = useState(0);
  const heldRunId = useRef(lastRunId);
  heldRunId.current = lastRunId;
  const startedReads = useRef(0);
  const newestLanded = useRef(0);
  const seenRunState = useRef<string | null>(null);

  // A read can outlast the 5 s, so reads overlap: an answer older than the
  // newest one that has landed changes nothing.
  const refreshLastRun = useCallback(async () => {
    if (!name) return;
    const read = ++startedReads.current;
    try {
      const data = await readJson<{ schedules: ScheduleSummary[] }>('/api/schedules');
      if (read < newestLanded.current) return;
      newestLanded.current = read;
      setFollowFailed(false);
      const newest = (data.schedules || []).find((schedule) => schedule.schedule_name === name)?.last_run_id;
      if (newest && newest !== heldRunId.current) {
        setLastRunId(newest);
        setScheduler(null);
        setLiveRunGraph(null);
      }
    } catch {
      if (read < newestLanded.current) return;
      newestLanded.current = read;
      setFollowFailed(true);
    }
  }, [name]);

  useEffect(() => {
    const timer = window.setInterval(() => { void refreshLastRun(); }, FOLLOW_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [refreshLastRun]);

  useEffect(() => {
    if (!scheduler) return;
    const runState = `${lastRunId}:${isTerminalStatus(scheduler.status)}`;
    if (seenRunState.current !== null && seenRunState.current !== runState) {
      setPastRunsVersion((version) => version + 1);
    }
    seenRunState.current = runState;
  }, [lastRunId, scheduler]);

  const listFailed = useRetriedRead<{ schedules: ScheduleSummary[] }>(
    name && hinted === undefined ? '/api/schedules' : null,
    (data) => {
      const match = (data.schedules || []).find((schedule) => schedule.schedule_name === name);
      setLastRunId(match?.last_run_id ?? null);
    },
  );
  // A status the page already holds came from a poll, which is newer than this
  // read when this read lands late.
  const statusFailed = useRetriedRead<Scheduler>(
    lastRunId ? `/api/runs/${lastRunId}` : null,
    (data) => setScheduler((held) => held ?? data),
  );
  const graphFailed = useRetriedRead<RunGraph>(lastRunId ? `/api/runs/${lastRunId}/graph` : null, setLiveRunGraph);

  return {
    lastRunId,
    scheduler,
    setScheduler,
    liveRunGraph,
    readFailed: listFailed || statusFailed || graphFailed || followFailed,
    refreshLastRun,
    pastRunsVersion,
  };
}
