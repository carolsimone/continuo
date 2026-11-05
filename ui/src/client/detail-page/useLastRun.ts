import { useState, type Dispatch, type SetStateAction } from 'react';
import type { RunGraph, ScheduleSummary, Scheduler } from '../types';
import { useRetriedRead } from './useRetriedRead';

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
export function useLastRun(
  name: string | undefined,
  locationState: unknown,
): {
  lastRunId: string | null | undefined;
  scheduler: Scheduler | null;
  setScheduler: Dispatch<SetStateAction<Scheduler | null>>;
  liveRunGraph: RunGraph | null;
  readFailed: boolean;
} {
  const [hinted] = useState(() => hintedLastRunId(locationState));
  const [lastRunId, setLastRunId] = useState<string | null | undefined>(hinted);
  const [scheduler, setScheduler] = useState<Scheduler | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);

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
    readFailed: listFailed || statusFailed || graphFailed,
  };
}
