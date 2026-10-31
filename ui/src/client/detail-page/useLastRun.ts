import { useEffect, useState, type Dispatch, type SetStateAction } from 'react';
import type { RunGraph, ScheduleSummary, Scheduler } from '../types';

// The id a dashboard card passes as navigation state: the schedule's last run
// (null when it never ran), or undefined when the page was opened without one.
function hintedLastRunId(locationState: unknown): string | null | undefined {
  if (locationState == null) return undefined;
  return ((locationState as { last_run_id?: string | null } | null)?.last_run_id ?? null);
}

// The schedule's last run, for one schedule's page: its id (undefined while
// it is being resolved, null when the schedule never ran), its status and its
// graph. The dashboard card passes the id as navigation state so the lookup
// can be skipped; without it the id is read from the schedule list. The state
// is read once, when the page arrives at the schedule.
export function useLastRun(
  name: string | undefined,
  locationState: unknown,
): {
  lastRunId: string | null | undefined;
  lastRunReadFailed: boolean;
  lastRunObservation: string | null | undefined;
  scheduler: Scheduler | null;
  setScheduler: Dispatch<SetStateAction<Scheduler | null>>;
  liveRunGraph: RunGraph | null;
} {
  const [hinted] = useState(() => hintedLastRunId(locationState));
  const [lastRunId, setLastRunId] = useState<string | null | undefined>(hinted);
  // Set when the schedule list could not be read on arrival: lastRunId is then
  // null without the page knowing the schedule never ran.
  const [lastRunReadFailed, setLastRunReadFailed] = useState(false);
  const [scheduler, setScheduler] = useState<Scheduler | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);

  useEffect(() => {
    if (!name || hinted !== undefined) return;

    let cancelled = false;
    fetch('/api/schedules')
      .then((response) => response.json())
      .then((data: { schedules: ScheduleSummary[] }) => {
        if (cancelled) return;
        const match = (data.schedules || []).find((schedule) => schedule.schedule_name === name);
        setLastRunId(match?.last_run_id ?? null);
      })
      .catch(() => {
        if (cancelled) return;
        setLastRunId(null);
        setLastRunReadFailed(true);
      });
    return () => { cancelled = true; };
  }, [name]);

  useEffect(() => {
    if (!lastRunId) return;
    let cancelled = false;

    fetch(`/api/runs/${lastRunId}`)
      .then((response) => response.json())
      .then((data: Scheduler) => {
        if (!cancelled) setScheduler(data);
      })
      .catch(() => {
        if (!cancelled) setScheduler(null);
      });

    return () => {
      cancelled = true;
    };
  }, [lastRunId]);

  useEffect(() => {
    if (!lastRunId) return;
    let cancelled = false;

    fetch(`/api/runs/${lastRunId}/graph`)
      .then((response) => response.json())
      .then((data: RunGraph) => {
        if (!cancelled) setLiveRunGraph(data);
      })
      .catch(() => {
        if (!cancelled) setLiveRunGraph(null);
      });

    return () => {
      cancelled = true;
    };
  }, [lastRunId]);

  // undefined while the page does not know the last run: its read is still
  // loading or failed.
  const lastRunObservation = lastRunReadFailed ? undefined : lastRunId;

  return { lastRunId, lastRunReadFailed, lastRunObservation, scheduler, setScheduler, liveRunGraph };
}
