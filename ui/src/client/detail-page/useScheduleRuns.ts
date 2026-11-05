import { useState } from 'react';
import type { RunSummary } from '../types';
import { useRetriedRead } from './useRetriedRead';

// Past runs shown per page; also bounds the height of the run list.
export const RUNS_PAGE_SIZE = 10;

// One page of the schedule's past runs, newest first. `runsTotal` is the
// schedule's full run count (null until the first page arrives, and while the
// answer carries none); `runsCount` falls back to the rows read. A read that
// fails keeps the page already shown and is made again until it lands.
export function useScheduleRuns(name: string | undefined): {
  runs: RunSummary[];
  runsTotal: number | null;
  runsPage: number;
  setRunsPage: (page: number) => void;
  runsCount: number;
  readFailed: boolean;
} {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [runsPage, setRunsPage] = useState(0);
  const [runsTotal, setRunsTotal] = useState<number | null>(null);

  const readFailed = useRetriedRead<{ runs: RunSummary[]; total_count?: number }>(
    name ? `/api/schedules/${name}/runs?limit=${RUNS_PAGE_SIZE}&offset=${runsPage * RUNS_PAGE_SIZE}` : null,
    (data) => {
      setRuns(data.runs || []);
      setRunsTotal(typeof data.total_count === 'number' ? data.total_count : null);
    },
  );

  return { runs, runsTotal, runsPage, setRunsPage, runsCount: runsTotal ?? runs.length, readFailed };
}
