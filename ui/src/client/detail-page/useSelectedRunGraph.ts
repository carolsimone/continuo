import { useEffect, useState } from 'react';
import type { RunGraph } from '../types';
import { useRetriedRead } from './useRetriedRead';

// The graph a chosen past run ran on. It is null while that read is in flight
// or failing, and when no run is chosen; a failed read is made again until it
// lands.
export function useSelectedRunGraph(selectedRunId: string | null): {
  runGraph: RunGraph | null;
  readFailed: boolean;
} {
  const [runGraph, setRunGraph] = useState<RunGraph | null>(null);

  useEffect(() => { setRunGraph(null); }, [selectedRunId]);

  const readFailed = useRetriedRead<RunGraph>(
    selectedRunId ? `/api/runs/${selectedRunId}/graph` : null,
    setRunGraph,
  );

  return { runGraph, readFailed };
}
