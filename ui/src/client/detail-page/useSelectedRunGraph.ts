import { useEffect, useState } from 'react';
import type { RunGraph } from '../types';

// The graph a chosen past run ran on. It is null while that read is in
// flight, when it failed, and when no run is chosen.
export function useSelectedRunGraph(selectedRunId: string | null): RunGraph | null {
  const [runGraph, setRunGraph] = useState<RunGraph | null>(null);

  useEffect(() => {
    if (!selectedRunId) {
      setRunGraph(null);
      return;
    }

    let cancelled = false;
    setRunGraph(null);

    fetch(`/api/runs/${selectedRunId}/graph`)
      .then((response) => response.json())
      .then((data: RunGraph) => {
        if (!cancelled) setRunGraph(data);
      })
      .catch(() => {
        if (!cancelled) setRunGraph(null);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedRunId]);

  return runGraph;
}
