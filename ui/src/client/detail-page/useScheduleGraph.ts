import { useEffect, useState } from 'react';
import type { DetailMode, GraphState } from '../detail-page-helpers';
import type { ScheduleGraph } from '../types';

// The schedule's catalog graph from the live release, read on arrival and,
// in latest mode, re-read every 5 s so a promotion shows without a reload.
export function useScheduleGraph(
  name: string | undefined,
  mode: DetailMode,
): { graph: ScheduleGraph | null; graphState: GraphState; promotionSeq: number } {
  const [graph, setGraph] = useState<ScheduleGraph | null>(null);
  const [graphState, setGraphState] = useState<GraphState>('loading');
  const [promotionSeq, setPromotionSeq] = useState<number>(0);

  useEffect(() => {
    setGraph(null);
    setGraphState('loading');
  }, [name]);

  useEffect(() => {
    if (!name) return;
    let cancelled = false;

    const fetchGraph = () => {
      fetch(`/api/schedules/${name}/graph`)
        .then((response) => response.json())
        .then((data: ScheduleGraph) => {
          if (cancelled) return;
          setGraph(data);
          setGraphState(data.nodes.length > 0 ? 'ready' : 'empty');
          setPromotionSeq(Number(data.promotion_seq ?? 0));
        })
        .catch(() => {
          if (cancelled) return;
          setGraph(null);
          setGraphState('error');
        });
    };

    fetchGraph();
    if (mode === 'latest') {
      const id = setInterval(fetchGraph, 5000);
      return () => { cancelled = true; clearInterval(id); };
    }
    return () => { cancelled = true; };
  }, [name, mode]);

  return { graph, graphState, promotionSeq };
}
