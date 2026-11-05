import { useEffect, useState } from 'react';
import type { DetailMode, GraphState } from '../detail-page-helpers';
import { readJson } from '../read-json';
import type { ScheduleGraph } from '../types';

// The schedule's catalog graph from the live release, read on arrival and,
// in latest mode, re-read every 5 s so a promotion shows without a reload. A
// read that fails keeps the graph already held and reports 'error'; in run mode
// the read is made again every 5 s until one lands.
export function useScheduleGraph(
  name: string | undefined,
  mode: DetailMode,
): { graph: ScheduleGraph | null; graphState: GraphState; promotionSeq: number } {
  const [graph, setGraph] = useState<ScheduleGraph | null>(null);
  const [graphState, setGraphState] = useState<GraphState>('loading');
  const [promotionSeq, setPromotionSeq] = useState<number>(0);

  useEffect(() => {
    if (!name) return;
    let cancelled = false;
    let timer: number | undefined;

    const fetchGraph = () => {
      readJson<ScheduleGraph>(`/api/schedules/${name}/graph`)
        .then((data) => {
          if (cancelled) return;
          setGraph(data);
          setGraphState(data.nodes.length > 0 ? 'ready' : 'empty');
          setPromotionSeq(Number(data.promotion_seq ?? 0));
          if (mode !== 'latest') window.clearInterval(timer);
        })
        .catch(() => {
          if (!cancelled) setGraphState('error');
        });
    };

    fetchGraph();
    timer = window.setInterval(fetchGraph, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [name, mode]);

  return { graph, graphState, promotionSeq };
}
