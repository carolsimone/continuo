import { useCallback, useState } from 'react';
import { settlesRequest, type ActionRequestId } from '../request-id';

// The "Start run" button's request for a schedule: it starts a manual run. A press that gets no
// answer and is repeated, while the page still shows the last run the first
// press saw, sends the same request id, so the run the first press made is
// returned rather than made twice. `lastRunObservation` is that last run (see
// Observation in request-id.ts); the id is renewed once the server has given
// its final answer. onStarted runs once the server has accepted the press.
export function useManualRun(
  name: string | undefined,
  lastRunObservation: string | null | undefined,
  requestId: Pick<ActionRequestId, 'take' | 'renew'>,
  onStarted?: () => void,
): {
  startState: 'idle' | 'loading' | 'success' | 'error';
  startError: string | null;
  start: () => Promise<void>;
} {
  const { take: takeRequestId, renew: renewRequestId } = requestId;
  const [startState, setStartState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [startError, setStartError] = useState<string | null>(null);

  const start = useCallback(async () => {
    if (!name) return;
    setStartState('loading');
    setStartError(null);
    try {
      const id = takeRequestId(name, lastRunObservation);
      const res = await fetch(`/api/schedules/${encodeURIComponent(name)}/manual-run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: id }),
      });
      // The page keeps one request id across schedules: an answer that lands
      // after another schedule's press took over renews nothing.
      if (settlesRequest(res)) renewRequestId(id);
      if (res.ok) {
        setStartState('success');
        setTimeout(() => setStartState('idle'), 3000);
        onStarted?.();
      } else {
        const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setStartError(body.error ?? 'Request failed — please try again');
        setStartState('error');
      }
    } catch {
      setStartError('Request failed — please try again');
      setStartState('error');
    }
  }, [name, lastRunObservation, takeRequestId, renewRequestId, onStarted]);

  return { startState, startError, start };
}
