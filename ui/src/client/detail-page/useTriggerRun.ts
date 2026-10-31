import { useCallback, useState } from 'react';
import { settlesRequest, type ActionRequestId } from '../request-id';

// The "Trigger run" button's request for a schedule. A press that gets no
// answer and is repeated, while the page still shows the last run the first
// press saw, sends the same request id, so the run the first press made is
// returned rather than made twice. `lastRunObservation` is that last run (see
// Observation in request-id.ts); the id is renewed once the server has given
// its final answer.
export function useTriggerRun(
  name: string | undefined,
  lastRunObservation: string | null | undefined,
  requestId: Pick<ActionRequestId, 'take' | 'renew'>,
): {
  triggerState: 'idle' | 'loading' | 'success' | 'error';
  triggerError: string | null;
  trigger: () => Promise<void>;
} {
  const { take: takeRequestId, renew: renewRequestId } = requestId;
  const [triggerState, setTriggerState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);

  const trigger = useCallback(async () => {
    if (!name) return;
    setTriggerState('loading');
    setTriggerError(null);
    try {
      const id = takeRequestId(name, lastRunObservation);
      const res = await fetch(`/api/schedules/${encodeURIComponent(name)}/trigger`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: id }),
      });
      // The page keeps one request id across schedules: an answer that lands
      // after another schedule's press took over renews nothing.
      if (settlesRequest(res)) renewRequestId(id);
      if (res.ok) {
        setTriggerState('success');
        setTimeout(() => setTriggerState('idle'), 3000);
      } else {
        const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setTriggerError(body.error ?? 'Request failed — please try again');
        setTriggerState('error');
      }
    } catch {
      setTriggerError('Request failed — please try again');
      setTriggerState('error');
    }
  }, [name, lastRunObservation, takeRequestId, renewRequestId]);

  return { triggerState, triggerError, trigger };
}
