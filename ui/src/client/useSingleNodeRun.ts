import { useCallback, useState } from 'react';
import { settlesRequest, useActionRequestId, type Observation } from './request-id';
import type { NodeOperation } from '../server/shared/operation';

type SingleNodeRunState = 'idle' | 'loading' | 'success' | 'error';

const REQUEST_FAILED = 'Request failed — please try again';

interface NodeRef {
  service: string;
  schema: string;
  table: string;
}

interface Options {
  // The operation a press asks for unless postRun pins another.
  operation: NodeOperation;
  // The newest run of the node a press could have made, as the page shows it
  // (see Observation).
  observed: Observation;
  // Called once the server has started the run.
  onStarted?: () => void;
}

// useSingleNodeRun asks for a run of one node alone, through
// POST /api/nodes/:service/:schema/:table/run, and holds the state of the
// press: loading while the request is in flight, success for 3 s once the run
// started, error with the message to show when it did not.
//
// The request id is that of one user action: a press that gets no answer and
// is repeated with the same node, operation and body, while the page still
// shows the newest run of the node the first press saw, sends the same id, so
// the run the first press made is returned rather than made twice. A newer run
// on the page makes the next press a new action, and the id is renewed once the
// server has given its final answer.
export function useSingleNodeRun({ service, schema, table }: NodeRef, { operation, observed, onStarted }: Options) {
  const [runState, setRunState] = useState<SingleNodeRunState>('idle');
  const [runError, setRunError] = useState<string | null>(null);
  const { take: takeRequestId, renew: renewRunRequestId } = useActionRequestId();

  // opOverride pins the operation for a caller that must send exactly what it
  // confirmed, independent of the select's current value. The promise
  // resolves to null once the run started, and to the error shown otherwise.
  const postRun = useCallback(async (body: object, opOverride?: NodeOperation): Promise<string | null> => {
    setRunState('loading');
    setRunError(null);
    try {
      const op = opOverride ?? operation;
      const asked = { ...body, operation: op === 'run' ? '' : op };
      const withOp = { ...asked, request_id: takeRequestId(`${service}.${schema}.${table}|${JSON.stringify(asked)}`, observed) };
      const res = await fetch(
        `/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/run`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(withOp) },
      );
      if (settlesRequest(res)) renewRunRequestId();
      if (res.ok) {
        setRunState('success');
        onStarted?.();
        setTimeout(() => setRunState('idle'), 3000);
        return null;
      }
      const errBody = await res.json().catch(() => ({ error: REQUEST_FAILED }));
      const message: string = errBody.error ?? REQUEST_FAILED;
      setRunError(message);
      setRunState('error');
      return message;
    } catch {
      setRunError(REQUEST_FAILED);
      setRunState('error');
      return REQUEST_FAILED;
    }
  }, [service, schema, table, onStarted, operation, observed, takeRequestId, renewRunRequestId]);

  return { runState, runError, postRun };
}
