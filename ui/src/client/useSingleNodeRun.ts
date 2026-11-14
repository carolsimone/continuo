import { useCallback, useLayoutEffect, useRef, useState } from 'react';
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
  // (see Observation). A page that shows no such run by its id leaves it out:
  // a press then keeps its id until the server has given its final answer.
  observed?: Observation;
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
// server has given its final answer. Without an observation only that final
// answer renews it.
//
// The state of the press belongs to the node the component shows. When the
// component is given another node the state returns to idle, and the answer of
// a press made for the node it left changes nothing it shows: it neither marks
// the new node's press as started or failed nor calls onStarted, whose reads
// are those of the node left. The answer still settles the request id.
export function useSingleNodeRun({ service, schema, table }: NodeRef, { operation, observed, onStarted }: Options) {
  const [runState, setRunState] = useState<SingleNodeRunState>('idle');
  const [runError, setRunError] = useState<string | null>(null);
  const { take: takeRequestId, renew: renewRunRequestId } = useActionRequestId();

  // The node the component shows now, which an answer compares with the node
  // its press was made for. It is set when a change of node commits, before
  // any answer can arrive.
  const shownNode = `${service}.${schema}.${table}`;
  const shownNodeRef = useRef(shownNode);
  useLayoutEffect(() => {
    shownNodeRef.current = shownNode;
    setRunState('idle');
    setRunError(null);
  }, [shownNode]);

  // opOverride pins the operation for a caller that must send exactly what it
  // confirmed, independent of the select's current value. The promise
  // resolves to null once the run started, and to the error shown otherwise.
  const postRun = useCallback(async (body: object, opOverride?: NodeOperation): Promise<string | null> => {
    const pressedFor = shownNode;
    const stillShown = () => shownNodeRef.current === pressedFor;
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
        if (stillShown()) {
          setRunState('success');
          onStarted?.();
          setTimeout(() => { if (stillShown()) setRunState('idle'); }, 3000);
        }
        return null;
      }
      const errBody = await res.json().catch(() => ({ error: REQUEST_FAILED }));
      const message: string = errBody.error ?? REQUEST_FAILED;
      if (stillShown()) {
        setRunError(message);
        setRunState('error');
      }
      return message;
    } catch {
      if (stillShown()) {
        setRunError(REQUEST_FAILED);
        setRunState('error');
      }
      return REQUEST_FAILED;
    }
  }, [shownNode, service, schema, table, onStarted, operation, observed, takeRequestId, renewRunRequestId]);

  return { runState, runError, postRun };
}
