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

// The latest press of one node: its token tells it from the node's other
// presses, and state and error are what the node shows.
interface Press {
  token: number;
  state: SingleNodeRunState;
  error: string | null;
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
// The state of a press belongs to the node it was made for. Each node the
// component has shown keeps the state of its latest press, and the component
// returns the state of the node it shows now: idle for a node with no press,
// and the press of a node again once the component comes back to it. The answer
// of a press made for another node only changes the state of that press, and
// does not call onStarted, whose reads are those of the node shown. An answer
// renews only the request id its own press sent, so it never closes the
// request of a press made since for another node.
export function useSingleNodeRun({ service, schema, table }: NodeRef, { operation, observed, onStarted }: Options) {
  const [presses, setPresses] = useState<Readonly<Record<string, Press>>>({});
  const nextToken = useRef(0);
  const { take: takeRequestId, renew: renewRunRequestId } = useActionRequestId();

  // The node the component shows now, which an answer compares with the node
  // its press was made for. Until a change of node commits, the component still
  // shows the node left, and an answer then belongs to it.
  const shownNode = `${service}.${schema}.${table}`;
  const shownNodeRef = useRef(shownNode);
  useLayoutEffect(() => {
    shownNodeRef.current = shownNode;
  }, [shownNode]);

  const shown = presses[shownNode];
  const runState: SingleNodeRunState = shown?.state ?? 'idle';
  const runError = shown?.error ?? null;

  // opOverride pins the operation for a caller that must send exactly what it
  // confirmed, independent of the select's current value. The promise
  // resolves to null once the run started, and to the error shown otherwise.
  const postRun = useCallback(async (body: object, opOverride?: NodeOperation): Promise<string | null> => {
    const pressedFor = shownNode;
    const token = ++nextToken.current;
    const stillShown = () => shownNodeRef.current === pressedFor;
    // Each state change is the press's own: a later press of the node has
    // another token, and the answers and timers of this one leave it alone.
    const settle = (state: SingleNodeRunState, error: string | null) => setPresses(prev => (
      prev[pressedFor]?.token === token ? { ...prev, [pressedFor]: { token, state, error } } : prev
    ));
    setPresses(prev => ({ ...prev, [pressedFor]: { token, state: 'loading', error: null } }));
    try {
      const op = opOverride ?? operation;
      const asked = { ...body, operation: op === 'run' ? '' : op };
      const requestId = takeRequestId(`${service}.${schema}.${table}|${JSON.stringify(asked)}`, observed);
      const res = await fetch(
        `/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/run`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...asked, request_id: requestId }) },
      );
      if (settlesRequest(res)) renewRunRequestId(requestId);
      if (res.ok) {
        settle('success', null);
        if (stillShown()) onStarted?.();
        setTimeout(() => settle('idle', null), 3000);
        return null;
      }
      const errBody = await res.json().catch(() => ({ error: REQUEST_FAILED }));
      const message: string = errBody.error ?? REQUEST_FAILED;
      settle('error', message);
      return message;
    } catch {
      settle('error', REQUEST_FAILED);
      return REQUEST_FAILED;
    }
  }, [shownNode, service, schema, table, onStarted, operation, observed, takeRequestId, renewRunRequestId]);

  return { runState, runError, postRun };
}
