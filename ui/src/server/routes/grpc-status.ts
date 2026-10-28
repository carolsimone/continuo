import * as grpc from '@grpc/grpc-js';
import type { Response } from 'express';
import { isMaintenanceRefusal, sendMaintenance } from '../maintenance';

// ABORTED_RETRY_AFTER_SECONDS is the Retry-After of a command whose request
// id is held by concurrent calls that keep rolling back: they end within a
// transaction's time, so the same request id may be sent again at once.
const ABORTED_RETRY_AFTER_SECONDS = 1;

// Maps a gRPC status code from a backend service onto the HTTP status the
// browser should see. ABORTED, a command whose request id concurrent calls
// kept rolling back, is 503: the browser keeps a request id open on a 5xx, so
// the retry of the action sends the same id. An absent or unrecognised code is
// a server-side fault.
export function grpcToHttpStatus(code: number | undefined): number {
  switch (code) {
    case grpc.status.INVALID_ARGUMENT:
      return 400;
    case grpc.status.NOT_FOUND:
      return 404;
    case grpc.status.FAILED_PRECONDITION:
    case grpc.status.ALREADY_EXISTS:
      return 409;
    case grpc.status.ABORTED:
      return 503;
    default:
      return 500;
  }
}

// sendGrpcError answers a failed backend call: a maintenance refusal becomes
// 503 maintenance, ABORTED a 503 with a Retry-After, and anything else keeps
// its mapped status, with message as its error (the error's own message
// unless the caller names another).
export function sendGrpcError(res: Response, err: any, message: string | undefined = err?.message): void {
  if (isMaintenanceRefusal(err)) {
    sendMaintenance(res);
    return;
  }
  if (err?.code === grpc.status.ABORTED) {
    res.set('Retry-After', String(ABORTED_RETRY_AFTER_SECONDS));
  }
  res.status(grpcToHttpStatus(err?.code)).json({ error: message });
}
