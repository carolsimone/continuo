import * as grpc from '@grpc/grpc-js';

// Maps a gRPC status code from a backend service onto the HTTP status the
// browser should see. An absent or unrecognised code is a server-side fault.
export function grpcToHttpStatus(code: number | undefined): number {
  switch (code) {
    case grpc.status.INVALID_ARGUMENT:
      return 400;
    case grpc.status.NOT_FOUND:
      return 404;
    case grpc.status.FAILED_PRECONDITION:
      return 409;
    default:
      return 500;
  }
}
