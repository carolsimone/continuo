import { describe, it, expect } from 'vitest';
import * as grpc from '@grpc/grpc-js';
import { grpcToHttpStatus } from '../../src/server/routes/grpc-status';

describe('grpcToHttpStatus', () => {
  it('maps INVALID_ARGUMENT to 400', () => {
    expect(grpcToHttpStatus(grpc.status.INVALID_ARGUMENT)).toBe(400);
  });

  it('maps NOT_FOUND to 404', () => {
    expect(grpcToHttpStatus(grpc.status.NOT_FOUND)).toBe(404);
  });

  it('maps FAILED_PRECONDITION to 409', () => {
    expect(grpcToHttpStatus(grpc.status.FAILED_PRECONDITION)).toBe(409);
  });

  it('maps INTERNAL to 500', () => {
    expect(grpcToHttpStatus(grpc.status.INTERNAL)).toBe(500);
  });

  it('maps an unknown code to 500', () => {
    expect(grpcToHttpStatus(999)).toBe(500);
  });

  it('maps a missing code to 500', () => {
    expect(grpcToHttpStatus(undefined)).toBe(500);
  });
});
