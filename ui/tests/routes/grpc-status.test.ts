import { describe, it, expect } from 'vitest';
import * as grpc from '@grpc/grpc-js';
import express from 'express';
import request from 'supertest';
import { grpcToHttpStatus, sendGrpcError } from '../../src/server/routes/grpc-status';
import { settlesRequest } from '../../src/client/request-id';

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

  it('maps ALREADY_EXISTS (a request_id reused with different parameters) to 409', () => {
    expect(grpcToHttpStatus(grpc.status.ALREADY_EXISTS)).toBe(409);
  });

  it('maps ABORTED (concurrent calls of one request_id kept rolling back) to 503', () => {
    expect(grpcToHttpStatus(grpc.status.ABORTED)).toBe(503);
  });

  it('leaves an ABORTED request open, so the retry of the action sends the same request_id', () => {
    expect(settlesRequest({ ok: false, status: grpcToHttpStatus(grpc.status.ABORTED) })).toBe(false);
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

describe('sendGrpcError', () => {
  it('answers ABORTED with 503 and a Retry-After, without the maintenance code', async () => {
    const app = express();
    const err = Object.assign(new Error('the request id is claimed by calls that keep rolling back'), { code: grpc.status.ABORTED });
    app.post('/x', (_req, res) => sendGrpcError(res, err));

    const res = await request(app).post('/x');

    expect(res.status).toBe(503);
    expect(res.headers['retry-after']).toBe('1');
    expect(res.body.error).toMatch(/keep rolling back/);
    expect(res.body).not.toHaveProperty('code');
  });
});
