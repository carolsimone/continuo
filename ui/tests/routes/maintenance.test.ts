import { describe, it, expect } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import {
  MAINTENANCE_CODE, MAINTENANCE_MESSAGE, MAINTENANCE_TRAILER_KEY,
  loadMaintenance, maintenanceGate, isMaintenanceRefusal,
} from '../../src/server/maintenance';
import { sendGrpcError } from '../../src/server/routes/grpc-status';

describe('loadMaintenance', () => {
  it('accepts exactly "true" and "false"', () => {
    expect(loadMaintenance({ MAINTENANCE_ENABLED: 'true' })).toBe(true);
    expect(loadMaintenance({ MAINTENANCE_ENABLED: 'false' })).toBe(false);
  });
  it.each([undefined, '', 'True', 'TRUE', '1', 'yes', ' true', 'on'])('refuses %j', (raw) => {
    expect(() => loadMaintenance(raw === undefined ? {} : { MAINTENANCE_ENABLED: raw })).toThrow(/MAINTENANCE_ENABLED/);
  });
});

function gatedApp(enabled: boolean) {
  const app = express();
  app.use('/api', maintenanceGate(enabled));
  app.all('/api/*', (_req, res) => res.status(200).json({ reached: true }));
  return app;
}

const GATED = [
  '/api/schedules/daily/trigger',
  '/api/schedulers/abc/rerun',
  '/api/schedulers/abc/rebase',
  '/api/nodes/svc/sch/tbl/run',
  '/api/v1/releases',
  '/api/releases/r1/retry-remediation',
  '/API/Schedules/daily/TRIGGER',   // Express routes case-insensitively, so the gate must too
  '/api/schedules/daily/trigger/',  // and with a trailing slash
];

describe('maintenanceGate', () => {
  it.each(GATED)('refuses POST %s with 503 maintenance', async (path) => {
    const res = await request(gatedApp(true)).post(path).send({});
    expect(res.status).toBe(503);
    expect(res.headers['retry-after']).toBe('300');
    expect(res.body).toEqual({ error: MAINTENANCE_MESSAGE, code: MAINTENANCE_CODE });
  });
  it.each([
    ['POST', '/api/schedules/daily/cancel'],
    ['POST', '/api/remediation/proposals/p1/pull-request'],
    ['GET', '/api/schedules'],
    ['GET', '/api/v1/releases/r1'],
  ])('lets %s %s through', async (method, path) => {
    const res = await (method === 'GET' ? request(gatedApp(true)).get(path) : request(gatedApp(true)).post(path).send({}));
    expect(res.status).toBe(200);
  });
  it('lets everything through when off', async () => {
    expect((await request(gatedApp(false)).post('/api/schedules/daily/trigger').send({})).status).toBe(200);
  });
});

function grpcErr(code: number, message: string, trailer?: Record<string, string>) {
  const metadata = new grpc.Metadata();
  for (const [k, v] of Object.entries(trailer ?? {})) metadata.set(k, v);
  return Object.assign(new Error(`${code} ${message}`), { code, details: message, metadata });
}

describe('backend refusals', () => {
  it('a gRPC refusal with the maintenance trailer becomes 503 maintenance', async () => {
    const err = grpcErr(grpc.status.UNAVAILABLE, MAINTENANCE_MESSAGE, { [MAINTENANCE_TRAILER_KEY]: 'true' });
    expect(isMaintenanceRefusal(err)).toBe(true);
    const app = express();
    app.post('/x', (_req, res) => sendGrpcError(res, err));
    const res = await request(app).post('/x');
    expect(res.status).toBe(503);
    expect(res.body.code).toBe(MAINTENANCE_CODE);
  });
  it('an UNAVAILABLE without the trailer is an outage and keeps its 500', async () => {
    const err = grpcErr(grpc.status.UNAVAILABLE, 'connection refused');
    expect(isMaintenanceRefusal(err)).toBe(false);
    const app = express();
    app.post('/x', (_req, res) => sendGrpcError(res, err));
    const res = await request(app).post('/x');
    expect(res.status).toBe(500);
    expect(res.body.code).toBeUndefined();
  });
});
