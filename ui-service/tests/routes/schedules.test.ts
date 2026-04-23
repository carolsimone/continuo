import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createSchedulesRouter } from '../../src/server/routes/schedules';

const mockTriggerSchedule = vi.fn();
const mockListAllSchedules = vi.fn();
const mockCancelSchedule = vi.fn();

const mockStateClient = {
  triggerSchedule: mockTriggerSchedule,
  listAllSchedules: mockListAllSchedules,
  cancelSchedule: mockCancelSchedule,
};

// createSchedulesRouter requires a graph client too — stub it
const mockGraphClient = {} as any;

const app = express();
app.use(express.json());
app.use('/api/schedules', createSchedulesRouter(mockStateClient as any, mockGraphClient));

describe('POST /api/schedules/:name/trigger', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 200 with schedule_id on success', async () => {
    mockTriggerSchedule.mockImplementation((_req: any, cb: any) =>
      cb(null, { schedule_id: 'aaa-bbb-ccc' })
    );

    const res = await request(app).post('/api/schedules/hourly/trigger');

    expect(res.status).toBe(200);
    expect(res.body.schedule_id).toBe('aaa-bbb-ccc');
    expect(mockTriggerSchedule).toHaveBeenCalledWith(
      { schedule_name: 'hourly' },
      expect.any(Function)
    );
  });

  it('returns 404 when schedule not in catalog', async () => {
    const err = Object.assign(new Error('schedule "nope" not found in catalog'), {
      code: grpc.status.NOT_FOUND,
    });
    mockTriggerSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/nope/trigger');

    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/not found/);
  });

  it('returns 409 when schedule already has an active run', async () => {
    const err = Object.assign(new Error('schedule "hourly" already has an active run'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockTriggerSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/hourly/trigger');

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/active run/);
  });

  it('returns 500 on unexpected gRPC error', async () => {
    const err = Object.assign(new Error('internal'), { code: grpc.status.INTERNAL });
    mockTriggerSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/hourly/trigger');

    expect(res.status).toBe(500);
  });
});

describe('POST /api/schedules/:name/cancel', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns 200 with schedule_id on success', async () => {
    mockCancelSchedule.mockImplementation((_req: any, cb: any) =>
      cb(null, { schedule_id: 'abc-123' })
    );

    const res = await request(app)
      .post('/api/schedules/my-schedule/cancel')
      .send({ cancelled_by: 'operator', cancellation_reason: 'manual' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ schedule_id: 'abc-123' });
    expect(mockCancelSchedule).toHaveBeenCalledWith(
      {
        schedule_name: 'my-schedule',
        cancelled_by: 'operator',
        cancellation_reason: 'manual',
      },
      expect.any(Function)
    );
  });

  it('returns 409 on FAILED_PRECONDITION (no active run)', async () => {
    const err = Object.assign(new Error('no active run'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockCancelSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/no-run/cancel').send({});

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/no active run/);
  });

  it('returns 404 when schedule not found', async () => {
    const err = Object.assign(new Error('schedule not found'), {
      code: grpc.status.NOT_FOUND,
    });
    mockCancelSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/unknown/cancel').send({});

    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/not found/);
  });

  it('returns 500 on unexpected gRPC error', async () => {
    const err = Object.assign(new Error('internal'), { code: grpc.status.INTERNAL });
    mockCancelSchedule.mockImplementation((_req: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/hourly/cancel').send({});

    expect(res.status).toBe(500);
  });
});
