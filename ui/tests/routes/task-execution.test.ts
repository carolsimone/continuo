import request from 'supertest';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import express from 'express';

vi.mock('../../src/server/s3', () => ({
  getLogObject: vi.fn(),
}));

import { getLogObject } from '../../src/server/s3';
import { createTaskExecutionRouter } from '../../src/server/routes/task-execution';

const app = express();
app.use('/api/task-execution', createTaskExecutionRouter());

describe('GET /api/task-execution/:id/logs', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('returns 400 when key param is missing', async () => {
    const res = await request(app).get('/api/task-execution/some-id/logs');
    expect(res.status).toBe(400);
  });

  it('streams log content as text/plain', async () => {
    (getLogObject as any).mockResolvedValue('FATAL: dbt model failed\nline 2');
    const key = encodeURIComponent('logs/task-executions/svc/sc/tbl/uuid.log');
    const res = await request(app).get(`/api/task-execution/some-id/logs?key=${key}`);
    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toMatch(/text\/plain/);
    expect(res.text).toContain('FATAL: dbt model failed');
    expect(getLogObject).toHaveBeenCalledWith('logs/task-executions/svc/sc/tbl/uuid.log');
  });

  it('returns 502 when S3 fetch fails', async () => {
    (getLogObject as any).mockRejectedValue(new Error('NoSuchKey'));
    const res = await request(app).get('/api/task-execution/id/logs?key=some/key.log');
    expect(res.status).toBe(502);
  });
});
