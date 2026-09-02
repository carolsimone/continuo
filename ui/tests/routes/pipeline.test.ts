import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createPipelineRouter } from '../../src/server/routes/pipeline';

describe('pipeline router', () => {
  it('proxies the active run', async () => {
    const client = { getPipeline: vi.fn().mockResolvedValue({ active: { run_id: 'verify-1', run_kind: 'verification', status: 'compiling' } }) };
    const app = express();
    app.use('/api/pipeline', createPipelineRouter(client as any));
    const res = await request(app).get('/api/pipeline');
    expect(res.status).toBe(200);
    expect(res.body.active.run_kind).toBe('verification');
  });
});
