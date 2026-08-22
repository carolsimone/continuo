import { describe, it, expect } from 'vitest';
import request from 'supertest';
import express from 'express';
import { createFeaturesRouter } from '../../src/server/routes/features';

describe('GET /api/features', () => {
  it('reports the chat bridge as enabled when the flag is true', async () => {
    const app = express();
    app.use('/api/features', createFeaturesRouter(true));
    const res = await request(app).get('/api/features');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ chatBridgeEnabled: true });
  });

  it('reports the chat bridge as disabled when the flag is false', async () => {
    const app = express();
    app.use('/api/features', createFeaturesRouter(false));
    const res = await request(app).get('/api/features');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ chatBridgeEnabled: false });
  });
});
