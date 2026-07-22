import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import express from 'express';
import { writeFileSync, unlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { createConfigRouter } from '../../src/server/routes/config';

const VALID_CONFIG_PATH = join(tmpdir(), 'test-cancel-config-valid.json');
const BAD_CONFIG_PATH = join(tmpdir(), 'test-cancel-config-bad.json');
const MISSING_FIELDS_CONFIG_PATH = join(tmpdir(), 'test-cancel-config-missing-fields.json');
const STRAY_KEY_CONFIG_PATH = join(tmpdir(), 'test-cancel-config-stray.json');

describe('GET /api/config', () => {
  beforeAll(() => {
    writeFileSync(
      VALID_CONFIG_PATH,
      JSON.stringify({
        cancellation_reasons: ['Incorrect trigger', 'Other'],
      }),
    );
    writeFileSync(BAD_CONFIG_PATH, 'not valid json {{{');
    writeFileSync(MISSING_FIELDS_CONFIG_PATH, JSON.stringify({ unrelated: true }));
    writeFileSync(
      STRAY_KEY_CONFIG_PATH,
      JSON.stringify({
        cancellation_reasons: ['Incorrect trigger', 'Other'],
        cancel_by_emails: ['placeholder1@example.com'],
      }),
    );
  });

  afterAll(() => {
    try { unlinkSync(VALID_CONFIG_PATH); } catch {}
    try { unlinkSync(BAD_CONFIG_PATH); } catch {}
    try { unlinkSync(MISSING_FIELDS_CONFIG_PATH); } catch {}
    try { unlinkSync(STRAY_KEY_CONFIG_PATH); } catch {}
  });
  it('returns the config as JSON when the file exists', async () => {
    const app = express();
    app.use('/api/config', createConfigRouter(VALID_CONFIG_PATH));
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(200);
    expect(res.body.cancellation_reasons).toEqual(['Incorrect trigger', 'Other']);
  });

  it('returns 503 when the config file is missing', async () => {
    const app = express();
    app.use('/api/config', createConfigRouter('/no/such/file.json'));
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/unavailable/i);
  });

  it('returns 503 when the config file contains invalid JSON', async () => {
    const app = express();
    app.use('/api/config', createConfigRouter(BAD_CONFIG_PATH));
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/unavailable/i);
  });

  it('never leaks cancel_by_emails even if present on disk', async () => {
    const app = express();
    app.use('/api/config', createConfigRouter(STRAY_KEY_CONFIG_PATH));
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(200);
    expect(res.body.cancellation_reasons).toEqual(['Incorrect trigger', 'Other']);
    expect(res.body).not.toHaveProperty('cancel_by_emails');
    expect(Object.keys(res.body)).toEqual(['cancellation_reasons']);
  });

  it('returns 503 when required fields are missing from the config', async () => {
    const app = express();
    app.use('/api/config', createConfigRouter(MISSING_FIELDS_CONFIG_PATH));
    const res = await request(app).get('/api/config');
    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/unavailable/i);
  });
});
