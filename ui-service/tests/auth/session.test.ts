import { describe, it, expect } from 'vitest';
import { SessionStore } from '../../src/server/auth/session';
import { FakeRedis } from './fake-redis';
import type { AuthUser } from '../../src/server/auth/types';

const user: AuthUser = { userId: 'idp|u1', email: 'a@b.com', name: 'A', role: 'viewer' };

const HOUR = 3600;

function makeStore(idle = 8 * HOUR, max = 24 * HOUR) {
  let t = 1_000_000_000_000; // fixed epoch start
  const now = () => t;
  const redis = new FakeRedis(now);
  const store = new SessionStore(redis, idle, max, now);
  return { store, redis, advance: (seconds: number) => { t += seconds * 1000; } };
}

describe('SessionStore', () => {
  it('create returns an unguessable id and load returns the record', async () => {
    const { store } = makeStore();
    const id = await store.create(user);
    expect(id.length).toBeGreaterThanOrEqual(40); // 32 random bytes, base64url
    const record = await store.load(id);
    expect(record).toMatchObject(user);
  });

  it('stores under the uisession: prefix', async () => {
    const { store, redis } = makeStore();
    const id = await store.create(user);
    expect(redis.keys()).toEqual([`uisession:${id}`]);
  });

  it('load touches the idle window: a session stays alive under regular use', async () => {
    const { store, advance } = makeStore();
    const id = await store.create(user);
    for (let i = 0; i < 3; i++) {
      advance(7 * HOUR); // each gap below the 8h idle TTL
      expect(await store.load(id)).not.toBeNull();
    }
  });

  it('expires after the idle TTL with no activity', async () => {
    const { store, advance } = makeStore();
    const id = await store.create(user);
    advance(9 * HOUR);
    expect(await store.load(id)).toBeNull();
  });

  it('never extends past the absolute cap, even with constant activity', async () => {
    const { store, advance } = makeStore();
    const id = await store.create(user);
    for (let i = 0; i < 23; i++) {
      advance(1 * HOUR);
      await store.load(id);
    }
    advance(2 * HOUR); // total age now 25h > 24h cap
    expect(await store.load(id)).toBeNull();
  });

  it('destroy revokes immediately', async () => {
    const { store } = makeStore();
    const id = await store.create(user);
    await store.destroy(id);
    expect(await store.load(id)).toBeNull();
  });

  it('load of a missing or empty id is null', async () => {
    const { store } = makeStore();
    expect(await store.load('')).toBeNull();
    expect(await store.load('nope')).toBeNull();
  });

  it('treats a corrupt session record as no session and deletes it', async () => {
    const { store, redis } = makeStore();
    await redis.set('uisession:bad-json', 'not json', 'EX', 3600);
    await redis.set('uisession:not-object', '42', 'EX', 3600);
    expect(await store.load('bad-json')).toBeNull();
    expect(await store.load('not-object')).toBeNull();
    expect(redis.keys()).toEqual([]);
  });
});
