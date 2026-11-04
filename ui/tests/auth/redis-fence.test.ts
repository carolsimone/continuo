import { describe, it, expect, vi } from 'vitest';
import { ReplyError } from 'ioredis';
import { checkPermissionFence } from '../../src/server/auth/redis-fence';

const VIOLATION =
  'redis login continuo_ui can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl';

// A client signed in as username whose probe answers each call in turn: a
// value resolves, an Error rejects.
function fakeRedis(answers: unknown[], username = 'continuo_ui') {
  const call = vi.fn(async (..._args: string[]) => {
    const answer = answers.length > 1 ? answers.shift() : answers[0];
    if (answer instanceof Error) throw answer;
    return answer;
  });
  return { call, options: { username } };
}

// A clock that sleeping advances, so the wait for an unreachable Redis runs
// without real time.
function fakeClock() {
  const clock = { now: 0, sleeps: [] as number[] };
  return {
    clock,
    timing: {
      now: () => clock.now,
      sleep: async (ms: number) => {
        clock.sleeps.push(ms);
        clock.now += ms;
      },
      log: { warn: () => {} },
    },
  };
}

describe('checkPermissionFence', () => {
  it('passes when Redis answers the probe with NOPERM', async () => {
    const redis = fakeRedis([new ReplyError("NOPERM No permissions to access a key")]);
    await expect(checkPermissionFence(redis, 'perService')).resolves.toBeUndefined();
    expect(redis.call).toHaveBeenCalledWith('XINFO', 'GROUPS', 'continuo-permission-probe');
  });

  it('fails, naming the login, when the probe succeeds', async () => {
    const redis = fakeRedis([[]]);
    await expect(checkPermissionFence(redis, 'perService')).rejects.toThrow(VIOLATION);
  });

  it('fails when Redis answers that the probe key does not exist', async () => {
    const redis = fakeRedis([new ReplyError('ERR no such key')]);
    await expect(checkPermissionFence(redis, 'perService')).rejects.toThrow(VIOLATION);
  });

  it('names the default login when no username is set', async () => {
    const redis = fakeRedis([[]], '');
    await expect(checkPermissionFence(redis, 'perService')).rejects.toThrow(
      /^redis login default can read keys outside its own/,
    );
  });

  it('checks nothing in shared mode', async () => {
    const redis = fakeRedis([[]]);
    await expect(checkPermissionFence(redis, 'shared')).resolves.toBeUndefined();
    expect(redis.call).not.toHaveBeenCalled();
  });

  it('refuses an unknown mode', async () => {
    const redis = fakeRedis([[]]);
    await expect(checkPermissionFence(redis, 'perservice')).rejects.toThrow('unknown REDIS_ACL_MODE "perservice"');
    expect(redis.call).not.toHaveBeenCalled();
  });

  it('waits out a Redis it cannot reach yet', async () => {
    const { clock, timing } = fakeClock();
    const redis = fakeRedis([
      new Error('connect ECONNREFUSED 10.0.0.1:6379'),
      new Error('Reached the max retries per request limit (which is 20).'),
      new ReplyError('NOPERM No permissions to access a key'),
    ]);
    await expect(checkPermissionFence(redis, 'perService', timing)).resolves.toBeUndefined();
    expect(redis.call).toHaveBeenCalledTimes(3);
    expect(clock.sleeps).toEqual([1000, 2000]);
  });

  it('gives up on an unreachable Redis without calling it a fence violation', async () => {
    const { clock, timing } = fakeClock();
    const redis = fakeRedis([new Error('connect ECONNREFUSED 10.0.0.1:6379')]);
    const err = await checkPermissionFence(redis, 'perService', timing).catch((e: Error) => e);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toBe(
      'check the permissions of redis login continuo_ui: connect ECONNREFUSED 10.0.0.1:6379',
    );
    expect(clock.now).toBeLessThanOrEqual(300_000);
  });

  it('does not wait out a refused sign-in', async () => {
    const { clock, timing } = fakeClock();
    const redis = fakeRedis([new ReplyError('WRONGPASS invalid username-password pair or user is disabled.')]);
    await expect(checkPermissionFence(redis, 'perService', timing)).rejects.toThrow(/WRONGPASS/);
    expect(clock.sleeps).toEqual([]);
  });
});
