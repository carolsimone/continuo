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

  it.each([
    'LOADING Redis is loading the dataset in memory',
    'BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.',
    'MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to no.',
    'TRYAGAIN Multiple keys request during rehashing of slot',
    'CLUSTERDOWN The cluster is down',
    'ERR max number of clients reached',
  ])('waits out a Redis that answers %s, as one it cannot reach', async (reply) => {
    const { clock, timing } = fakeClock();
    const redis = fakeRedis([new ReplyError(reply), new ReplyError('NOPERM No permissions to access a key')]);
    await expect(checkPermissionFence(redis, 'perService', timing)).resolves.toBeUndefined();
    expect(redis.call).toHaveBeenCalledTimes(2);
    expect(clock.sleeps).toEqual([1000]);
  });

  it('gives up on a Redis that stays loading without calling it a fence violation', async () => {
    const { timing } = fakeClock();
    const redis = fakeRedis([new ReplyError('LOADING Redis is loading the dataset in memory')]);
    await expect(checkPermissionFence(redis, 'perService', timing)).rejects.toThrow(
      'check the permissions of redis login continuo_ui: LOADING Redis is loading the dataset in memory',
    );
  });

  it('does not wait out a refused sign-in', async () => {
    const { clock, timing } = fakeClock();
    const redis = fakeRedis([new ReplyError('WRONGPASS invalid username-password pair or user is disabled.')]);
    await expect(checkPermissionFence(redis, 'perService', timing)).rejects.toThrow(/WRONGPASS/);
    expect(clock.sleeps).toEqual([]);
  });
});

// The chart's credentials.redis.mode reaches ui as REDIS_ACL_MODE: a mode the
// chart allows but ui refuses would stop it at boot.
describe('REDIS_ACL_MODES', () => {
  it('match values.schema.json credentials.redis.mode', async () => {
    const { readFileSync } = await import('fs');
    const path = await import('path');
    const { REDIS_ACL_MODES } = await import('../../src/server/auth/redis-fence');
    const schema = JSON.parse(readFileSync(path.resolve(__dirname, '../../../deploy/continuo/values.schema.json'), 'utf8'));
    expect([...REDIS_ACL_MODES].sort()).toEqual([...schema.properties.credentials.properties.redis.properties.mode.enum].sort());
  });
});

// The key the fence probes must lie outside every generated login's key
// patterns, or a correctly configured login would fail its own fence.
describe('the permission probe key', () => {
  it('matches no key pattern of the generated Redis rules', async () => {
    const { readFileSync } = await import('fs');
    const path = await import('path');
    const redis = fakeRedis([new ReplyError('NOPERM No permissions to access a key')]);
    await checkPermissionFence(redis as never, 'perService');
    const key = String(redis.call.mock.calls[0].at(-1));
    expect(key).toMatch(/^continuo-/);
    const rules = JSON.parse(readFileSync(path.resolve(__dirname, '../../../deploy/continuo/files/redis-acl-rules.json'), 'utf8'));
    const glob = (pattern: string) =>
      new RegExp('^' + pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.') + '$');
    for (const login of rules.logins as { username: string; rule: string }[]) {
      for (const token of login.rule.split(' ').filter((t) => t.includes('~'))) {
        const pattern = token.slice(token.indexOf('~') + 1);
        expect(glob(pattern).test(key), `${login.username} ${pattern} covers ${key}`).toBe(false);
      }
    }
  });
});
