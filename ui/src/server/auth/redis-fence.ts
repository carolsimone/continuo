import { ReplyError } from 'ioredis';

// The REDIS_ACL_MODE values. In perService mode each service signs in as its
// own Redis login, whose rule fences it to its own keys; in shared mode every
// service signs in with the same password and nothing is checked.
export const REDIS_ACL_MODES = ['shared', 'perService'] as const;
export type RedisAclMode = (typeof REDIS_ACL_MODES)[number];

// The key the fence asks Redis about. It lies outside every key pattern of the
// generated Redis rules, the ones under the continuo: prefix and the
// unprefixed stream names alike, so a login built from those rules is always
// refused it.
const PERMISSION_PROBE_KEY = 'continuo-permission-probe';

// How long the fence waits for a Redis it cannot reach yet, retrying after a
// delay that starts at INITIAL_DELAY_MS and doubles up to MAX_DELAY_MS.
const STARTUP_TIMEOUT_MS = 300_000;
const INITIAL_DELAY_MS = 1_000;
const MAX_DELAY_MS = 10_000;

// Reply errors that mean the client never signed in, which waiting cannot fix
// and which say nothing about the login's keys.
const SIGN_IN_REFUSED = /^(WRONGPASS|NOAUTH)\b/;

// Reply errors a Redis sends while it cannot serve yet but will without any
// change on the client's side (the list pkg/redis retries): LOADING while it
// replays its AOF, among others. The fence waits them out like a Redis it
// cannot reach.
const NOT_SERVING_YET = /^(LOADING|BUSY|MASTERDOWN|TRYAGAIN|CLUSTERDOWN) |^ERR max number of clients reached/;

interface FenceRedis {
  call(command: string, ...args: string[]): Promise<unknown>;
  options: { username?: string };
}

interface FenceTiming {
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  log: Pick<Console, 'warn'>;
}

const realTiming: FenceTiming = {
  now: () => Date.now(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  log: console,
};

// Makes sure, in perService mode, that the login redis signs in as cannot
// reach keys outside its own. It runs XINFO GROUPS on PERMISSION_PROBE_KEY:
// every generated rule allows that command, so a NOPERM answer can only mean
// the key is outside the login's patterns, and any other answer (the key's
// groups, "no such key", another error reply) rejects naming the login. A
// refused sign-in rejects with Redis's answer. A Redis it cannot reach yet, or
// one that answers NOT_SERVING_YET (LOADING while it replays its AOF), is
// waited out for up to STARTUP_TIMEOUT_MS, after which it rejects with that
// error, not as a fence violation. In shared mode it checks
// nothing; an unknown mode rejects.
export async function checkPermissionFence(
  redis: FenceRedis,
  mode: string,
  timing: FenceTiming = realTiming,
): Promise<void> {
  if (mode === 'shared') return;
  if (mode !== 'perService') {
    throw new Error(`unknown REDIS_ACL_MODE "${mode}": expected shared or perService`);
  }
  const login = redis.options.username || 'default';
  const deadline = timing.now() + STARTUP_TIMEOUT_MS;
  let delay = INITIAL_DELAY_MS;
  for (let attempt = 1; ; attempt++) {
    try {
      await redis.call('XINFO', 'GROUPS', PERMISSION_PROBE_KEY);
    } catch (err) {
      const message = (err as Error).message;
      if (err instanceof ReplyError && !NOT_SERVING_YET.test(message)) {
        if (message.startsWith('NOPERM')) return;
        if (SIGN_IN_REFUSED.test(message)) throw new Error(`redis login ${login} could not sign in: ${message}`);
        break;
      }
      if (timing.now() + delay > deadline) {
        throw new Error(`check the permissions of redis login ${login}: ${message}`);
      }
      timing.log.warn(`redis: not serving yet (attempt ${attempt}), retrying in ${delay / 1000}s: ${message}`);
      await timing.sleep(delay);
      delay = Math.min(delay * 2, MAX_DELAY_MS);
      continue;
    }
    break;
  }
  throw new Error(
    `redis login ${login} can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl`,
  );
}
