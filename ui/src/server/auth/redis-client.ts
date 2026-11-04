import type { RedisOptions } from 'ioredis';
import { readSecret } from '../secrets';
import type { OidcAuthConfig } from './config';

// The options ui opens its Redis connection with. ioredis's ready check sends
// INFO, which the generated continuo_ui rule does not grant: in perService mode
// the check is off, since every connect would otherwise log a NOPERM and skip
// it anyway. The permission fence then waits out a Redis that answers LOADING
// while it replays its AOF. In shared mode the login may run INFO, so the
// check holds commands back until Redis has loaded its data.
export function redisClientOptions(
  cfg: Pick<OidcAuthConfig, 'redisUsername' | 'redisAclMode'>,
  env: NodeJS.ProcessEnv,
): RedisOptions {
  return {
    username: cfg.redisUsername,
    password: readSecret(env, 'REDIS_PASSWORD'),
    enableReadyCheck: cfg.redisAclMode === 'shared',
  };
}
