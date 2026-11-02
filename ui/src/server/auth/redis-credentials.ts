import { readSecret } from '../secrets';

interface RedisCredentialOptions {
  options: { username?: string; password?: string };
}

// Sets the Redis username from REDIS_USERNAME and the password from
// REDIS_PASSWORD or REDIS_PASSWORD_FILE onto the options a client reads when
// it opens its next connection. Calling it before each reconnect makes the
// client authenticate with the password the file holds then.
export function refreshRedisCredentials(client: RedisCredentialOptions, env: NodeJS.ProcessEnv): void {
  client.options.username = env.REDIS_USERNAME || undefined;
  client.options.password = readSecret(env, 'REDIS_PASSWORD');
}

// Refreshes the client's credentials from env on every 'reconnecting' event,
// so a password file rewritten while the client is disconnected is used by
// the next connection. A file that cannot be read is logged and the client
// keeps the previous password.
export function watchRedisCredentials(
  redis: RedisCredentialOptions & { on(event: 'reconnecting', listener: () => void): unknown },
  env: NodeJS.ProcessEnv,
  log: Pick<Console, 'error'>,
): void {
  redis.on('reconnecting', () => {
    try {
      refreshRedisCredentials(redis, env);
    } catch (err) {
      log.error(`redis: keeping the previous credentials: ${(err as Error).message}`);
    }
  });
}
