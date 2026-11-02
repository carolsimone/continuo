import { readSecret } from '../secrets';

// Sets the Redis username from REDIS_USERNAME and the password from
// REDIS_PASSWORD or REDIS_PASSWORD_FILE onto the options a client reads when
// it opens its next connection. Calling it before each reconnect makes the
// client authenticate with the password the file holds then.
export function refreshRedisCredentials(
  client: { options: { username?: string; password?: string } },
  env: NodeJS.ProcessEnv,
): void {
  client.options.username = env.REDIS_USERNAME || undefined;
  client.options.password = readSecret(env, 'REDIS_PASSWORD');
}
