import { describe, it, expect } from 'vitest';
import { redisClientOptions } from '../../src/server/auth/redis-client';

describe('redisClientOptions', () => {
  // ioredis's ready check sends INFO, which the generated continuo_ui rule
  // does not grant: in perService mode every connect would log a NOPERM.
  it('turns the ready check off in perService mode', () => {
    const opts = redisClientOptions({ redisUsername: 'continuo_ui', redisAclMode: 'perService' }, {});
    expect(opts.enableReadyCheck).toBe(false);
  });

  // The shared login may run INFO, so the ready check holds commands back
  // while Redis replays its AOF.
  it('keeps the ready check in shared mode', () => {
    const opts = redisClientOptions({ redisUsername: undefined, redisAclMode: 'shared' }, {});
    expect(opts.enableReadyCheck).toBe(true);
  });

  it('signs in with the configured username and REDIS_PASSWORD', () => {
    const opts = redisClientOptions(
      { redisUsername: 'continuo_ui', redisAclMode: 'perService' },
      { REDIS_PASSWORD: 'secret' },
    );
    expect(opts.username).toBe('continuo_ui');
    expect(opts.password).toBe('secret');
  });

  it('grants nothing the generated ui rule lacks: the rule has no +info', async () => {
    const { readFileSync } = await import('fs');
    const path = await import('path');
    const rules = JSON.parse(
      readFileSync(path.join(__dirname, '../../../deploy/continuo/files/redis-acl-rules.json'), 'utf8'),
    ) as { logins: { service: string; rule: string }[] };
    const ui = rules.logins.find((l) => l.service === 'ui');
    expect(ui).toBeDefined();
    expect(ui!.rule.split(' ')).not.toContain('+info');
  });
});
