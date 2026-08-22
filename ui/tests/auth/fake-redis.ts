import type { RedisLike } from '../../src/server/auth/session';

// Minimal in-memory stand-in for the four ioredis calls SessionStore uses.
// Takes an injectable clock so TTL tests control time explicitly.
export class FakeRedis implements RedisLike {
  private store = new Map<string, { value: string; expiresAt: number }>();

  constructor(public now: () => number = Date.now) {}

  private alive(key: string) {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= this.now()) {
      this.store.delete(key);
      return undefined;
    }
    return entry;
  }

  async get(key: string): Promise<string | null> {
    return this.alive(key)?.value ?? null;
  }

  async set(key: string, value: string, _ex: 'EX', seconds: number): Promise<unknown> {
    this.store.set(key, { value, expiresAt: this.now() + seconds * 1000 });
    return 'OK';
  }

  async del(...keys: string[]): Promise<number> {
    let n = 0;
    for (const k of keys) if (this.store.delete(k)) n++;
    return n;
  }

  async expire(key: string, seconds: number): Promise<number> {
    const entry = this.alive(key);
    if (!entry) return 0;
    entry.expiresAt = this.now() + seconds * 1000;
    return 1;
  }

  keys(): string[] {
    return [...this.store.keys()];
  }
}
