import { randomBytes } from 'crypto';
import type { AuthUser } from './types';

export interface SessionRecord extends AuthUser {
  createdAt: number; // epoch ms
}

// The subset of ioredis the store needs; tests substitute an in-memory fake.
export interface RedisLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ex: 'EX', seconds: number): Promise<unknown>;
  del(...keys: string[]): Promise<number>;
  expire(key: string, seconds: number): Promise<number>;
}

const KEY_PREFIX = 'uisession:';

// Server-side sessions: the browser cookie holds only the random id, so
// deleting the Redis key revokes the session instantly. The Redis TTL gives a
// sliding idle window; createdAt bounds total lifetime at the absolute cap.
export class SessionStore {
  constructor(
    private readonly redis: RedisLike,
    private readonly idleTtlSeconds: number,
    private readonly maxTtlSeconds: number,
    private readonly now: () => number = Date.now,
  ) {}

  private ttlFor(createdAt: number): number {
    const ageSeconds = Math.floor((this.now() - createdAt) / 1000);
    return Math.min(this.idleTtlSeconds, this.maxTtlSeconds - ageSeconds);
  }

  async create(user: AuthUser): Promise<string> {
    const id = randomBytes(32).toString('base64url');
    const record: SessionRecord = { ...user, createdAt: this.now() };
    await this.redis.set(KEY_PREFIX + id, JSON.stringify(record), 'EX', this.ttlFor(record.createdAt));
    return id;
  }

  async load(id: string): Promise<SessionRecord | null> {
    if (!id) return null;
    const raw = await this.redis.get(KEY_PREFIX + id);
    if (!raw) return null;
    let record: SessionRecord;
    try {
      const parsed: unknown = JSON.parse(raw);
      if (typeof parsed !== 'object' || parsed === null) throw new Error('not an object');
      record = parsed as SessionRecord;
    } catch {
      await this.redis.del(KEY_PREFIX + id);
      return null;
    }
    const ttl = this.ttlFor(record.createdAt);
    if (ttl <= 0) {
      await this.redis.del(KEY_PREFIX + id);
      return null;
    }
    await this.redis.expire(KEY_PREFIX + id, ttl);
    return record;
  }

  async destroy(id: string): Promise<void> {
    if (id) await this.redis.del(KEY_PREFIX + id);
  }
}
