// Query-parameter parsing for paged list endpoints. A malformed, negative, or
// zero limit falls back to the endpoint default rather than erroring, so a bad
// URL degrades to the normal page instead of a 400.

export function parseLimit(raw: unknown, opts: { def: number; max: number }): number {
  const n = parseInt(String(raw), 10);
  if (Number.isNaN(n) || n < 1) return opts.def;
  return Math.min(n, opts.max);
}

export function parseOffset(raw: unknown): number {
  const n = parseInt(String(raw), 10);
  if (Number.isNaN(n) || n < 0) return 0;
  return n;
}
