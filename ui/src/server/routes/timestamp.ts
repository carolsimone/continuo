// A protobuf Timestamp as @grpc/grpc-js decodes it: seconds is a string (or
// number) because int64 is loaded as a string.
export interface ProtoTimestamp {
  seconds: string | number;
  nanos?: number;
}

// toISO renders a Timestamp as an ISO-8601 string. An unset Timestamp (absent
// or null) is null; a value that is already a string passes through.
export function toISO(ts: ProtoTimestamp | string | null | undefined): string | null {
  if (!ts) return null;
  if (typeof ts === 'string') return ts;
  const ms = Number(ts.seconds) * 1000 + Math.floor((ts.nanos ?? 0) / 1_000_000);
  return new Date(ms).toISOString();
}
