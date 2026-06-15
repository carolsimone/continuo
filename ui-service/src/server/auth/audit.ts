// One structured JSON line per security-relevant event, on stdout — the log
// channel collected on every deployment surface; deployers route it to a SIEM
// (Security Information and Event Management) system.
export function audit(event: string, fields: Record<string, unknown>): void {
  console.log(JSON.stringify({ ts: new Date().toISOString(), audit: true, event, ...fields }));
}
