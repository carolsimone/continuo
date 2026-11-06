import { ScheduleSummary } from './types';

// Whether a schedule runs on its own: scheduled (its cron fires), paused (it has
// a cron, and nothing starts on its own) or manual (no cron: it runs only when
// triggered).
export type ScheduleState = 'scheduled' | 'paused' | 'manual';

export function scheduleState(s: Pick<ScheduleSummary, 'cron' | 'paused'>): ScheduleState {
  if (s.cron.trim() === '') return 'manual';
  return s.paused ? 'paused' : 'scheduled';
}

// clockParts reads an instant as the wall clock of timeZone, or of the
// viewer's own zone when timeZone is empty or one this browser does not know.
function clockParts(iso: string, timeZone: string, options: Intl.DateTimeFormatOptions): Map<string, string> {
  let format: Intl.DateTimeFormat;
  try {
    format = new Intl.DateTimeFormat('en-US', { ...options, timeZone: timeZone || undefined });
  } catch {
    format = new Intl.DateTimeFormat('en-US', options);
  }
  return new Map(format.formatToParts(new Date(iso)).map(p => [p.type, p.value]));
}

// formatDayTime renders an instant as "28 Oct 01:00" in timeZone ("" is the
// viewer's zone).
export function formatDayTime(iso: string, timeZone = ''): string {
  const p = clockParts(iso, timeZone, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
  return `${p.get('day')} ${p.get('month')} ${p.get('hour')}:${p.get('minute')}`;
}

// formatFireDate renders the date of an instant as "Wed 28 Oct 2026" in
// timeZone.
export function formatFireDate(iso: string, timeZone: string): string {
  const p = clockParts(iso, timeZone, { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' });
  return `${p.get('weekday')} ${p.get('day')} ${p.get('month')} ${p.get('year')}`;
}

// formatFireTime renders the time of an instant as "01:00" in timeZone.
export function formatFireTime(iso: string, timeZone: string): string {
  const p = clockParts(iso, timeZone, { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
  return `${p.get('hour')}:${p.get('minute')}`;
}
