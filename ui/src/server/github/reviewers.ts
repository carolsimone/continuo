// A GitHub login: 1 to 39 letters, digits and single hyphens, never leading or
// trailing with a hyphen.
const LOGIN = '[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}';
const USER = new RegExp(`^@?(${LOGIN})$`);
// "org/team", written with or without a leading @: an organization login, a
// slash, and the team's slug.
const TEAM = new RegExp(`^@?${LOGIN}/([A-Za-z0-9][A-Za-z0-9_.-]*)$`);

export type OwnerKind = 'user' | 'team' | 'other';

// classifyOwner reads how an owner, as a node's code writes it, can be asked to
// review: a team by its slug (the part after the slash of `org/team` or
// `@org/team`), a user by its login (with or without a leading @), and
// anything else (`finance`, `team@acme.com`, a name with spaces) as other,
// which GitHub cannot be asked for. A blank owner is undefined.
export function classifyOwner(owner: string): { kind: OwnerKind; name: string } | undefined {
  const text = owner.trim();
  if (text === '') return undefined;
  const team = TEAM.exec(text);
  if (team) return { kind: 'team', name: team[1] };
  const user = USER.exec(text);
  if (user) return { kind: 'user', name: user[1] };
  return { kind: 'other', name: text };
}

// splitOwners sorts owners into the users and teams to ask for a review and the
// others to list only. Each list keeps the order its first entry came in and
// holds a name once: logins and slugs compare without regard to case, so
// `@acme/finance` and `acme/finance` are one team.
export function splitOwners(owners: string[]): { users: string[]; teams: string[]; other: string[] } {
  const out = { users: [] as string[], teams: [] as string[], other: [] as string[] };
  const seen = { user: new Set<string>(), team: new Set<string>(), other: new Set<string>() };
  const lists = { user: out.users, team: out.teams, other: out.other };
  for (const owner of owners) {
    const c = classifyOwner(owner);
    if (!c) continue;
    const key = c.kind === 'other' ? c.name : c.name.toLowerCase();
    if (seen[c.kind].has(key)) continue;
    seen[c.kind].add(key);
    lists[c.kind].push(c.name);
  }
  return out;
}
