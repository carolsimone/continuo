import { describe, expect, it } from 'vitest';
import { splitOwners } from '../../src/server/github/reviewers';

describe('splitOwners', () => {
  it('splits teams, users and other owners', () => {
    expect(splitOwners(['acme/finance', '@acme/data-eng', 'alice', 'finance team', 'team@acme.com'])).toEqual({
      users: ['alice'],
      teams: ['finance', 'data-eng'],
      other: ['finance team', 'team@acme.com'],
    });
  });

  it('takes the slug after the slash as the team, whether or not the owner starts with @', () => {
    expect(splitOwners(['acme/finance']).teams).toEqual(['finance']);
    expect(splitOwners(['@acme/finance']).teams).toEqual(['finance']);
  });

  it('drops a duplicate once @org/team and org/team are the same team', () => {
    expect(splitOwners(['acme/finance', '@acme/finance', 'acme/finance'])).toEqual({
      users: [],
      teams: ['finance'],
      other: [],
    });
  });

  it('drops a duplicate user, however the login is cased', () => {
    expect(splitOwners(['Alice', 'alice', 'bob', 'bob'])).toEqual({
      users: ['Alice', 'bob'],
      teams: [],
      other: [],
    });
  });

  it('drops a duplicate other owner', () => {
    expect(splitOwners(['finance team', 'finance team']).other).toEqual(['finance team']);
  });

  it('reads a GitHub login by its shape', () => {
    // 39 characters is the longest login; a hyphen cannot lead, trail or double up.
    const longest = 'a'.repeat(39);
    expect(splitOwners([longest, 'a-b', '9lives']).users).toEqual([longest, 'a-b', '9lives']);
    const refused = ['a'.repeat(40), '-lead', 'trail-', 'dou--ble', 'with space', 'dot.ted', 'under_score'];
    expect(splitOwners(refused)).toEqual({ users: [], teams: [], other: refused });
  });

  it('lists an owner that is not org/team as other, never as a team', () => {
    const other = ['acme/', '/finance', 'a/b/c', '@/finance', 'ac me/finance', 'acme/fin ance'];
    expect(splitOwners(other)).toEqual({ users: [], teams: [], other });
  });

  it('reads a bare word with the shape of a login as a user, since only GitHub knows who exists', () => {
    expect(splitOwners(['finance'])).toEqual({ users: ['finance'], teams: [], other: [] });
  });

  it('keeps an @login as a user', () => {
    expect(splitOwners(['@alice']).users).toEqual(['alice']);
  });

  it('trims the owners and leaves out an empty one', () => {
    expect(splitOwners(['  alice  ', '', '   ', ' acme/finance '])).toEqual({
      users: ['alice'],
      teams: ['finance'],
      other: [],
    });
  });

  it('is empty for no owners', () => {
    expect(splitOwners([])).toEqual({ users: [], teams: [], other: [] });
  });
});
