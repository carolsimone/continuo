import { describe, it, expect } from 'vitest';
import { githubWebBaseUrl, commitUrl } from '../../src/server/github/web-base';

describe('githubWebBaseUrl', () => {
  it('maps the public API host to github.com, and no configuration at all to github.com', () => {
    expect(githubWebBaseUrl('https://api.github.com')).toBe('https://github.com');
    expect(githubWebBaseUrl('https://api.github.com/')).toBe('https://github.com');
    expect(githubWebBaseUrl(undefined)).toBe('https://github.com');
    expect(githubWebBaseUrl('')).toBe('https://github.com');
  });
  it('maps a GitHub Enterprise Server API base (/api/v3 under the web host) to that host', () => {
    expect(githubWebBaseUrl('https://ghe.example.com/api/v3')).toBe('https://ghe.example.com');
    expect(githubWebBaseUrl('https://ghe.example.com/api/v3/')).toBe('https://ghe.example.com');
  });
  it('keeps any other base as the web host, without a trailing slash', () => {
    expect(githubWebBaseUrl('http://stub-github:9200/')).toBe('http://stub-github:9200');
  });
});

describe('commitUrl', () => {
  it('links an owner/name repo and commit under the web base', () => {
    expect(commitUrl('https://ghe.example.com', 'acme/demo', 'abcdef1234567'))
      .toBe('https://ghe.example.com/acme/demo/commit/abcdef1234567');
  });
  it('is empty when either half is missing or the repo is not owner/name', () => {
    expect(commitUrl('https://github.com', '', 'abc')).toBe('');
    expect(commitUrl('https://github.com', 'acme/demo', '')).toBe('');
    expect(commitUrl('https://github.com', 'demo', 'abc')).toBe('');
    expect(commitUrl('https://github.com', 'acme/demo/extra', 'abc')).toBe('');
  });
});
